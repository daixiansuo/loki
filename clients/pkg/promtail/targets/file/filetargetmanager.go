package file

import (
	"context"
	"fmt"
	"github.com/docker/docker/api/types"
	dockerutil "github.com/grafana/loki/clients/pkg/promtail/util"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/discovery/kubernetes"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/relabel"

	"github.com/grafana/loki/clients/pkg/logentry/stages"
	"github.com/grafana/loki/clients/pkg/promtail/api"
	"github.com/grafana/loki/clients/pkg/promtail/positions"
	"github.com/grafana/loki/clients/pkg/promtail/scrapeconfig"
	"github.com/grafana/loki/clients/pkg/promtail/targets/target"

	"github.com/grafana/loki/pkg/util"
)

const (
	pathLabel              = "__path__"
	hostLabel              = "__host__"
	kubernetesPodNodeField = "spec.nodeName"
)

// custom label

// copy from prometheus
const (
	// from prometheus.discovery.kubernetes
	metaLabelPrefix  = model.MetaLabelPrefix + "kubernetes_"
	namespaceLabel   = metaLabelPrefix + "namespace"
	metricsNamespace = "prometheus_sd_kubernetes"
	presentValue     = model.LabelValue("true")
	// from prometheus.discovery.kubernetes.pod
	podNameLabel                  = metaLabelPrefix + "pod_name"
	podIPLabel                    = metaLabelPrefix + "pod_ip"
	podContainerNameLabel         = metaLabelPrefix + "pod_container_name"
	podContainerPortNameLabel     = metaLabelPrefix + "pod_container_port_name"
	podContainerPortNumberLabel   = metaLabelPrefix + "pod_container_port_number"
	podContainerPortProtocolLabel = metaLabelPrefix + "pod_container_port_protocol"
	podContainerIdLabel           = metaLabelPrefix + "pod_container_id"
	podContainerIsInit            = metaLabelPrefix + "pod_container_init"
	podReadyLabel                 = metaLabelPrefix + "pod_ready"
	podPhaseLabel                 = metaLabelPrefix + "pod_phase"
	podLabelPrefix                = metaLabelPrefix + "pod_label_"
	podLabelPresentPrefix         = metaLabelPrefix + "pod_labelpresent_"
	podAnnotationPrefix           = metaLabelPrefix + "pod_annotation_"
	podAnnotationPresentPrefix    = metaLabelPrefix + "pod_annotationpresent_"
	podNodeNameLabel              = metaLabelPrefix + "pod_node_name"
	podHostIPLabel                = metaLabelPrefix + "pod_host_ip"
	podUID                        = metaLabelPrefix + "pod_uid"
	podControllerKind             = metaLabelPrefix + "pod_controller_kind"
	podControllerName             = metaLabelPrefix + "pod_controller_name"
)

// FileTargetManager manages a set of targets.
// nolint:golint
type FileTargetManager struct {
	log     log.Logger
	quit    context.CancelFunc
	syncers map[string]*targetSyncer
	manager *discovery.Manager
}

// NewFileTargetManager creates a new TargetManager.
func NewFileTargetManager(
	metrics *Metrics,
	logger log.Logger,
	positions positions.Positions,
	client api.EntryHandler,
	scrapeConfigs []scrapeconfig.Config,
	targetConfig *Config,
	docker *dockerutil.DockerClient,
) (*FileTargetManager, error) {
	reg := metrics.reg
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}

	ctx, quit := context.WithCancel(context.Background())
	tm := &FileTargetManager{
		log:     logger,
		quit:    quit,
		syncers: map[string]*targetSyncer{},
		manager: discovery.NewManager(ctx, log.With(logger, "component", "discovery")),
	}

	hostname, err := hostname()
	if err != nil {
		return nil, err
	}

	configs := map[string]discovery.Configs{}
	for _, cfg := range scrapeConfigs {
		if !cfg.HasServiceDiscoveryConfig() {
			continue
		}

		pipeline, err := stages.NewPipeline(log.With(logger, "component", "file_pipeline"), cfg.PipelineStages, &cfg.JobName, reg)
		if err != nil {
			return nil, err
		}

		// Add Source value to the static config target groups for unique identification
		// within scrape pool. Also, default target label to localhost if target is not
		// defined in promtail config.
		// Just to make sure prometheus target group sync works fine.
		for i, tg := range cfg.ServiceDiscoveryConfig.StaticConfigs {
			tg.Source = fmt.Sprintf("%d", i)
			if len(tg.Targets) == 0 {
				tg.Targets = []model.LabelSet{
					{model.AddressLabel: "localhost"},
				}
			}
		}

		// Add an additional api-level node filtering, so we only fetch pod metadata for
		// all the pods from the current node. Without this filtering we will have to
		// download metadata for all pods running on a cluster, which may be a long operation.
		for _, kube := range cfg.ServiceDiscoveryConfig.KubernetesSDConfigs {
			if kube.Role == kubernetes.RolePod {
				selector := fmt.Sprintf("%s=%s", kubernetesPodNodeField, hostname)
				kube.Selectors = []kubernetes.SelectorConfig{
					{Role: kubernetes.RolePod, Field: selector},
				}
			}
		}

		s := &targetSyncer{
			metrics:        metrics,
			log:            logger,
			positions:      positions,
			relabelConfig:  cfg.RelabelConfigs,
			targets:        map[string]*FileTarget{},
			droppedTargets: []target.Target{},
			hostname:       hostname,
			entryHandler:   pipeline.Wrap(client),
			targetConfig:   targetConfig,
			docker:         docker,
		}
		tm.syncers[cfg.JobName] = s
		configs[cfg.JobName] = cfg.ServiceDiscoveryConfig.Configs()
	}

	go tm.run()
	go util.LogError("running target manager", tm.manager.Run)

	return tm, tm.manager.ApplyConfig(configs)
}

func (tm *FileTargetManager) run() {
	for targetGroups := range tm.manager.SyncCh() {
		for jobName, groups := range targetGroups {
			tm.syncers[jobName].sync(groups)
		}
	}
}

// Ready if there's at least one file target
func (tm *FileTargetManager) Ready() bool {
	for _, s := range tm.syncers {
		if s.ready() {
			return true
		}
	}
	return false
}

// Stop the TargetManager.
func (tm *FileTargetManager) Stop() {
	tm.quit()

	for _, s := range tm.syncers {
		s.stop()
	}

}

// ActiveTargets returns the active targets currently being scraped.
func (tm *FileTargetManager) ActiveTargets() map[string][]target.Target {
	result := map[string][]target.Target{}
	for jobName, syncer := range tm.syncers {
		result[jobName] = append(result[jobName], syncer.ActiveTargets()...)
	}
	return result
}

// AllTargets returns all targets, active and dropped.
func (tm *FileTargetManager) AllTargets() map[string][]target.Target {
	result := map[string][]target.Target{}
	for jobName, syncer := range tm.syncers {
		result[jobName] = append(result[jobName], syncer.ActiveTargets()...)
		result[jobName] = append(result[jobName], syncer.DroppedTargets()...)
	}
	return result
}

// targetSyncer sync targets based on service discovery changes.
type targetSyncer struct {
	metrics      *Metrics
	log          log.Logger
	positions    positions.Positions
	entryHandler api.EntryHandler
	hostname     string

	droppedTargets []target.Target
	targets        map[string]*FileTarget
	mtx            sync.Mutex

	relabelConfig []*relabel.Config
	targetConfig  *Config

	// docker
	docker *dockerutil.DockerClient
}

// sync synchronize target based on received target groups received by service discovery
func (s *targetSyncer) sync(groups []*targetgroup.Group) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	targets := map[string]struct{}{}
	dropped := []target.Target{}
	for _, group := range groups {
		for _, t := range group.Targets {
			level.Debug(s.log).Log("msg", "new target", "labels", t)
			discoveredLabels := group.Labels.Merge(t)
			var labelMap = make(map[string]string)
			for k, v := range discoveredLabels.Clone() {
				labelMap[string(k)] = string(v)
			}

			processedLabels := relabel.Process(labels.FromMap(labelMap), s.relabelConfig...)

			var labels = make(model.LabelSet)
			for k, v := range processedLabels.Map() {
				labels[model.LabelName(k)] = model.LabelValue(v)
			}

			// Drop empty targets (drop in relabeling).
			if processedLabels == nil {
				dropped = append(dropped, target.NewDroppedTarget("dropping target, no labels", discoveredLabels))
				level.Debug(s.log).Log("msg", "dropping target, no labels")
				s.metrics.failedTargets.WithLabelValues("empty_labels").Inc()
				continue
			}

			host, ok := labels[hostLabel]
			if ok && string(host) != s.hostname {
				labels["host"] = host
				dropped = append(dropped, target.NewDroppedTarget(fmt.Sprintf("ignoring target, wrong host (labels:%s hostname:%s)", labels.String(), s.hostname), discoveredLabels))
				level.Debug(s.log).Log("msg", "ignoring target, wrong host", "labels", labels.String(), "hostname", s.hostname)
				s.metrics.failedTargets.WithLabelValues("wrong_host").Inc()
				continue
			}
			var (
				path        model.LabelValue
				containerId model.LabelValue
			)
			if fileBasedDiscovery, ok := labels["file_based_discovery"]; ok && string(fileBasedDiscovery) == "static_configs" {
				// is base file_based_discovery
				addressTarget, ok := labels["__address__"]
				if !ok {
					dropped = append(dropped, target.NewDroppedTarget("no path for target", discoveredLabels))
					level.Info(s.log).Log("msg", "no path for target", "labels", labels.String())
					s.metrics.failedTargets.WithLabelValues("no_path").Inc()
					continue
				}
				hostTargetSplit := strings.Split(string(addressTarget), ":")
				if len(hostTargetSplit) != 2 {
					continue
				}
				if hostTargetSplit[0] != "*" && hostTargetSplit[0] != s.hostname {
					continue
				}
				path = model.LabelValue(hostTargetSplit[1])

			} else {
				path, ok = labels[pathLabel]
				if !ok {
					dropped = append(dropped, target.NewDroppedTarget("no path for target", discoveredLabels))
					level.Info(s.log).Log("msg", "no path for target", "labels", labels.String())
					s.metrics.failedTargets.WithLabelValues("no_path").Inc()
					continue
				}
				// 检测是否是k8s， 既是否包含podcontanerid的标签
				containerId, ok := labels[podContainerIdLabel]
				if ok {
					inspcet, err := s.docker.DockerInspect(string(containerId))
					if err != nil {
						level.Error(s.log).Log("msg", "failed get docker inspect information")
					} else {
						path = model.LabelValue(getCustomPodLogPathFromDockerInspect(inspcet))
					}
				}
			}

			for k := range labels {
				if strings.HasPrefix(string(k), "__") {
					delete(labels, k)
				}
			}

			key := labels.String()
			targets[key] = struct{}{}
			if _, ok := s.targets[key]; ok {
				t := s.targets[key]
				if string(containerId) != "" && strings.Compare(t.preContainerId, string(containerId)) == 0 {
					dropped = append(dropped, target.NewDroppedTarget("ignoring target, already exists", discoveredLabels))
					level.Debug(s.log).Log("msg", "ignoring target, already exists", "labels", labels.String())
					s.metrics.failedTargets.WithLabelValues("exists").Inc()
					continue
				}
				// pod crashbackoff过一次，当pod crashbackoff的时候drop掉
				go t.Stop() // if existed ,but container id is different, then stop it, create new one
				dropped = append(dropped, target.NewDroppedTarget("ignoring target, already exists", discoveredLabels))
				level.Debug(s.log).Log("msg", "ignoring target, already exists ,but pod crash_backoff", "labels", labels.String())
				delete(s.targets, key)
			}

			level.Info(s.log).Log("msg", "Adding target", "key", key)
			t, err := s.newTarget(string(path), labels, discoveredLabels)
			if err != nil {
				dropped = append(dropped, target.NewDroppedTarget(fmt.Sprintf("Failed to create target: %s", err.Error()), discoveredLabels))
				level.Error(s.log).Log("msg", "Failed to create target", "key", key, "error", err)
				s.metrics.failedTargets.WithLabelValues("error").Inc()
				continue
			}
			t.preContainerId = string(containerId)
			s.metrics.targetsActive.Add(1.)
			s.targets[key] = t
		}
	}

	for key, target := range s.targets {
		if _, ok := targets[key]; !ok {
			level.Info(s.log).Log("msg", "Removing target", "key", key)
			target.Stop()
			s.metrics.targetsActive.Add(-1.)
			delete(s.targets, key)
		}
	}
	s.droppedTargets = dropped
}

func (s *targetSyncer) newTarget(path string, labels model.LabelSet, discoveredLabels model.LabelSet) (*FileTarget, error) {
	return NewFileTarget(s.metrics, s.log, s.entryHandler, s.positions, path, labels, discoveredLabels, s.targetConfig)
}

func (s *targetSyncer) DroppedTargets() []target.Target {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return append([]target.Target(nil), s.droppedTargets...)
}

func (s *targetSyncer) ActiveTargets() []target.Target {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	actives := []target.Target{}
	for _, t := range s.targets {
		actives = append(actives, t)
	}
	return actives
}

func (s *targetSyncer) ready() bool {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	for _, target := range s.targets {
		if target.Ready() {
			return true
		}
	}
	return false
}
func (s *targetSyncer) stop() {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	for key, target := range s.targets {
		level.Info(s.log).Log("msg", "Removing target", "key", key)
		target.Stop()
		delete(s.targets, key)
	}
	s.entryHandler.Stop()
}

func hostname() (string, error) {
	hostname := os.Getenv("HOSTNAME")
	if hostname != "" {
		return hostname, nil
	}

	return os.Hostname()
}

// getCustomPodLogPathFromDockerInspect get some custom log in pod by combine some hard code with some path get from docker inspect
func getCustomPodLogPathFromDockerInspect(inspect *types.ContainerJSON) string {
	// for tomcat about logs is located at docker's data path
	var (
		tomcatAccessLog   string = "/usr/local/tomcat/logs/localhost_access_log.log"
		tomcatCatalinaLog string = "/usr/local/tomcat/logs/catalina.out"
		jsonAppLog        string = "/root/logs/*/appJson/jsonApp.*.log"
		accessRestLog     string = "/root/logs/*/access.*.log"
		accessDubboLog    string = "/root/logs/*/dubboAccess.*.log"
		mongoLog          string = "/root/logs/*/sql.*.log"
		gcLog             string = "/root/logs/*/gc.log"
		javaMemory        string = "/root/logs/*/memory.log"
		application       string = "/root/logs/*/application.log"
		appLog            string = "/root/logs/*/app.log"
	)

	var pathString string = "{" + inspect.LogPath + ","
	if graphDiff, err := dockerutil.GetDockerDataPath(inspect); err == nil {
		pathString += path.Join(graphDiff, tomcatCatalinaLog) + "," +
			path.Join(graphDiff, appLog) + "," +
			path.Join(graphDiff, tomcatAccessLog) + "," +
			path.Join(graphDiff, gcLog) + "," +
			path.Join(graphDiff, jsonAppLog) + "," +
			path.Join(graphDiff, accessRestLog) + "," +
			path.Join(graphDiff, accessDubboLog) + "," +
			path.Join(graphDiff, mongoLog) + "," +
			path.Join(graphDiff, javaMemory) + "," +
			path.Join(graphDiff, application) + ","
	}

	pathString += "}"
	return pathString
}
