package filesystem

type FakeFileHandler struct {
	manager  *Manager
	receiver chan string
	count    int
}

func (f *FakeFileHandler) Status() bool {
	return true
}

func (f *FakeFileHandler) run() {
	for c := range f.receiver {
		_ = c
		f.count++
	}
}

func (f *FakeFileHandler) Receiver() chan string {
	return f.receiver
}

func (f *FakeFileHandler) Stop() {
	close(f.receiver)
}

var _ Handler = (*FakeFileHandler)(nil)
