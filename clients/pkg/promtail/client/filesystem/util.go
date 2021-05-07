package filesystem

import (
	"fmt"
	"os"
	"time"
)

func Open(filename string) (*os.File, error) {
	return os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
}

func FileExisted(filename string) bool {
	if _, err := os.Stat(filename); err != nil {
		if !os.IsNotExist(err) {
			// todo log
		}
		return false
	}
	return true
}

// write file
//

// dateToday return an date string according
func date2String(opts ...string) string {
	if len(opts) > 0 {
		return opts[0]
	}
	local,_ := time.LoadLocation("Asia/Shanghai")
	year, month, day := time.Now().In(local).Date()
	return fmt.Sprintf("%d-%d-%d", year, month, day)
}
