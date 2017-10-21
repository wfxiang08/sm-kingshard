package media_utils

import "time"

// 当前时间
func Microseconds() int64 {
	return time.Now().UnixNano() / int64(time.Microsecond)
}
