package utils

import "time"

const (
	kMilliSecondInverse = 1.0 / float64(time.Millisecond)
	kMicroSecondInverse = 1.0 / float64(time.Microsecond)
)

// 当前时间
func Microseconds() float64 {
	return float64(time.Now().UnixNano()) * kMicroSecondInverse
}

func ElapsedMillSeconds(start time.Time, end time.Time) float64 {
	return float64(end.Sub(start)) * kMilliSecondInverse
}

func ElapsedMillSecondsByNano(startNanoSeconds int64, endNanoSeconds int64) float64 {
	return float64(endNanoSeconds-startNanoSeconds) * kMilliSecondInverse
}

func Nano2Milli(elapsed int64) float64 {
	return float64(elapsed) * kMilliSecondInverse
}

func Nano2MilliDuration(elapsed time.Duration) float64 {
	return float64(elapsed) * kMilliSecondInverse
}