package config

var ProfileMode bool

// 默认是false, 可以在命令行中打开
func init() {
	ProfileMode = false
}
