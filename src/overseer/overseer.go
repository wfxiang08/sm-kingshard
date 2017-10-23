// Package overseer implements daemonizable
// self-upgrading binaries in Go (golang).
package overseer

import (
	"errors"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

const (
	envSlaveID = "OVERSEER_SLAVE_ID"
	envIsSlave = "OVERSEER_IS_SLAVE"
	envNumFDs  = "OVERSEER_NUM_FDS"
	envBinID   = "OVERSEER_BIN_ID"
	envBinPath = "OVERSEER_BIN_PATH"
)

// Config defines overseer's run-time configuration
type Config struct {
	//Program's main function
	Program func(state State)

	//Program's zero-downtime socket listening addresses (set this or Address)
	Addresses []string

	//RestartSignal will manually trigger a graceful restart. Defaults to SIGUSR2.
	RestartSignal os.Signal
	//TerminateTimeout controls how long overseer should
	//wait for the program to terminate itself. After this
	//timeout, overseer will issue a SIGKILL.
	TerminateTimeout time.Duration

	//Debug enables all [overseer] logs.
	Debug bool
	//NoWarn disables warning [overseer] logs.
	NoWarn bool

	Pidfile   string
}

func validate(c *Config) error {
	//validate
	if c.Program == nil {
		return errors.New("overseer.Config.Program required")
	}

	// 设置重启信号: kill -USR2 pid
	if c.RestartSignal == nil {
		c.RestartSignal = SIGUSR2
	}

	// 默认结束时最多等待30s
	if c.TerminateTimeout <= 0 {
		c.TerminateTimeout = 30 * time.Second
	}
	return nil
}

func Run(c *Config) error {

	if err := validate(c); err != nil {
		return err
	}
	// run either in master or slave mode
	// slave mode由 master来触发
	// 正常情况下，我们会以master的方式启动；然后master再启动slave
	if os.Getenv(envIsSlave) == "1" {
		slaveProcess := &slave{Config: c}
		return slaveProcess.run()
	} else {

		//
		// pid文件的管理
		//
		if len(c.Pidfile) > 0 {
			if pidfile, err := filepath.Abs(c.Pidfile); err != nil {
				log.WarnErrorf(err, "parse pidfile = '%s' failed", c.Pidfile)
				// 将pid写入文件
			} else if err := ioutil.WriteFile(pidfile, []byte(strconv.Itoa(os.Getpid())), 0644); err != nil {
				log.WarnErrorf(err, "write pidfile = '%s' failed", pidfile)
			} else {
				// 下面的 masterProcess.run() 会一直活着，因此这里的defer会等到程序退出
				defer func() {
					if err := os.Remove(pidfile); err != nil {
						log.WarnErrorf(err, "remove pidfile = '%s' failed", pidfile)
					}
				}()
				log.Warnf("option --pidfile = %s", pidfile)
			}
		}
	}

	masterProcess := &master{Config: c}
	return masterProcess.run()
}
