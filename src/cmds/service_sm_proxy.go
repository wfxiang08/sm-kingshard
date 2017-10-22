package main

import (
	"config"
	"core/hack"
	"flag"
	"fmt"
	"github.com/fatih/color"
	"github.com/wfxiang08/cyutils/utils/atomic2"
	"github.com/wfxiang08/cyutils/utils/http"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"net"
	"overseer"
	"proxy/server"
	"web"
)

var (
	configFile  = flag.String("config", "/etc/ks.yaml", "kingshard config file")
	logLevel    = flag.String("log-level", "", "log level [debug|info|warn|error], default error")
	logPath     = flag.String("log-path", "", "")
	version     = flag.Bool("v", false, "the version of kingshard")
	address     = flag.String("address", "", "MySQL Proxy Address")
	pidfile     = flag.String("pidfile", "", "pidfile")
	profileAddr = flag.String("profile_address", "", "profile address")
	cfg         *config.Config
)

func main() {
	flag.Parse()

	if *version {
		fmt.Printf("==> Git commit:%s, Build time:%s\n", hack.Version, hack.Compile)
		return
	}
	if len(*configFile) == 0 {
		log.Errorf("must use a config file")
		return
	}

	// 解析配置文件
	var err error
	cfg, err = config.ParseConfigFile(*configFile)
	if err != nil {
		log.ErrorErrorf(err, "parse config file error")
		return
	}

	if len(*logPath) != 0 {
		f, err := log.NewRollingFile(*logPath, 3)
		if err != nil {
			log.PanicErrorf(err, "open rolling log file failed: %s", *logPath)
		} else {
			defer f.Close()
			log.StdLog = log.New(f, "")
		}
	}

	if *logLevel != "" {
		log.SetLogLevel(*logLevel)
	} else {
		log.SetLogLevel(cfg.LogLevel)
	}

	var running atomic2.Bool
	running.Set(true)

	// 1. 开启Profile
	if len(*profileAddr) > 0 {
		go http.StartHttpProfile(*profileAddr, running)
	}

	// 2. 准备addresses
	if len(*address) > 0 {
		log.Printf("Use address from commandline: %s", *address)
		cfg.Addr = *address
	}

	var addresses []string
	addresses = append(addresses, cfg.Addr)

	if len(cfg.WebAddr) > 0 {
		addresses = append(addresses, cfg.WebAddr)
	}

	// 3. 运行overseer
	overseer.Run(&overseer.Config{
		Program:   gracefulServer, // 执行的函数体
		Addresses: addresses,
		Debug:     false,
		Pidfile:   *pidfile,
	})

}

func gracefulServer(state overseer.State) {

	svr, err := server.NewServer(cfg, state.Listeners[0])
	if err != nil {
		log.PanicErrorf(err, "start server failed")
	}

	if len(state.Listeners) > 1 {
		// 启动http admin server
		go func() {
			apiSvr, err := web.NewApiServer(cfg, svr, state.Listeners[1].(*net.TCPListener))
			if err != nil {
				log.ErrorErrorf(err, color.RedString("web.NewApiServer failed"))
				return
			}
			apiSvr.Run()
		}()
	}

	// 启动db proxy service
	svr.Run()
}
