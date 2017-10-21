package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"config"
	"core/hack"
	"github.com/facebookgo/pidfile"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"media_utils"
	"proxy/server"
	"strings"
	"web"

	"github.com/wfxiang08/cyutils/utils"
	"time"
)

const (
	kTenSeconds = 1000000 * 10
)

var configFile *string = flag.String("config", "/etc/ks.yaml", "kingshard config file")
var logLevel *string = flag.String("log-level", "", "log level [debug|info|warn|error], default error")
var logPath *string = flag.String("log-path", "", "")
var version *bool = flag.Bool("v", false, "the version of kingshard")
var exitTimeout *int = flag.Int("et", 10, "Exit timeout in seconds")
var address *string = flag.String("address", "", "MySQL Proxy Address")

const banner string = `kingshard proxy`

func main() {
	start := media_utils.Microseconds()
	fmt.Println(banner)
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

	fmt.Printf("==> Git commit:%s, Build time:%s\n", hack.Version, hack.Compile)

	if *version {
		return
	}
	if len(*configFile) == 0 {
		fmt.Println("must use a config file")
		return
	}

	// 解析配置文件
	cfg, err := config.ParseConfigFile(*configFile)
	if err != nil {
		fmt.Printf("parse config file error:%v\n", err.Error())
		return
	}

	log.SetLevel(log.LEVEL_INFO)
	maxKeepDays := 3

	// 设置Log文件
	//when the log file size greater than 1GB, kingshard will generate a new file
	if len(*logPath) != 0 {
		f, err := log.NewRollingFile(*logPath, maxKeepDays)
		if err != nil {
			log.PanicErrorf(err, "open rolling log file failed: %s", *logPath)
		} else {
			defer f.Close()
			log.StdLog = log.New(f, "")
		}
	}

	if *logLevel != "" {
		setLogLevel(*logLevel)
	} else {
		setLogLevel(cfg.LogLevel)
	}

	if len(*address) > 0 {
		log.Printf("Use address from commandline: %s", *address)
		cfg.Addr = *address
	}

	if strings.Index(cfg.Addr, ":") == -1 {
		if utils.FileExist(cfg.Addr) && media_utils.Lsof(cfg.Addr) == 0 {
			log.Printf(media_utils.Magenta("Remove unused sock file: %s"), cfg.Addr)
			os.Remove(cfg.Addr)
		}
	}

	var svr *server.Server
	var apiSvr *web.ApiServer

	// 最核心的逻辑： Server
	svr, err = server.NewServer(cfg)

	if err != nil {
		log.ErrorErrorf(err, "server.NewServer failed")
		return
	}

	if len(cfg.WebAddr) > 0 {
		apiSvr, err = web.NewApiServer(cfg, svr)
		if err != nil {
			log.ErrorErrorf(err, media_utils.Red("web.NewApiServer failed"))
			svr.Close()
			return
		}
	}

	// 监听signal
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGUSR1, syscall.SIGUSR2)

	isOldProcess := false
	hasReload := false
	//stopNum := 0
	go func() {

		for {
			// 异步处理信号
			sig := <-sc
			switch sig {
			case syscall.SIGINT, syscall.SIGTERM:
				fallthrough
				//log.Printf(media_utils.Red("QUIT SIGNAL --> %v"), sig)
				//// 关闭Server, 然后: srv.run就结束
				//svr.Close()
				//stopNum++
				//if stopNum > 3 {
				//	os.Exit(-1)
				//}

			case syscall.SIGQUIT:
				// kill -3 pid
				log.Printf(media_utils.Red("QUIT SIGNAL --> Quit"))
				os.Exit(-1)

			case syscall.SIGUSR1:
				// kill -USR1 pid
				// 不同的平台下的USR1, USR2等数字不一样
				//
				clientLists := svr.GetClientList()
				log.Printf(media_utils.Green("ClientList: %v"), clientLists)

			case syscall.SIGUSR2:
				// kill -USR1 pid
				// hasReload 被设置之后，就不再处理新的Reload需求
				if media_utils.Microseconds()-start > kTenSeconds && !hasReload {
					hasReload = true
					isOldProcess = true

					log.Printf(media_utils.Red("RELOAD SIGNAL --> %v, Restart new process"), sig.String())
					// 启动一个新进程
					if newPid, err := svr.StartProcess(); err != nil {
						log.ErrorErrorf(err, media_utils.Red("GraceNet.StartProcess error"))
					} else {
						log.Printf("Process Fork: %d ==> %d", os.Getpid(), newPid)
					}

					svr.Close()
				} else {
					log.Printf(media_utils.Red("Reload too frequent, ignore reload signal"))
				}
			}
		}
	}()

	if apiSvr != nil {
		// 对外提供API服务
		go apiSvr.Run()
	}

	elapsed := media_utils.Microseconds() - start
	log.Printf("Service starts up elapsed time: %.3fms", float64(elapsed)*0.001)

	// 服务启动了，准备写入pid文件
	pidfile.Write()

	// 提供mysql server的服务
	svr.Run()

	func() {
		// 等待所有的ActiveConnction处理完毕
		requestsDone := make(chan bool)
		go func() {
			log.Printf("Waiting All requests done for: %d", os.Getpid())
			// 化阻塞的请求为chan
			svr.WaitClientsDone()
			log.Printf("All requests done for: %d", os.Getpid())
			requestsDone <- true
		}()

		// 等待正常结束，或者timeout(不能控制用户的行为，如果用户永不退出，进程可能永远没有机会退出，因此增加了timeout
		select {
		case <-requestsDone:
			log.Printf("Request done, terminate successfully")
		case <-time.After(time.Second * time.Duration(*exitTimeout)):
			log.Printf("Exit timeout, ignore existing connections: %s", strings.Join(svr.GetClientList(), ","))
		}

		if isOldProcess {
			log.Printf(media_utils.Magenta("Kingshard old process exit for restart"))
		} else {
			// 只有当前的Process直接退出时，才删除pid; 否则新的Process会覆盖pid
			pidPath := pidfile.GetPidfilePath()
			if len(pidPath) > 0 && utils.FileExist(pidPath) {
				log.Printf("Delete pid file: %s", pidPath)
				os.Remove(pidPath)
			}

			log.Printf(media_utils.Magenta("Kingshard process exit succeed"))
		}
	}()
}

func setLogLevel(level string) {
	var lv = log.LEVEL_INFO
	switch strings.ToLower(level) {
	case "error":
		lv = log.LEVEL_ERROR
	case "warn", "warning":
		lv = log.LEVEL_WARN
	case "debug":
		lv = log.LEVEL_DEBUG
	case "info":
		fallthrough
	default:
		lv = log.LEVEL_INFO
	}
	log.SetLevel(lv)
	log.Infof("set log level to %s", level)
}
