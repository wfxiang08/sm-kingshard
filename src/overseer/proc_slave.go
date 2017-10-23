package overseer

import (
	"fmt"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

var (
	//DisabledState is a placeholder state for when
	//overseer is disabled and the program function
	//is run manually.
	DisabledState = State{Enabled: false}
)

// State contains the current run-time state of overseer
type State struct {
	//whether overseer is running enabled. When enabled,
	//this program will be running in a child process and
	//overseer will perform rolling upgrades.
	Enabled bool

	//ID is a SHA-1 hash of the current running binary
	ID string
	//StartedAt records the start time of the program
	StartedAt time.Time

	//监听的Listerns & 监听的地址
	Listeners []net.Listener
	Addresses []string

	//Path of the binary currently being executed
	BinPath string
}

//a overseer slave process

type slave struct {
	*Config
	id         string
	listeners  []*overseerListener
	masterPid  int
	masterProc *os.Process
	state      State
}

func (sp *slave) run() error {
	// log.Printf("Slave run...")

	sp.id = os.Getenv(envSlaveID)
	sp.state.Enabled = true
	sp.state.ID = os.Getenv(envBinID)
	sp.state.StartedAt = time.Now()
	sp.state.Addresses = sp.Config.Addresses

	sp.state.BinPath = os.Getenv(envBinPath)
	if err := sp.watchParent(); err != nil {
		return err
	}

	// 集成文件描述符
	if err := sp.initFileDescriptors(); err != nil {
		return err
	}

	sp.watchSignal()
	//run program with state
	// log.Printf("Slave start program run...")

	// 运行Program
	sp.Config.Program(sp.state)
	return nil
}

//
// 子进程监控父进程，通过 kill -0 方式监控"死活", 如果父进程挂了，子进程自己退出
//
func (sp *slave) watchParent() error {
	sp.masterPid = os.Getppid()

	// 子进程如何监控父进程
	proc, err := os.FindProcess(sp.masterPid)
	if err != nil {
		return fmt.Errorf("master process: %s", err)
	}
	sp.masterProc = proc
	go func() {
		//send signal 0 to master process forever
		for {
			//should not error as long as the process is alive
			// 如果父进程挂了，则子进程自动退出
			if err := sp.masterProc.Signal(syscall.Signal(0)); err != nil {
				os.Exit(1)
			}
			time.Sleep(2 * time.Second)
		}
	}()
	return nil
}

// 初始化文件描述符
func (sp *slave) initFileDescriptors() error {

	//inspect file descriptors
	numFDs, err := strconv.Atoi(os.Getenv(envNumFDs))
	if err != nil {
		return fmt.Errorf("invalid %s integer", envNumFDs)
	}
	sp.listeners = make([]*overseerListener, numFDs)
	sp.state.Listeners = make([]net.Listener, numFDs)

	// 遍历所有的FD, 尝试集成
	for i := 0; i < numFDs; i++ {
		// 问题来了? l在什么时候被创建的呢?
		f := os.NewFile(uintptr(3+i), "")
		l, err := net.FileListener(f)
		if err != nil {
			log.ErrorErrorf(err, "failed to inherit file descriptor: %d", i)
			return fmt.Errorf("failed to inherit file descriptor: %d", i)
		}
		u := newOverseerListener(l)
		sp.listeners[i] = u
		sp.state.Listeners[i] = u
	}

	return nil
}

func (sp *slave) watchSignal() {
	signals := make(chan os.Signal)
	// 监听重启信号
	signal.Notify(signals, sp.Config.RestartSignal)

	go func() {
		<-signals
		// 不再监听信号，多余的也不消费了
		signal.Stop(signals)

		log.Printf("graceful shutdown of slave requested")

		// 通知Master SIGUSR1，可以fork新的进程了。
		log.Printf("Child quit SIGUSR1 from [%d] to [%d]", os.Getpid(), sp.masterProc.Pid)
		sp.masterProc.Signal(SIGUSR1)

		// 通知完毕，自己在慢慢善后
		if len(sp.listeners) > 0 {
			// 释放listeners, 然后准备退出
			for _, l := range sp.listeners {
				l.release(sp.Config.TerminateTimeout)
			}
		}

		//start death-timer
		//如何退出呢? 自杀即可
		go func() {
			time.Sleep(sp.Config.TerminateTimeout + 1)
			// 甭管Clients是什么状态，slave该死的时候还是开开心心去死; 直接Exit(0)
			log.Printf("timeout. forceful shutdown")
			os.Exit(0)
		}()
	}()
}

func (sp *slave) triggerRestart() {
	if err := sp.masterProc.Signal(sp.Config.RestartSignal); err != nil {
		os.Exit(1)
	}
}

func (sp *slave) debugf(f string, args ...interface{}) {
	if sp.Config.Debug {
		log.Printf("[overseer slave#"+sp.id+"] "+f, args...)
	}
}

func (sp *slave) warnf(f string, args ...interface{}) {
	if sp.Config.Debug || !sp.Config.NoWarn {
		log.Printf("[overseer slave#"+sp.id+"] "+f, args...)
	}
}
