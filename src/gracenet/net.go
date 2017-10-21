// Package gracenet provides a family of Listen functions that either open a
// fresh connection or provide an inherited connection from when the process
// was started. The behave like their counterparts in the net package, but
// transparently provide support for graceful restarts without dropping
// connections. This is provided in a systemd socket activation compatible form
// to allow using socket activation.
//
// BUG: Doesn't handle closing of listeners.
package gracenet

import (
	"fmt"
	"github.com/wfxiang08/cyutils/utils/atomic2"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	// Used to indicate a graceful restart in the new process.
	envCountKey       = "LISTEN_FDS"
	envCountKeyPrefix = envCountKey + "="
)

// In order to keep the working directory the same as when we started we record
// it at startup.
var originalWD, _ = os.Getwd()

// Net provides the family of Listen functions and maintains the associated
// state. Typically you will have only once instance of Net per application.
type Net struct {
	inherited   []net.Listener
	active      []net.Listener // 当前处于活跃状态的Listener
	mutex       sync.Mutex
	inheritOnce sync.Once

	// used in tests to override the default behavior of starting from fd 3.
	fdStart int
}

//
// 继承Parent的sockets
//
func (n *Net) inherit() error {
	var retErr error

	n.inheritOnce.Do(func() {
		n.mutex.Lock()
		defer n.mutex.Unlock()

		// 通过环境变量传递参数
		countStr := os.Getenv(envCountKey)
		if countStr == "" {
			return
		}
		count, err := strconv.Atoi(countStr)
		if err != nil {
			retErr = fmt.Errorf("found invalid count value: %s=%s", envCountKey, countStr)
			return
		}

		// In tests this may be overridden.
		fdStart := n.fdStart
		if fdStart == 0 {
			// 前几个fd分别为: stdout, stderr, ...
			// In normal operations if we are inheriting, the listeners will begin at
			// fd 3.
			fdStart = 3
		}

		// 如何继承其他进程的listeners?
		// 这个似乎不靠谱? 原始的Listen是否能保证是连续的呢？
		for i := fdStart; i < fdStart+count; i++ {
			// 如何复用Socket呢？
			file := os.NewFile(uintptr(i), "listener")
			l, err := net.FileListener(file)
			if err != nil {
				file.Close()
				retErr = fmt.Errorf("error inheriting socket fd %d: %s", i, err)
				return
			}
			if err := file.Close(); err != nil {
				retErr = fmt.Errorf("error closing inherited socket fd %d: %s", i, err)
				return
			}
			n.inherited = append(n.inherited, l)
		}
	})
	return retErr
}

// Listen announces on the local network address laddr. The network net must be
// a stream-oriented network: "tcp", "tcp4", "tcp6", "unix" or "unixpacket". It
// returns an inherited net.Listener for the matching network and address, or
// creates a new one using net.Listen.
func (n *Net) Listen(nett, laddr string) (net.Listener, error) {
	switch nett {
	default:
		return nil, net.UnknownNetworkError(nett)
	case "tcp", "tcp4", "tcp6":
		addr, err := net.ResolveTCPAddr(nett, laddr)
		if err != nil {
			return nil, err
		}
		return n.ListenTCP(nett, addr)
	case "unix", "unixpacket", "invalid_unix_net_for_test":
		addr, err := net.ResolveUnixAddr(nett, laddr)
		if err != nil {
			return nil, err
		}
		return n.ListenUnix(nett, addr)
	}
}

// ListenTCP announces on the local network address laddr. The network net must
// be: "tcp", "tcp4" or "tcp6". It returns an inherited net.Listener for the
// matching network and address, or creates a new one using net.ListenTCP.
func (n *Net) ListenTCP(nett string, laddr *net.TCPAddr) (*net.TCPListener, error) {
	if err := n.inherit(); err != nil {
		return nil, err
	}

	n.mutex.Lock()
	defer n.mutex.Unlock()

	// 如果继承了listener, 则将listener状态切换到 active

	// look for an inherited listener
	for i, l := range n.inherited {
		if l == nil {
			// we nil used inherited listeners
			continue
		}
		if isSameAddr(l.Addr(), laddr) {
			n.inherited[i] = nil
			n.active = append(n.active, l)
			return l.(*net.TCPListener), nil
		}
	}

	// make a fresh listener
	l, err := net.ListenTCP(nett, laddr)

	if err != nil {
		return nil, err
	}
	n.active = append(n.active, l)
	return l, nil
}

// ListenUnix announces on the local network address laddr. The network net
// must be a: "unix" or "unixpacket". It returns an inherited net.Listener for
// the matching network and address, or creates a new one using net.ListenUnix.
func (n *Net) ListenUnix(nett string, laddr *net.UnixAddr) (*net.UnixListener, error) {
	if err := n.inherit(); err != nil {
		return nil, err
	}

	n.mutex.Lock()
	defer n.mutex.Unlock()

	// look for an inherited listener
	for i, l := range n.inherited {
		if l == nil {
			// we nil used inherited listeners
			continue
		}
		if isSameAddr(l.Addr(), laddr) {
			n.inherited[i] = nil
			n.active = append(n.active, l)
			ul := l.(*net.UnixListener)

			//// 退出之后不删除
			//if ul != nil {
			//	ul.SetUnlinkOnClose(false)
			//}
			return ul, nil
		}
	}

	// make a fresh listener
	l, err := net.ListenUnix(nett, laddr)
	if err != nil {
		return nil, err
	}
	// 退出之后不删除
	//l.SetUnlinkOnClose(false)
	n.active = append(n.active, l)
	return l, nil
}

// activeListeners returns a snapshot copy of the active listeners.
func (n *Net) activeListeners() ([]net.Listener, error) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	// 当前活跃的Listeners
	ls := make([]net.Listener, len(n.active))
	copy(ls, n.active)
	return ls, nil
}

func isSameAddr(a1, a2 net.Addr) bool {
	if a1.Network() != a2.Network() {
		return false
	}
	a1s := a1.String()
	a2s := a2.String()
	if a1s == a2s {
		return true
	}

	// This allows for ipv6 vs ipv4 local addresses to compare as equal. This
	// scenario is common when listening on localhost.
	const ipv6prefix = "[::]"
	a1s = strings.TrimPrefix(a1s, ipv6prefix)
	a2s = strings.TrimPrefix(a2s, ipv6prefix)
	const ipv4prefix = "0.0.0.0"
	a1s = strings.TrimPrefix(a1s, ipv4prefix)
	a2s = strings.TrimPrefix(a2s, ipv4prefix)
	return a1s == a2s
}

// StartProcess starts a new process passing it the active listeners. It
// doesn't fork, but starts a new process using the same environment and
// arguments as when it was originally started. This allows for a newly
// deployed binary to be started. It returns the pid of the newly started
// process when successful.
func (n *Net) StartProcess(running *atomic2.Bool) (int, error) {
	// 1. 获取Listeners
	listeners, err := n.activeListeners()
	if err != nil {
		return 0, err
	}

	// Extract the fds from the listeners.
	files := make([]*os.File, len(listeners))
	for i, l := range listeners {
		files[i], err = l.(filer).File()
		if err != nil {
			log.ErrorErrorf(err, "Get File from listener failed")
			return 0, err
		}

		// 延迟关闭
		defer func(file *os.File, l net.Listener) {
			log.Printf("Closing listener: %d ...", int(file.Fd()))
			running.Set(false)

			// 如果被Clone后，当前进程的Unix Domain Socket就不再删除
			if u, ok := l.(*net.UnixListener); ok {
				u.SetUnlinkOnClose(false)
			}
			file.Close()
			// l.Close()
		}(files[i], l)
	}

	// Use the original binary location. This works with symlinks such that if
	// the file it points to has been changed we will use the updated symlink.
	argv0, err := exec.LookPath(os.Args[0])
	if err != nil {
		return 0, err
	}

	// Pass on the environment and replace the old count key with the new one.
	var env []string
	for _, v := range os.Environ() {
		// 过滤掉: envCountKeyPrefix
		if !strings.HasPrefix(v, envCountKeyPrefix) {
			env = append(env, v)
		}
	}

	// 重新添加: envCountKeyPrefix
	// 问题?
	env = append(env, fmt.Sprintf("%s%d", envCountKeyPrefix, len(listeners)))

	allFiles := append([]*os.File{os.Stdin, os.Stdout, os.Stderr}, files...)
	process, err := os.StartProcess(argv0, os.Args, &os.ProcAttr{
		Dir:   originalWD,
		Env:   env,
		Files: allFiles,
	})
	if err != nil {
		return 0, err
	}

	// 启动之后等待两秒，等待新进程Ready
	time.Sleep(time.Second * 2)
	// 新的Process启动之后，原有的
	return process.Pid, nil
}

type filer interface {
	File() (*os.File, error)
}
