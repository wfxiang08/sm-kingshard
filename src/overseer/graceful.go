package overseer

//overseer listeners and connections allow graceful
//restarts by tracking when all connections from a listener
//have been closed

import (
	"net"
	"os"
	"sync"
	"time"
)

func newOverseerListener(l net.Listener) *overseerListener {
	return &overseerListener{
		Listener:     l,
		closeByForce: make(chan bool),
	}
}

//gracefully closing net.Listener
type overseerListener struct {
	net.Listener
	closeError   error
	closeByForce chan bool      // 手动强制关闭，现在似乎没有使用
	wg           sync.WaitGroup // 等待Accepted请求处理完毕
}

func (l *overseerListener) Accept() (net.Conn, error) {

	var connection net.Conn
	// 处理TCP和Unix Domain Socket的不同
	if tcpListener, ok := l.Listener.(*net.TCPListener); ok {
		// 支持TCP
		conn, err := tcpListener.AcceptTCP()
		if err != nil {
			return nil, err
		}

		// 添加KeepAlive等
		// Unix Domain Socket就不需要这些设置
		conn.SetKeepAlive(true)                  // see http.tcpKeepAliveListener
		conn.SetKeepAlivePeriod(3 * time.Minute) // see http.tcpKeepAliveListener

		connection = conn
	} else {
		// 支持Unix Domain Socket
		conn, err := l.Listener.(*net.UnixListener).AcceptUnix()
		if err != nil {
			return nil, err
		}
		connection = conn
	}

	uconn := overseerConn{
		Conn:   connection,
		wg:     &l.wg,
		closed: make(chan bool),
	}
	go func() {
		//connection watcher
		select {
		case <-l.closeByForce:
			// listener会整体控制所有的clients
			// 关闭 closeByForce 之后，所有的 <-l.closeByForce 都会返回, 然后所有的uconn都会强制关闭
			// Conn还有其他正常关闭的渠道吗?
			uconn.Close()
		case <-uconn.closed:
			//closed manually
		}
	}()
	l.wg.Add(1)
	return uconn, nil
}

//non-blocking trigger close
func (l *overseerListener) release(timeout time.Duration) {
	//stop accepting connections - release fd
	//不再接受新的请求
	l.closeError = l.Listener.Close()

	//start timer, close by force if deadline not met
	waited := make(chan bool)
	go func() {
		// 等待已有的请求处理完毕
		// 实现: wait group --> channel的转换
		l.wg.Wait()
		waited <- true
	}()

	// 自然退出，或者"强制退出" (close也会导致 <-l.closeByForce 返回)
	go func() {
		select {
		case <-time.After(timeout):
			// timeout 强制退出
			close(l.closeByForce)
		case <-waited:
			// 等待 clients 都正常退出，也就是: l.wg.Wait()返回
			//no need to force close
		}
	}()
}

//blocking wait for close
func (l *overseerListener) Close() error {
	l.wg.Wait()
	return l.closeError
}

func (l *overseerListener) File() *os.File {
	// returns a dup(2) - FD_CLOEXEC flag *not* set
	if tl, ok := l.Listener.(*net.TCPListener); ok {
		fl, _ := tl.File()
		return fl
	} else {
		ul, _ := l.Listener.(*net.UnixListener)
		fl, _ := ul.File()
		return fl
	}
}

//notifying on close net.Conn
type overseerConn struct {
	net.Conn
	wg     *sync.WaitGroup
	closed chan bool
}

// 外接主动调用关闭
func (o overseerConn) Close() error {
	err := o.Conn.Close()
	if err == nil {
		o.wg.Done()
		o.closed <- true
	}
	return err
}
