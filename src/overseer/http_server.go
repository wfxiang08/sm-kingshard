package overseer

import (
	"net"
	"net/http"
	"time"
)

type TcpKeepAliveListener struct {
	*net.TCPListener
}

func (ln TcpKeepAliveListener) Accept() (c net.Conn, err error) {
	tc, err := ln.AcceptTCP()
	if err != nil {
		return
	}
	tc.SetKeepAlive(true)
	tc.SetKeepAlivePeriod(3 * time.Minute)
	return tc, nil
}

// 如何利用已有的TcpListener来对外提供http服务？
func HttpServe(tcpListener *net.TCPListener, handler http.Handler) error {
	return http.Serve(&TcpKeepAliveListener{tcpListener}, handler)
}
