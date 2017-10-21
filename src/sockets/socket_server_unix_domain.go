package sockets

import (
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"net"
	"os"
	"time"
	"github.com/wfxiang08/cyutils/utils"
)

type TServerUnixDomain struct {
	listener      net.Listener
	addr          net.Addr
	clientTimeout time.Duration
}

func NewTServerUnixDomain(listenAddr string) (*TServerUnixDomain, error) {
	return NewTServerUnixDomainTimeout(listenAddr, 0)
}

func NewTServerUnixDomainTimeout(listenAddr string, clientTimeout time.Duration) (*TServerUnixDomain, error) {

	// 删除之前已经存在的问题
	if utils.FileExist(listenAddr) {
		os.Remove(listenAddr)
	}

	addr, err := net.ResolveUnixAddr("unix", listenAddr)
	if err != nil {
		return nil, err
	}
	return &TServerUnixDomain{addr: addr, clientTimeout: clientTimeout}, nil
}

func (p *TServerUnixDomain) Listen() (net.Listener, error) {
	if p.listener != nil {
		return p.listener, nil
	}
	l, err := net.Listen(p.addr.Network(), p.addr.String())
	if err != nil {
		return nil, err
	}
	p.listener = l

	log.Printf("Network: %s, Addr: %s", p.addr.Network(), p.addr.String())

	// 注意: 该Socket需要给所有需要访问该接口的人以读写的权限
	// 因此最终的 sock文件的权限为: 0777
	// 例如: aa.sock root/root 07777
	//      换一个用户，rm aa.sock 似乎无效
	filePath := p.addr.String()
	os.Chmod(filePath, os.ModePerm)

	return p.listener, nil
}