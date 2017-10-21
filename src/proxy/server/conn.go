// Copyright 2016 The kingshard Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package server

import (
	"backend"
	"bytes"
	"core/hack"
	"encoding/binary"
	"fmt"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"mysql"
	"net"
	"runtime"
	"sync"
	"github.com/wfxiang08/cyutils/utils/atomic2"
)

//client <-> proxy
type ClientConn struct {
	sync.Mutex

	pkg          *mysql.PacketIO

	c            net.Conn

	proxy        *Server

	capability   uint32

	connectionId uint32

	status       uint16
	collation    mysql.CollationId
	charset      string

	user         string
	CurrentDB    string           // 当前Connection上默认的db

	salt         []byte

	schema       *Schema

	txConns      map[*backend.Node]*backend.BackendConn

	closed       atomic2.Bool

	lastInsertId int64
	affectedRows int64

	stmtId       uint32

	stmts        map[uint32]*Stmt //prepare相关,client端到proxy的stmt
}

var DEFAULT_CAPABILITY uint32 = mysql.CLIENT_LONG_PASSWORD | mysql.CLIENT_LONG_FLAG |
		mysql.CLIENT_CONNECT_WITH_DB | mysql.CLIENT_PROTOCOL_41 |
		mysql.CLIENT_TRANSACTIONS | mysql.CLIENT_SECURE_CONNECTION

var baseConnId uint32 = 10000

func (c *ClientConn) IsAllowConnect() bool {
	// 获取RemoteIP
	remoteIp := c.c.RemoteAddr().String()
	// 如果是通过unix domain socket访问，则直接允许
	if len(remoteIp) < 3 {
		return true
	}

	// log.Printf("Remote Ip: %s", remoteIp)
	clientHost, _, err := net.SplitHostPort(remoteIp)
	if err != nil {
		log.ErrorError(err, "SplitHostport failed")
	}
	clientIP := net.ParseIP(clientHost)

	// 判断是否支持Ip限制，如果没有限制，则直接放行
	// 如果有限制，则做一个过滤
	ipVec := c.proxy.allowips[c.proxy.allowipsIndex]
	if ipVecLen := len(ipVec); ipVecLen == 0 {
		return true
	}
	for _, ip := range ipVec {
		if ip.Equal(clientIP) {
			return true
		}
	}

	log.Errorf("server IsAllowConnect: %d, ip address: %s access denied by kindshard.", mysql.ER_ACCESS_DENIED_ERROR, c.c.RemoteAddr().String())
	return false
}

//
// MySQL如何握手呢?
//
func (c *ClientConn) Handshake() error {

	// 服务器Handshake到Client
	if err := c.writeInitialHandshake(); err != nil {
		log.ErrorErrorf(err, "server Handshake %v msg: send initial handshake error", c.connectionId)
		return err
	}

	// 等待Client回复
	if err := c.readHandshakeResponse(); err != nil {
		log.ErrorErrorf(err, "server readHandshakeResponse %v, msg: read Handshake Response error",
			c.connectionId)
		return err
	}

	// OK就完事
	if err := c.writeOK(nil); err != nil {
		log.ErrorErrorf(err, "server %v readHandshakeResponse write ok fail",
			c.connectionId)
		return err
	}

	c.pkg.Sequence = 0
	return nil
}

// 多次调用不会有副作用
func (c *ClientConn) Close() error {

	if c.closed.CompareAndSwap(false, true) {
		log.Debugf("Close client connection: %d", c.connectionId)
		c.c.Close()
	}
	return nil
}

func (c *ClientConn) writeInitialHandshake() error {
	// 前4个bytes作用?
	data := make([]byte, 4, 128)

	//min version 10
	data = append(data, 10)

	//server version[00]
	data = append(data, mysql.ServerVersion...)
	data = append(data, 0)

	// connection id
	// Connection的标志:
	data = append(data, byte(c.connectionId), byte(c.connectionId >> 8), byte(c.connectionId >> 16), byte(c.connectionId >> 24))

	// auth-plugin-data-part-1
	data = append(data, c.salt[0:8]...)

	//filter [00]
	data = append(data, 0)

	//capability flag lower 2 bytes, using default capability here
	//
	// DEFAULT_CAPABILITY 的4个字节，在不同地方发送
	//
	data = append(data, byte(DEFAULT_CAPABILITY), byte(DEFAULT_CAPABILITY >> 8))

	// TODO: 需要定制, 可能优先支持utf8mb4编码
	//charset, utf-8 default
	data = append(data, uint8(mysql.DEFAULT_COLLATION_ID))

	//status
	data = append(data, byte(c.status), byte(c.status >> 8))

	//below 13 byte may not be used
	//capability flag upper 2 bytes, using default capability here
	data = append(data, byte(DEFAULT_CAPABILITY >> 16), byte(DEFAULT_CAPABILITY >> 24))

	//filter [0x15], for wireshark dump, value is 0x15
	data = append(data, 0x15)

	//reserved 10 [00]
	data = append(data, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

	//auth-plugin-data-part-2
	data = append(data, c.salt[8:]...)

	//filter [00]
	data = append(data, 0)

	// 最终的Payload是通过Packet一口气写出去的
	return c.writePacket(data)
}

// Packet的读写
func (c *ClientConn) readPacket() ([]byte, error) {
	return c.pkg.ReadPacket()
}

func (c *ClientConn) writePacket(data []byte) error {
	return c.pkg.WritePacket(data)
}

// Packets的一口气写完
func (c *ClientConn) writePacketBatch(total, data []byte, direct bool) ([]byte, error) {
	return c.pkg.WritePacketBatch(total, data, direct)
}

func (c *ClientConn) readHandshakeResponse() error {
	// 读取Packet
	data, err := c.readPacket()

	if err != nil {
		return err
	}

	pos := 0

	//capability
	c.capability = binary.LittleEndian.Uint32(data[:4])
	pos += 4

	//skip max packet size
	pos += 4

	//charset, skip, if you want to use another charset, use set names
	//c.collation = CollationId(data[pos])
	pos++

	//skip reserved 23[00]
	pos += 23

	//user name
	c.user = string(data[pos : pos + bytes.IndexByte(data[pos:], 0)])

	pos += len(c.user) + 1

	//auth length and auth
	authLen := int(data[pos])
	pos++
	auth := data[pos : pos + authLen]

	//
	// 校验用户名和密码
	// TODO: 可以支持多个用户名和密码
	//
	checkAuth := mysql.CalcPassword(c.salt, []byte(c.proxy.cfg.Password))
	if c.user != c.proxy.cfg.User || !bytes.Equal(auth, checkAuth) {
		log.Errorf("ClientConn readHandshakeResponse: auth: failed ")
		return mysql.NewDefaultError(mysql.ER_ACCESS_DENIED_ERROR, c.user, c.c.RemoteAddr().String(), "Yes")
	}

	pos += authLen

	// mysql -u root -pxxx -P9900 -h127.0.0.1 shard_sm
	// 在初始连接中使用database name
	var db string
	if c.capability & mysql.CLIENT_CONNECT_WITH_DB > 0 {
		if len(data[pos:]) == 0 {
			return nil
		}

		db = string(data[pos : pos + bytes.IndexByte(data[pos:], 0)])
		pos += len(c.CurrentDB) + 1

	}

	c.CurrentDB = db

	return nil
}

//
// ClientConn 如何运转呢？
//
func (c *ClientConn) Run() {
	// 处理异常? 这些代码能否统一起来呢?
	defer func() {
		r := recover()
		if err, ok := r.(error); ok {
			const size = 4096
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)]

			log.ErrorErrorf(err, "ClientConn Run: %v", string(buf))
		}

		c.Close()
	}()

	for {
		// 读取来自Client的请求
		data, err := c.readPacket()

		// 出错，则Over
		if err != nil {
			return
		}

		// 分配请求: 有点点像Codis的模式
		if err := c.dispatch(data); err != nil {
			c.proxy.counter.IncrErrLogTotal()

			// 直接报一行错误
			if err1, ok := err.(*mysql.SqlError); ok {
				log.Errorf("Server run connectionId: %v, SQL Error: %s", c.connectionId, err1.Error())
			} else {
				log.ErrorErrorf(err, "Server run connectionId: %v", c.connectionId)
			}

			// 给客户端报错，结束Connection
			c.writeError(err)
			if err == mysql.ErrBadConn {
				c.Close()
			}
		}

		// 如果close, 则暂停
		if c.closed.Get() {
			return
		}

		// Sequence 限定在一个交互中，一个Payload过大时， Sequence会比较有用，其他情况下, Sequence都为0
		c.pkg.Sequence = 0
	}
}

//
// 收到client的请求，如何处理呢？
//
func (c *ClientConn) dispatch(data []byte) error {
	c.proxy.counter.IncrClientQPS()

	// log.Debugf("ClientConn: %s", string(data))

	// 读取Command和data
	cmd := data[0]
	data = data[1:]

	switch cmd {
	case mysql.COM_QUIT:
		// 退出(回滚当前的事务)
		c.handleRollback()
		c.Close()
		return nil

	case mysql.COM_QUERY:
		// 解析执行Query
		// 简单的SQL语句
		return c.handleQuery(hack.String(data))

	case mysql.COM_PING:
		// 如果是Ping, 则直接返回Client OK
		return c.writeOK(nil)

	case mysql.COM_INIT_DB:
		//
		return c.handleUseDB(hack.String(data))

	case mysql.COM_FIELD_LIST:
		return c.handleFieldList(data)

	// Statement的使用
	// 不支持在Sharding模式下使用，单表还是可以考虑的
	case mysql.COM_STMT_PREPARE:
		return c.handleStmtPrepare(hack.String(data))

	case mysql.COM_STMT_EXECUTE:
		// 参考: https://dev.mysql.com/doc/internals/en/com-stmt-execute.html
		return c.handleStmtExecute(data)
	case mysql.COM_STMT_CLOSE:
		// 如何关闭Statement呢？
		return c.handleStmtClose(data)
	case mysql.COM_STMT_SEND_LONG_DATA:
		return c.handleStmtSendLongData(data)
	case mysql.COM_STMT_RESET:
		return c.handleStmtReset(data)

	case mysql.COM_SET_OPTION:
		return c.writeEOF(0)

	default:
		// 其他的Command暂时不支持
		msg := fmt.Sprintf("command %d not supported now", cmd)
		log.Errorf("ClientConn dispatch: %s", msg)
		return mysql.NewError(mysql.ER_UNKNOWN_ERROR, msg)
	}

	return nil
}

// 返回数据给Client
func (c *ClientConn) writeOK(r *mysql.Result) error {
	if r == nil {
		r = &mysql.Result{Status: c.status}
	}
	data := make([]byte, 4, 32)

	data = append(data, mysql.OK_HEADER)

	// 成功返回数据
	data = append(data, mysql.PutLengthEncodedInt(r.AffectedRows)...)
	data = append(data, mysql.PutLengthEncodedInt(r.InsertId)...)

	if c.capability & mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, byte(r.Status), byte(r.Status >> 8))
		data = append(data, 0, 0)
	}

	return c.writePacket(data)
}

func (c *ClientConn) writeError(e error) error {
	var m *mysql.SqlError
	var ok bool
	if m, ok = e.(*mysql.SqlError); !ok {
		m = mysql.NewError(mysql.ER_UNKNOWN_ERROR, e.Error())
	}

	data := make([]byte, 4, 16 + len(m.Message))

	data = append(data, mysql.ERR_HEADER)
	data = append(data, byte(m.Code), byte(m.Code >> 8))

	if c.capability & mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, '#')
		data = append(data, m.State...)
	}

	data = append(data, m.Message...)

	return c.writePacket(data)
}

func (c *ClientConn) writeEOF(status uint16) error {
	data := make([]byte, 4, 9)

	data = append(data, mysql.EOF_HEADER)
	if c.capability & mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, 0, 0)
		data = append(data, byte(status), byte(status >> 8))
	}

	return c.writePacket(data)
}

func (c *ClientConn) writeEOFBatch(total []byte, status uint16, direct bool) ([]byte, error) {
	data := make([]byte, 4, 9)

	data = append(data, mysql.EOF_HEADER)
	if c.capability & mysql.CLIENT_PROTOCOL_41 > 0 {
		data = append(data, 0, 0)
		data = append(data, byte(status), byte(status >> 8))
	}

	return c.writePacketBatch(total, data, direct)
}
