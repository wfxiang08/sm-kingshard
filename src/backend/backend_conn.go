package backend

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"mysql"
	"net"
	"strings"
)

//
// 负责连接到后端MySQL服务器
//
//proxy <-> mysql server
type Conn struct {
	conn net.Conn

	pkg *mysql.PacketIO

	addr      string
	user      string
	password  string
	CurrentDB string

	capability uint32

	status uint16

	collation mysql.CollationId
	charset   string
	salt      []byte

	pushTimestamp int64
	pkgErr        error
}

//
// 使用给定的参数连接到后端数据库
//
func (c *Conn) Connect(addr string, user string, password string, db string) error {
	c.addr = addr
	c.user = user
	c.password = password
	c.CurrentDB = db // 这个一般不会改变

	// 可以通过Config文件的charset来修改
	c.collation = mysql.DEFAULT_COLLATION_ID
	c.charset = mysql.DEFAULT_CHARSET

	//t0 := time.Now()
	err := c.ReConnect()
	//t1 := time.Now()
	// log.Debugf("Connect address: %s elapsed: %.3fms, DB: %s", addr, utils.ElapsedMillSeconds(t0, t1), db)
	return err
}

func (c *Conn) ReConnect() error {
	if c.conn != nil {
		c.conn.Close()
	}

	// tcp, unix协议的支持
	n := "tcp"
	if strings.Contains(c.addr, "/") {
		n = "unix"
	}

	netConn, err := net.Dial(n, c.addr)
	if err != nil {
		return err
	}

	tcpConn := netConn.(*net.TCPConn)

	//
	// 没有使用sql db connection
	// 直接通过tcp connection 来访问MySQL
	//
	//SetNoDelay controls whether the operating system should delay packet transmission
	// in hopes of sending fewer packets (Nagle's algorithm).
	// The default is true (no delay),
	// meaning that data is sent as soon as possible after a Write.
	//I set this option false.
	// 为什么呢?
	tcpConn.SetNoDelay(false)
	tcpConn.SetKeepAlive(true)
	c.conn = tcpConn

	// 如何创建MySQL的链接呢?
	// TCP Connection/Unix Socket --> pkg
	c.pkg = mysql.NewPacketIO(tcpConn)

	// 建立连接
	// 此处是作为client去连接Remote的Server, 因此需要先读取Handshake, 然后再回复
	if err := c.readInitialHandshake(); err != nil {
		c.conn.Close()
		return err
	}

	if err := c.writeAuthHandshake(); err != nil {
		c.conn.Close()

		return err
	}

	if _, err := c.readOK(); err != nil {
		c.conn.Close()

		return err
	}

	//we must always use autocommit
	//默认是autocommit, 如果不是这样，会有很多问题需要考虑
	if !c.IsAutoCommit() {
		if _, err := c.exec("set autocommit = 1"); err != nil {
			c.conn.Close()

			return err
		}
	}

	return nil
}

func (c *Conn) Close() error {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
		c.salt = nil
		c.pkgErr = nil
	}

	return nil
}

func (c *Conn) readPacket() ([]byte, error) {
	d, err := c.pkg.ReadPacket()
	c.pkgErr = err
	return d, err
}

func (c *Conn) writePacket(data []byte) error {
	err := c.pkg.WritePacket(data)
	c.pkgErr = err
	return err
}

func (c *Conn) readInitialHandshake() error {
	data, err := c.readPacket()
	if err != nil {
		return err
	}

	if data[0] == mysql.ERR_HEADER {
		return errors.New("read initial handshake error")
	}

	if data[0] < mysql.MinProtocolVersion {
		return fmt.Errorf("invalid protocol version %d, must >= 10", data[0])
	}

	//skip mysql version and connection id
	//mysql version end with 0x00
	//connection id length is 4
	pos := 1 + bytes.IndexByte(data[1:], 0x00) + 1 + 4

	c.salt = append(c.salt, data[pos:pos+8]...)

	//skip filter
	pos += 8 + 1

	//capability lower 2 bytes
	c.capability = uint32(binary.LittleEndian.Uint16(data[pos : pos+2]))

	pos += 2

	if len(data) > pos {
		//skip server charset
		//c.charset = data[pos]
		pos += 1

		c.status = binary.LittleEndian.Uint16(data[pos : pos+2])
		pos += 2

		c.capability = uint32(binary.LittleEndian.Uint16(data[pos:pos+2]))<<16 | c.capability

		pos += 2

		//skip auth data len or [00]
		//skip reserved (all [00])
		pos += 10 + 1

		// The documentation is ambiguous about the length.
		// The official Python library uses the fixed length 12
		// mysql-proxy also use 12
		// which is not documented but seems to work.
		c.salt = append(c.salt, data[pos:pos+12]...)
	}

	return nil
}

func (c *Conn) writeAuthHandshake() error {
	// Adjust client capability flags based on server support
	capability := mysql.CLIENT_PROTOCOL_41 | mysql.CLIENT_SECURE_CONNECTION |
		mysql.CLIENT_LONG_PASSWORD | mysql.CLIENT_TRANSACTIONS | mysql.CLIENT_LONG_FLAG

	capability &= c.capability

	//packet length
	//capbility 4
	//max-packet size 4
	//charset 1
	//reserved all[0] 23
	length := 4 + 4 + 1 + 23

	//username
	length += len(c.user) + 1

	//we only support secure connection
	auth := mysql.CalcPassword(c.salt, []byte(c.password))

	length += 1 + len(auth)

	// 握手时指定CurrrentDB
	if len(c.CurrentDB) > 0 {
		capability |= mysql.CLIENT_CONNECT_WITH_DB
		length += len(c.CurrentDB) + 1
	}

	c.capability = capability

	data := make([]byte, length+4)

	//capability [32 bit]
	data[4] = byte(capability)
	data[5] = byte(capability >> 8)
	data[6] = byte(capability >> 16)
	data[7] = byte(capability >> 24)

	//MaxPacketSize [32 bit] (none)
	//data[8] = 0x00
	//data[9] = 0x00
	//data[10] = 0x00
	//data[11] = 0x00

	//Charset [1 byte]
	data[12] = byte(c.collation)

	//Filler [23 bytes] (all 0x00)
	pos := 13 + 23

	//User [null terminated string]
	if len(c.user) > 0 {
		pos += copy(data[pos:], c.user)
	}
	//data[pos] = 0x00
	pos++

	// auth [length encoded integer]
	data[pos] = byte(len(auth))
	pos += 1 + copy(data[pos+1:], auth)

	// db [null terminated string]
	if len(c.CurrentDB) > 0 {
		pos += copy(data[pos:], c.CurrentDB)
		//data[pos] = 0x00
	}

	return c.writePacket(data)
}

func (c *Conn) writeCommand(command byte) error {
	c.pkg.Sequence = 0

	return c.writePacket([]byte{
		0x01, //1 bytes long
		0x00,
		0x00,
		0x00, //sequence
		command,
	})
}

func (c *Conn) writeCommandBuf(command byte, arg []byte) error {
	c.pkg.Sequence = 0

	length := len(arg) + 1

	data := make([]byte, length+4)

	data[4] = command

	copy(data[5:], arg)

	return c.writePacket(data)
}

func (c *Conn) writeCommandStr(command byte, arg string) error {
	c.pkg.Sequence = 0

	length := len(arg) + 1

	data := make([]byte, length+4)

	data[4] = command

	copy(data[5:], arg)

	return c.writePacket(data)
}

func (c *Conn) writeCommandUint32(command byte, arg uint32) error {
	c.pkg.Sequence = 0

	return c.writePacket([]byte{
		0x05, //5 bytes long
		0x00,
		0x00,
		0x00, //sequence

		command,

		byte(arg),
		byte(arg >> 8),
		byte(arg >> 16),
		byte(arg >> 24),
	})
}

func (c *Conn) writeCommandStrStr(command byte, arg1 string, arg2 string) error {
	c.pkg.Sequence = 0

	data := make([]byte, 4, 6+len(arg1)+len(arg2))

	data = append(data, command)
	data = append(data, arg1...)
	data = append(data, 0)
	data = append(data, arg2...)

	return c.writePacket(data)
}

func (c *Conn) Ping() error {
	if err := c.writeCommand(mysql.COM_PING); err != nil {
		return err
	}

	if _, err := c.readOK(); err != nil {
		return err
	}

	return nil
}

// 建立连接之后选择 database
func (c *Conn) UseDB(dbName string) error {
	// log.Printf("CurrentDB: %s vs. NewDB: %s", c.CurrentDB, dbName)
	// 为了保证时间效率，Conn的DB最好保持不变；不要再底层为不同的DB复用Connection
	if c.CurrentDB == dbName || len(dbName) == 0 {
		return nil
	}

	if err := c.writeCommandStr(mysql.COM_INIT_DB, dbName); err != nil {
		return err
	}

	if _, err := c.readOK(); err != nil {
		return err
	}

	c.CurrentDB = dbName
	return nil
}

func (c *Conn) GetDB() string {
	return c.CurrentDB
}

func (c *Conn) GetAddr() string {
	return c.addr
}

// 如何执行命令?
func (c *Conn) Execute(command string, args ...interface{}) (*mysql.Result, error) {
	if len(args) == 0 {
		return c.exec(command)
	} else {
		if s, err := c.Prepare(command); err != nil {
			return nil, err
		} else {
			var r *mysql.Result
			r, err = s.Execute(args...)
			s.Close()
			return r, err
		}
	}
}

func (c *Conn) ClosePrepare(id uint32) error {
	return c.writeCommandUint32(mysql.COM_STMT_CLOSE, id)
}

// 事务相关的操作
func (c *Conn) Begin() error {
	_, err := c.exec("begin")
	return err
}

func (c *Conn) Commit() error {
	_, err := c.exec("commit")
	return err
}

func (c *Conn) Rollback() error {
	_, err := c.exec("rollback")
	return err
}

// 设置AutoCommit
func (c *Conn) SetAutoCommit(n uint8) error {
	if n == 0 {
		if _, err := c.exec("set autocommit = 0"); err != nil {
			c.conn.Close()

			return err
		}
	} else {
		if _, err := c.exec("set autocommit = 1"); err != nil {
			c.conn.Close()

			return err
		}
	}
	return nil
}

// 重置字符集合
func (c *Conn) SetCharset(charset string, collation mysql.CollationId) error {
	charset = strings.Trim(charset, "\"'`")
	if charset == "utf8" && c.charset == "utf8mb4" {
		// 如果客户端使用utf8, proxy为utf8mb4, 那么以proxy为准(反正utf8mb4是一个mysql上的概念，对php, go等没有影响）
		// 反过来则由危险，客户端期待utfmb4, 而proxy只实现utf8, 则会出现问题
		return nil
	}

	if collation == 0 {
		collation = mysql.CollationNames[mysql.Charsets[charset]]
	}

	if c.charset == charset && c.collation == collation {
		return nil
	}

	log.Debugf("Charset not match: %s vs. (client)%s, id: %d vs. %d", c.charset, charset)

	_, ok := mysql.CharsetIds[charset]
	if !ok {
		return fmt.Errorf("invalid charset %s", charset)
	}

	_, ok = mysql.Collations[collation]
	if !ok {
		return fmt.Errorf("invalid collation %s", collation)
	}

	if _, err := c.exec(fmt.Sprintf("SET NAMES %s COLLATE %s", charset, mysql.Collations[collation])); err != nil {
		return err
	} else {
		c.collation = collation
		c.charset = charset
		return nil
	}
}

func (c *Conn) FieldList(table string, wildcard string) ([]*mysql.Field, error) {
	if err := c.writeCommandStrStr(mysql.COM_FIELD_LIST, table, wildcard); err != nil {
		return nil, err
	}

	data, err := c.readPacket()
	if err != nil {
		return nil, err
	}

	fs := make([]*mysql.Field, 0, 4)
	var f *mysql.Field
	if data[0] == mysql.ERR_HEADER {
		return nil, c.handleErrorPacket(data)
	} else {
		for {
			if data, err = c.readPacket(); err != nil {
				return nil, err
			}

			// EOF Packet
			if c.isEOFPacket(data) {
				return fs, nil
			}

			if f, err = mysql.FieldData(data).Parse(); err != nil {
				return nil, err
			}
			fs = append(fs, f)
		}
	}
	return nil, fmt.Errorf("field list error")
}

// 直接执行Query --> writeCommandStr
func (c *Conn) exec(query string) (*mysql.Result, error) {
	// 直接执行Query
	if err := c.writeCommandStr(mysql.COM_QUERY, query); err != nil {
		return nil, err
	}

	// 读取执行的结果
	return c.readResult(false)
}

func (c *Conn) readResultset(data []byte, binary bool) (*mysql.Result, error) {
	result := &mysql.Result{
		Status:       0,
		InsertId:     0,
		AffectedRows: 0,

		Resultset: &mysql.Resultset{},
	}

	// column count
	count, _, n := mysql.LengthEncodedInt(data)

	if n-len(data) != 0 {
		return nil, mysql.ErrMalformPacket
	}

	result.Fields = make([]*mysql.Field, count)
	result.FieldNames = make(map[string]int, count)

	// 读取Columns
	if err := c.readResultColumns(result); err != nil {
		return nil, err
	}

	// 读取Rows
	if err := c.readResultRows(result, binary); err != nil {
		return nil, err
	}

	return result, nil
}

func (c *Conn) readResultColumns(result *mysql.Result) (err error) {
	var i int = 0
	var data []byte

	for {
		data, err = c.readPacket()
		if err != nil {
			return
		}

		// EOF Packet
		if c.isEOFPacket(data) {
			if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
				//result.Warnings = binary.LittleEndian.Uint16(data[1:])
				//todo add strict_mode, warning will be treat as error
				result.Status = binary.LittleEndian.Uint16(data[3:])
				c.status = result.Status
			}

			if i != len(result.Fields) {
				err = mysql.ErrMalformPacket
			}

			return
		}

		result.Fields[i], err = mysql.FieldData(data).Parse()
		if err != nil {
			return
		}

		result.FieldNames[string(result.Fields[i].Name)] = i

		i++
	}
}

func (c *Conn) readResultRows(result *mysql.Result, isBinary bool) (err error) {
	//
	// 读取RowDatas & status
	//
	var data []byte
	for {
		data, err = c.readPacket()

		if err != nil {
			return
		}

		// EOF Packet
		if c.isEOFPacket(data) {
			if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
				//result.Warnings = binary.LittleEndian.Uint16(data[1:])
				//todo add strict_mode, warning will be treat as error
				result.Status = binary.LittleEndian.Uint16(data[3:])
				c.status = result.Status
			}

			break
		}

		result.RowDatas = append(result.RowDatas, data)
	}

	// 如何将RowDatas解析成为Values呢?
	result.Values = make([][]interface{}, len(result.RowDatas))
	for i := range result.Values {
		result.Values[i], err = result.RowDatas[i].Parse(result.Fields, isBinary)

		if err != nil {
			return err
		}
	}

	return nil
}

// 读取Packet
func (c *Conn) readUntilEOF() (err error) {
	var data []byte

	for {
		data, err = c.readPacket()

		if err != nil {
			return
		}

		// EOF Packet
		if c.isEOFPacket(data) {
			return
		}
	}
	return
}

func (c *Conn) isEOFPacket(data []byte) bool {
	return data[0] == mysql.EOF_HEADER && len(data) <= 5
}

func (c *Conn) handleOKPacket(data []byte) (*mysql.Result, error) {
	var n int
	var pos int = 1

	r := new(mysql.Result)

	r.AffectedRows, _, n = mysql.LengthEncodedInt(data[pos:])
	pos += n
	r.InsertId, _, n = mysql.LengthEncodedInt(data[pos:])
	pos += n

	if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		r.Status = binary.LittleEndian.Uint16(data[pos:])
		c.status = r.Status
		pos += 2

		//todo:strict_mode, check warnings as error
		//Warnings := binary.LittleEndian.Uint16(data[pos:])
		//pos += 2
	} else if c.capability&mysql.CLIENT_TRANSACTIONS > 0 {
		r.Status = binary.LittleEndian.Uint16(data[pos:])
		c.status = r.Status
		pos += 2
	}

	//info
	return r, nil
}

func (c *Conn) handleErrorPacket(data []byte) error {
	e := new(mysql.SqlError)

	var pos int = 1

	e.Code = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	if c.capability&mysql.CLIENT_PROTOCOL_41 > 0 {
		//skip '#'
		pos++
		e.State = string(data[pos : pos+5])
		pos += 5
	}

	e.Message = string(data[pos:])

	return e
}

func (c *Conn) readOK() (*mysql.Result, error) {
	data, err := c.readPacket()
	if err != nil {
		return nil, err
	}

	if data[0] == mysql.OK_HEADER {
		return c.handleOKPacket(data)
	} else if data[0] == mysql.ERR_HEADER {
		return nil, c.handleErrorPacket(data)
	} else {
		return nil, errors.New("invalid ok packet")
	}
}

func (c *Conn) readResult(binary bool) (*mysql.Result, error) {
	data, err := c.readPacket()
	if err != nil {
		return nil, err
	}

	// 如何处理返回的数据呢?
	if data[0] == mysql.OK_HEADER {
		return c.handleOKPacket(data)
	} else if data[0] == mysql.ERR_HEADER {
		return nil, c.handleErrorPacket(data)
	} else if data[0] == mysql.LocalInFile_HEADER {
		return nil, mysql.ErrMalformPacket
	}

	// 其他的情况? 直接返回？？
	return c.readResultset(data, binary)
}

func (c *Conn) IsAutoCommit() bool {
	return c.status&mysql.SERVER_STATUS_AUTOCOMMIT > 0
}

func (c *Conn) IsInTransaction() bool {
	return c.status&mysql.SERVER_STATUS_IN_TRANS > 0
}

func (c *Conn) GetCharset() string {
	return c.charset
}
