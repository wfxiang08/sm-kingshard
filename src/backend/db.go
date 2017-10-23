package backend

import (
	"sync"
	"sync/atomic"
	"time"

	"core/errors"
	"fmt"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"mysql"
)

const (
	Up         = iota
	Down
	ManualDown
	Unknown

	InitConnCount           = 16
	DefaultMaxConnNum       = 1024
	kPingPeroid       int64 = 60 // 1分钟内都认为是可靠的
)

var dbMap map[string]*DB
var dbMutex sync.Mutex

func init() {
	dbMap = make(map[string]*DB)
}

// DB和golang DB比较类似，后端维持一个连接池
// 两个概念：
//   DB
//     BackendConn
//   DB是没有失败的概念的; BackendConn 可以失败; 如果BackendConn失败了，可以标记整个DB不可用
//
//  DB如何复用已有的连接?
//
type DB struct {
	sync.RWMutex // 用于管理 connsClosed & connsOpen 的一致性

	// 一个DB，对应后台的一个数据库，可能是Master，也可能是Slave
	addr     string
	user     string
	password string
	//
	// DB复用的基础, DB的默认的DbName没有实际作用, 因为下面的代码保证了DbName始终和预期一致
	// ClientConn#getBackendConn
	//
	// 这个是一个临时属性(每次代码执行时都会修改)
	DbName string
	state  int32

	MaxConnNum  int
	InitConnNum int

	// 一个DB后面对应着多个Connection
	// connsOpen 复用可用的Connection
	// connsClosed 复用Connection对象，避免内存分配
	connsClosed chan *Conn
	connsOpen   chan *Conn

	// 专门用于做Ping等处理的
	pingMutex   sync.Mutex
	lastChecked time.Time
	lastError   error
	checkConn   *Conn
	lastPing    time.Time
}

//
// 创建一个DB
//
func Open(addr string, user string, password string, dbName string, maxConnNum int) (*DB, error) {
	//
	// DB复用的基础, DB的默认的DbName没有实际作用, 因为下面的代码保证了DbName始终和预期一致
	// ClientConn#getBackendConn
	//
	key := fmt.Sprintf("%s:%s", addr, user)
	dbMutex.Lock()
	defer dbMutex.Unlock()

	if db, ok := dbMap[key]; ok {
		return db, nil
	}

	db := new(DB)
	db.addr = addr
	db.user = user
	db.password = password
	db.DbName = dbName

	// InitConnNum 和 MaxConnNum
	// 如何处理最大连接数呢?
	if 0 < maxConnNum {
		db.MaxConnNum = maxConnNum
		if db.MaxConnNum < 16 {
			db.InitConnNum = db.MaxConnNum
		} else {
			db.InitConnNum = db.MaxConnNum / 4
		}
	} else {
		db.MaxConnNum = DefaultMaxConnNum
		db.InitConnNum = InitConnCount
	}

	// 创建两个 chan
	db.connsClosed = make(chan *Conn, db.MaxConnNum)
	db.connsOpen = make(chan *Conn, db.MaxConnNum)

	// DB的初始状态是不确定的
	// 需要通过 checkNode 来调整
	atomic.StoreInt32(&(db.state), Unknown)

	// 初始连接的创建，如果创建失败，则停止
	for i := 0; i < db.InitConnNum; i++ {
		conn, err := db.newConn()
		if err == nil {
			conn.pushTimestamp = time.Now().Unix()
			db.connsOpen <- conn
		} else {
			log.ErrorErrorf(err, "Create initConn failed")
			break
		}
	}
	db.SetLastPing()

	// 设置缓存: 同一个Key不重复创建DB
	dbMap[key] = db
	return db, nil
}

func (db *DB) Addr() string {
	return db.addr
}

func (db *DB) State() string {
	var state string
	switch db.state {
	case Up:
		state = "up"
	case Down, ManualDown:
		state = "down"
	case Unknown:
		state = "unknow"
	}
	return state
}

func (db *DB) IdleConnCount() int {
	db.RLock()
	defer db.RUnlock()
	return len(db.connsOpen)
}

func (db *DB) Close() error {

	db.Lock()
	db.Unlock()

	close(db.connsOpen)
	for conn := range db.connsOpen {
		db.closeConn(conn)
	}
	close(db.connsClosed)

	// 重新初始化
	db.connsClosed = make(chan *Conn, db.MaxConnNum)
	db.connsOpen = make(chan *Conn, db.MaxConnNum)

	return nil
}

//
// 通过 checkConn 来专门实现ping的需求
//
func (db *DB) Ping() error {

	db.pingMutex.Lock()
	defer db.pingMutex.Unlock()

	// 如果最近1s内检查过，则直接返回上次的结果
	if time.Now().Sub(db.lastChecked) < time.Second {
		return db.lastError
	}

	// 更新本地的检测结果
	db.lastChecked = time.Now()
	var err error

	// 专门的checkConn
	if db.checkConn == nil {
		db.checkConn, err = db.newConn()
		if err != nil {
			db.closeConn(db.checkConn)
			db.checkConn = nil
			db.lastError = err
			return err
		}
	}
	err = db.checkConn.Ping()
	if err != nil {
		db.closeConn(db.checkConn)
		db.checkConn = nil
		db.lastError = err
		return err
	}
	db.lastError = nil
	return nil
}

//
// 创建一个到后端MySQL
//
func (db *DB) newConn() (*Conn, error) {
	co := new(Conn)
	if err := co.Connect(db.addr, db.user, db.password, db.DbName); err != nil {
		return nil, err
	}

	return co, nil
}

func (db *DB) closeConn(co *Conn) error {
	if co != nil {
		co.Close()

		// 回收Conn对象(closed)
		if db.connsClosed != nil {
			select {
			case db.connsClosed <- co:
				return nil
			default:
				return nil
			}
		}
	}
	return nil
}

//
// 将Conn的状态恢复成为正常状态, 一般连接上是没有问题的，但是连接状态上需要注意
//
func (db *DB) tryReuse(co *Conn) error {
	var err error
	//reuse Connection
	if co.IsInTransaction() {
		//we can not reuse a connection in transaction status
		err = co.Rollback()
		if err != nil {
			return err
		}
	}

	if !co.IsAutoCommit() {
		//we can not  reuse a connection not in autocomit
		_, err = co.exec("set autocommit = 1")
		if err != nil {
			return err
		}
	}

	// DEFAULT_CHARSET 必须在配置文件中设置
	if co.GetCharset() != mysql.DEFAULT_CHARSET {
		err = co.SetCharset(mysql.DEFAULT_CHARSET, mysql.DEFAULT_COLLATION_ID)
		if err != nil {
			return err
		}
	}

	return nil
}

// 如何复用已有的连接呢?
func (db *DB) PopConn() (*Conn, error) {
	var co *Conn
	var err error

	// t0 := time.Now()

	// 1. 获取"可联通的"的Conn
	co, err = db.GetConnFromIdle()
	// t1 := time.Now()
	if err != nil {
		return nil, err
	}

	// 2. 确保Conn状态可用，例如：事务状态，字符集
	err = db.tryReuse(co) // Reuse基本上不耗时间
	// t2 := time.Now()
	if err != nil {
		db.closeConn(co)
		return nil, err
	}

	// log.Debugf("PopConn elapsed, conn: %.3fms, reuse: %.3fms", utils.ElapsedMillSeconds(t0, t1), utils.ElapsedMillSeconds(t1, t2))
	//buf := make([]byte, 1<<16)
	//runtime.Stack(buf, false)
	//log.Printf("%s", buf)
	return co, nil
}

//
// 获取一个新的Connection
//
func (db *DB) GetConnFromIdle() (*Conn, error) {
	var co *Conn
	var err error

	db.RLock()
	defer db.RUnlock()

	if db.connsOpen == nil || db.connsClosed == nil {
		return nil, errors.ErrDatabaseClose
	}

	select {
	// 1. 优先直接复用可以直接使用的Conn
	case co = <-db.connsOpen:
		if co == nil {
			// 一般情况下，不应该为nil; 为nil可能表示要退出?
			return nil, errors.ErrConnIsNil
		}
		return db.tryEnsureConn(co)
	default:
		select {
		// 2. 复用: idleConns & cacheConns
		case co = <-db.connsClosed:
			// 重新打开
			err = co.Connect(db.addr, db.user, db.password, db.DbName)
			if err != nil {
				db.closeConn(co)
				return nil, err
			}
			return co, nil
		case co = <-db.connsOpen:
			if co == nil {
				return nil, errors.ErrConnIsNil
			}
			return db.tryEnsureConn(co)
		default:
			// 没有获取缓存
			log.Warnf("GetConnFromIdle, Get conn in default")
			//new connection
			return db.newConn()
		}
	}
}

/**
 * 尽可能保证数据可用
 */
func (db *DB) tryEnsureConn(co *Conn) (*Conn, error) {
	if co != nil && kPingPeroid < time.Now().Unix()-co.pushTimestamp {
		//t0 := time.Now()
		err := co.Ping()
		if err != nil {
			// 1. 异常的Conn, 关闭
			db.closeConn(co)
			//t1 := time.Now()
			// 2. 重新连接
			err = co.Connect(db.addr, db.user, db.password, db.DbName)
			if err != nil {
				db.closeConn(co)
				return nil, err
			}
			//t2 := time.Now()
			//log.Debugf("tryEnsureConn elapsed, ping: %.3fms, Conn: %s", utils.ElapsedMillSeconds(t0, t1),
			//	utils.ElapsedMillSeconds(t1, t2))
		}
		//else {
		//	t1 := time.Now()
		//	log.Debugf("tryEnsureConn elapsed, ping: %.3fms", utils.ElapsedMillSeconds(t0, t1))
		//}
	}

	// 直接认为当前的co是可用的，最坏情况是以连接失败结果返回给用户
	return co, nil
}

//
// 用完之后归还Connection, 如果不Cache, 则直接关闭
//
func (db *DB) PushConn(co *Conn, err error) {
	if co == nil {
		return
	}

	db.RLock()
	defer db.RUnlock()

	// 1. 不回收了，直接关闭
	if db.connsOpen == nil {
		co.Close()
		return
	}
	// 2. 出错了，直接关闭
	if err != nil {
		db.closeConn(co)
		return
	}

	// 3. 正常的Conn, 回收
	co.pushTimestamp = time.Now().Unix()
	select {
	case db.connsOpen <- co:
		return
	default:
		db.closeConn(co)
		return
	}
}

// 后端连接：
type BackendConn struct {
	*Conn
	db *DB
}

func (p *BackendConn) Close() {
	if p != nil && p.Conn != nil {
		if p.Conn.pkgErr != nil {
			p.db.closeConn(p.Conn)
		} else {
			p.db.PushConn(p.Conn, nil)
		}
		p.Conn = nil
	}
}

func (db *DB) GetConn() (*BackendConn, error) {
	c, err := db.PopConn()
	if err != nil {
		return nil, err
	}
	return &BackendConn{c, db}, nil
}

func (db *DB) SetLastPing() {
	db.lastPing = time.Now()
}

func (db *DB) GetLastPing() time.Time {
	return db.lastPing
}
