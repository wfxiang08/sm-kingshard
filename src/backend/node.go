package backend

import (
	"sync"
	"sync/atomic"
	"time"

	"config"
	"core/errors"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
)

const (
	Master      = "master"
	Slave       = "slave"
	SlaveSplit  = ","
	WeightSplit = "@"
)

// Node是什么概念呢?
// 1. Node对应配置文案中的node的定义，分为master, slave
// 2. 内部包含多个DB, 每一个DB有自己的可用性检测
//
type Node struct {
	Cfg config.NodeConfig

	Master           *DB
	sync.RWMutex     // 管理slaves的使用
	lastSlaveIndex   int
	validSlaveNum    int
	Slave            []*DB
	DownAfterNoAlive time.Duration
}

func (n *Node) CheckNode() {
	//to do
	//1 check connection alive
	//  每16s做一次健康检查? 能否正常工作每一个Connection也会有自己的判断, 只是ping的结果能做一个整体上的判断
	for {
		n.checkMaster()
		n.checkSlave()
		time.Sleep(16 * time.Second)
	}
}

func (n *Node) String() string {
	return n.Cfg.Name
}

// 获取Master的Connection
func (n *Node) GetMasterConn() (*BackendConn, error) {
	db := n.Master

	// 正常情况下, db不能为nil
	if db == nil {
		return nil, errors.ErrNoMasterConn
	}
	if atomic.LoadInt32(&(db.state)) == Down {
		return nil, errors.ErrMasterDown
	}

	return db.GetConn()
}

func (n *Node) checkMaster() {
	// 使用Master来检查db是否OK
	db := n.Master
	if db == nil {
		log.Errorf("Node: checkMaster Master is not alive")
		return
	}

	// 定期对数据库进行Ping
	if err := db.Ping(); err != nil {
		// Ping失败了，如何处理呢?
		log.ErrorErrorf(err, "Node: checkMaster ping db.Addr: %s", db.Addr())
	} else {
		// ping成功了，则更新状态
		db.SetLastPing()
		if atomic.LoadInt32(&(db.state)) != ManualDown {
			atomic.StoreInt32(&(db.state), Up)
		}
		return
	}

	// 标记Master数据库挂了
	if int64(n.DownAfterNoAlive) > 0 && time.Now().Unix()-db.GetLastPing() > int64(n.DownAfterNoAlive/time.Second) {
		log.Printf("Node: checkMaster Master down db.Addr: %s, Master_down_time: %d",
			db.Addr(), int64(n.DownAfterNoAlive/time.Second))
		n.DownMaster(db.addr, Down)
	}
}

func (n *Node) OpenDB(addr string) (*DB, error) {
	// 注意初始的配置
	db, err := Open(addr, n.Cfg.User, n.Cfg.Password, "", n.Cfg.MaxConnNum)
	return db, err
}

// 创建一个UpDB, 不一定能用
// 在数据库正常时，标记DB状态可用
func (n *Node) UpDB(addr string) (*DB, error) {
	db, err := n.OpenDB(addr)

	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		db.Close()
		atomic.StoreInt32(&(db.state), Down)
		return nil, err
	}
	atomic.StoreInt32(&(db.state), Up)
	return db, nil
}

// UpMaster 是通过admin 来启动
func (n *Node) UpMaster(addr string) error {
	db, err := n.UpDB(addr)
	if err != nil {
		log.ErrorErrorf(err, "Node UpMaster")
	} else {
		// 正常情况下, db不能为nil, Connection可以有问题，但是DB需要能工作
		n.Master = db
	}
	return err
}

// DownMaster 是通过admin 来关闭
func (n *Node) DownMaster(addr string, state int32) error {
	db := n.Master
	if db == nil || db.addr != addr {
		return errors.ErrNoMasterDB
	}

	// 关闭Master, 设置状态
	db.Close()
	atomic.StoreInt32(&(db.state), state)
	return nil
}

//
// ParseSlave
// ParseMaster
// 作为node和DB之间的纽带
//
func (n *Node) ParseMaster(masterStr string) error {
	var err error
	// 这个比较严重，必须返回Error
	if len(masterStr) == 0 {
		return errors.ErrNoMasterDB
	}

	// 这里似乎不能再返回Error
	n.Master, err = n.OpenDB(masterStr)
	return err
}
