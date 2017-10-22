package backend

import (
	"core/errors"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"strings"
	"sync/atomic"
	"time"
)

// 获取一个Slave? DB的选择很重要
func (n *Node) GetSlaveConn() (*BackendConn, error) {
	n.Lock()
	// 获取下一个Slave
	db, err := n.getNextSlave()
	n.Unlock()

	if err != nil {
		return nil, err
	}

	// 可能失败，也可能是Down? 如果是Down, 为什么要返回呢?
	if db == nil {
		return nil, errors.ErrNoSlaveDB
	}

	// Down/ManualDown 都是Down吧?
	if atomic.LoadInt32(&(db.state)) != Up {
		return nil, errors.ErrSlaveDown
	}

	return db.GetConn()
}

func (n *Node) checkSlave() {
	n.RLock()
	defer n.RUnlock()

	if n.Slave == nil {
		return
	}

	// 拷贝一份
	slaves := make([]*DB, len(n.Slave))
	copy(slaves, n.Slave)

	checkedIndex := 0
	invalidIndex := len(slaves)
	for i := 0; i < len(slaves); i++ {
		if err := slaves[checkedIndex].Ping(); err != nil {
			// 如果是ping出现错误, 则暂时认为没有问题, 知道时间间隔太大
			downSlave := int64(n.DownAfterNoAlive) > 0 && time.Now().Unix()-slaves[invalidIndex].GetLastPing() > int64(n.DownAfterNoAlive/time.Second)
			log.Printf("Node checkSlave, Slave down: %s, slave_down_time: %d", slaves[checkedIndex].Addr(), int64(n.DownAfterNoAlive/time.Second))

			if downSlave || atomic.LoadInt32(&(slaves[checkedIndex].state)) != Up {
				invalidIndex--
				atomic.StoreInt32(&(slaves[checkedIndex].state), Down)
				slaves[checkedIndex], slaves[invalidIndex] = slaves[invalidIndex], slaves[checkedIndex]
			} else {
				// 正常的状态延续
				checkedIndex++
			}
		} else {
			slaves[checkedIndex].SetLastPing()

			if atomic.LoadInt32(&(slaves[checkedIndex].state)) != ManualDown {
				atomic.StoreInt32(&(slaves[checkedIndex].state), Up)
				checkedIndex++
			} else {
				// 如果不能上线，则和被下线待遇一样
				invalidIndex--
				slaves[checkedIndex], slaves[invalidIndex] = slaves[invalidIndex], slaves[checkedIndex]
			}
			continue
		}
	}

}

// 添加一个Slave
func (n *Node) AddSlave(addr string) error {
	var db *DB
	var err error
	if len(addr) == 0 {
		return errors.ErrAddressNull
	}

	n.Lock()
	defer n.Unlock()
	for _, v := range n.Slave {
		if v.addr == addr {
			return errors.ErrSlaveExist
		}
	}

	// 添加新的slave
	if db, err = n.OpenDB(addr); err != nil {
		return err
	} else {
		// 不改变可用状态，有checkSlave来处理
		n.Slave = append(n.Slave, db)
		return nil
	}
}

// 删除一个Slave, InitBalancer
func (n *Node) DeleteSlave(addr string) error {
	var i int
	n.Lock()
	defer n.Unlock()

	slaveCount := len(n.Slave)
	s := make([]*DB, 0, slaveCount)

	validSlaveNum := n.validSlaveNum
	for i = 0; i < len(n.Slave); i++ {
		if n.Slave[i].addr != addr {
			s = append(s, n.Slave[i])
		} else {
			n.Slave[i].Close()
			if i < n.validSlaveNum {
				validSlaveNum--
			}
		}
	}
	n.Slave = s
	n.validSlaveNum = validSlaveNum

	return nil
}

// DownSlave 是通过admin 来关闭
func (n *Node) DownSlave(addr string, state int32) error {
	n.Lock()
	defer n.Unlock()

	if n.Slave == nil {
		return errors.ErrNoSlaveDB
	}

	for index, slave := range n.Slave {
		if slave.addr == addr {
			// 关闭对应的Slave? 然后呢设置状态
			slave.Close()
			if index >= n.validSlaveNum {
				// 如果本身有效，则调整状态
				if index != n.validSlaveNum-1 {
					n.Slave[index], n.Slave[n.validSlaveNum-1] = n.Slave[n.validSlaveNum-1], n.Slave[index]
				}
				n.validSlaveNum--
			}
			atomic.StoreInt32(&(slave.state), state)
			break
		}
	}
	return nil
}

//slaveStr(127.0.0.1:3306,192.168.0.12:3306)
//
// ParseSlave
// ParseMaster
// 作为node和DB之间的纽带
//
func (n *Node) ParseSlave(slave string) error {
	var db *DB
	var err error

	if len(slave) == 0 {
		return nil
	}
	// 逗号分隔的字符串
	slave = strings.Trim(slave, SlaveSplit)
	slaveArray := strings.Split(slave, SlaveSplit)
	count := len(slaveArray)
	n.Slave = make([]*DB, 0, count)

	//parse addr and weight
	for i := 0; i < count; i++ {
		// 正常情况下DB创建是不会失败的
		if db, err = n.OpenDB(slaveArray[i]); err != nil {
			return err
		}
		n.Slave = append(n.Slave, db)
	}
	return nil
}

// UpSlave 是通过admin 来开启
func (n *Node) UpSlave(addr string) error {
	db, err := n.UpDB(addr)
	if err != nil {
		log.ErrorErrorf(err, "Node UpSlave")
		return err
	}

	// 写锁定
	n.Lock()
	defer n.Unlock()

	for k, slave := range n.Slave {
		if slave.addr == addr {
			n.Slave[k] = db // 直接替换对应的DB
			return nil
		}
	}
	// 否则添加一个DB
	n.Slave = append(n.Slave, db)
	return err
}
