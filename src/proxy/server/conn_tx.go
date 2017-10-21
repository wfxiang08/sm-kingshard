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
	"mysql"
)

func (c *ClientConn) isInTransaction() bool {
	// 数据的事务模型:
	// AutoCommit模式下，就没有显示的事务实现了，提交就完事
	// 非AutoCommit模式下，需要通过Begin等显示地设置状态
	return c.status&mysql.SERVER_STATUS_IN_TRANS > 0 ||
		!c.isAutoCommit()
}

func (c *ClientConn) isAutoCommit() bool {
	return c.status&mysql.SERVER_STATUS_AUTOCOMMIT > 0
}

// Client开启事务, 然后所有的txConns都开始事务
func (c *ClientConn) handleBegin() error {
	for _, co := range c.txConns {
		if err := co.Begin(); err != nil {
			return err
		}
	}
	c.status |= mysql.SERVER_STATUS_IN_TRANS
	return c.writeOK(nil)
}

func (c *ClientConn) handleCommit() (err error) {
	if err := c.commit(); err != nil {
		return err
	} else {
		return c.writeOK(nil)
	}
}

func (c *ClientConn) handleRollback() (err error) {
	if err := c.rollback(); err != nil {
		return err
	} else {
		return c.writeOK(nil)
	}
}

func (c *ClientConn) commit() (err error) {
	c.status &= ^mysql.SERVER_STATUS_IN_TRANS

	for _, co := range c.txConns {
		if e := co.Commit(); e != nil {
			err = e
		}
		co.Close()
	}

	// TODO: 这个是什么问题呢?
	// 重置后端的Connection?
	c.txConns = make(map[*backend.Node]*backend.BackendConn)
	return
}

func (c *ClientConn) rollback() (err error) {
	c.status &= ^mysql.SERVER_STATUS_IN_TRANS

	// 关闭到所有后端的Transactions
	for _, co := range c.txConns {
		if e := co.Rollback(); e != nil {
			err = e
		}
		co.Close()
	}

	// TODO: 这个是什么问题呢?
	// 重置后端的Connection?
	c.txConns = make(map[*backend.Node]*backend.BackendConn)
	return
}
