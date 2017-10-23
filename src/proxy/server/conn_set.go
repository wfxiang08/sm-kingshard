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
	"fmt"
	"strings"
	"time"

	"backend"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"mysql"
	"sqlparser"
)

var nstring = sqlparser.String

func (c *ClientConn) handleSet(stmt *sqlparser.Set, sql string) (err error) {
	if len(stmt.Exprs) != 1 && len(stmt.Exprs) != 2 {
		return fmt.Errorf("must set one item once, not %s", nstring(stmt))
	}

	//log the SQL
	startTime := time.Now().UnixNano()
	defer func() {
		var state string
		if err != nil {
			state = "ERROR"
		} else {
			state = "OK"
		}
		execTime := float64(time.Now().UnixNano()-startTime) / float64(time.Millisecond)
		if execTime > float64(c.proxy.slowLogTime[c.proxy.slowLogTimeIndex]) {
			c.proxy.counter.IncrSlowLogTotal()

			log.Debug("SLOW State: %s, %.1fms, Remote:%s, SQL: %s", state, execTime,
				c.c.RemoteAddr(), sql)

		}

	}()

	k := string(stmt.Exprs[0].Name.Name)
	switch strings.ToUpper(k) {
	case `AUTOCOMMIT`, `@@AUTOCOMMIT`, `@@SESSION.AUTOCOMMIT`:
		return c.handleSetAutoCommit(stmt.Exprs[0].Expr)
	case `NAMES`,
		`CHARACTER_SET_RESULTS`, `@@CHARACTER_SET_RESULTS`, `@@SESSION.CHARACTER_SET_RESULTS`,
		`CHARACTER_SET_CLIENT`, `@@CHARACTER_SET_CLIENT`, `@@SESSION.CHARACTER_SET_CLIENT`,
		`CHARACTER_SET_CONNECTION`, `@@CHARACTER_SET_CONNECTION`, `@@SESSION.CHARACTER_SET_CONNECTION`:
		if len(stmt.Exprs) == 2 {
			//SET NAMES 'charset_name' COLLATE 'collation_name'
			return c.handleSetNames(stmt.Exprs[0].Expr, stmt.Exprs[1].Expr)
		}
		return c.handleSetNames(stmt.Exprs[0].Expr, nil)
	default:
		log.ErrorErrorf(err, "ClientConn handleSelect: %d command not supported, sql: %s", c.connectionId, sql)
		return c.writeOK(nil)
	}
}

func (c *ClientConn) handleSetAutoCommit(val sqlparser.ValExpr) error {
	flag := sqlparser.String(val)
	flag = strings.Trim(flag, "'`\"")
	// autocommit允许为 0, 1, ON, OFF, "ON", "OFF", 不允许"0", "1"
	if flag == `0` || flag == `1` {
		_, ok := val.(sqlparser.NumVal)
		if !ok {
			return fmt.Errorf("set autocommit error")
		}
	}
	switch strings.ToUpper(flag) {
	case `1`, `ON`:
		c.status |= mysql.SERVER_STATUS_AUTOCOMMIT
		if c.status&mysql.SERVER_STATUS_IN_TRANS > 0 {
			c.status &= ^mysql.SERVER_STATUS_IN_TRANS
		}
		for _, co := range c.txConns {
			if e := co.SetAutoCommit(1); e != nil {
				co.Close()
				c.txConns = make(map[*backend.Node]*backend.BackendConn)
				return fmt.Errorf("set autocommit error, %v", e)
			}
			co.Close()
		}
		c.txConns = make(map[*backend.Node]*backend.BackendConn)
	case `0`, `OFF`:
		c.status &= ^mysql.SERVER_STATUS_AUTOCOMMIT
	default:
		return fmt.Errorf("invalid autocommit flag %s", flag)
	}

	return c.writeOK(nil)
}

// golang mysql driver的实现
//// Handles parameters set in DSN after the connection is established
//func (mc *mysqlConn) handleParams() (err error) {
//	for param, val := range mc.cfg.Params {
//		switch param {
//		// Charset
//		case "charset":
//			charsets := strings.Split(val, ",")
//			for i := range charsets {
//				// ignore errors here - a charset may not exist
//				err = mc.exec("SET NAMES " + charsets[i])
//				if err == nil {
//					break
//				}
//			}
//			if err != nil {
//				return
//			}

func (c *ClientConn) handleSetNames(ch, ci sqlparser.ValExpr) error {
	var cid mysql.CollationId
	var ok bool

	value := sqlparser.String(ch)
	value = strings.Trim(value, "'`\"")

	// 如何处理charset呢?
	charset := strings.ToLower(value)
	if charset == "null" {
		return c.writeOK(nil)
	}

	// 一般情况下都不会指定ci, 因为不知道如何指定
	if ci == nil {
		if charset == "default" {
			charset = mysql.DEFAULT_CHARSET
		}
		cid, ok = mysql.CharsetIds[charset]
		if !ok {
			return fmt.Errorf("invalid charset %s", charset)
		}
	} else {
		collate := sqlparser.String(ci)
		collate = strings.Trim(collate, "'`\"")
		collate = strings.ToLower(collate)
		cid, ok = mysql.CollationNames[collate]
		if !ok {
			return fmt.Errorf("invalid collation %s", collate)
		}
	}
	c.charset = charset
	c.collation = cid

	return c.writeOK(nil)
}
