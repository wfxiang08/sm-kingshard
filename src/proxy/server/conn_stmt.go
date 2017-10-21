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
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"

	"backend"
	"core/errors"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"mysql"
	"sqlparser"
)

var paramFieldData []byte
var columnFieldData []byte

func init() {
	var p = &mysql.Field{Name: []byte("?")}
	var c = &mysql.Field{}

	paramFieldData = p.Dump()
	columnFieldData = c.Dump()
}

type Stmt struct {
	id uint32

	params  int
	columns int

	args []interface{}

	s sqlparser.Statement

	sql string
}

func (s *Stmt) ResetParams() {
	s.args = make([]interface{}, s.params)
}

func (c *ClientConn) handleStmtPrepare(sql string) error {
	if c.schema == nil {
		return mysql.NewDefaultError(mysql.ER_NO_DB_ERROR)
	}

	s := new(Stmt)

	sql = strings.TrimRight(sql, ";")

	log.Debugf("Prepare SQL: %s", sql)

	var err error
	s.s, err = sqlparser.Parse(sql)
	if err != nil {
		return fmt.Errorf(`parse sql "%s" error`, sql)
	}

	// 带有参数的sql
	s.sql = sql

	// Statement使用默认的Rule? 创建默认的连接 这个似乎没有什么错误
	n, ok := c.schema.AllNodes[c.CurrentDB]
	if !ok {
		return errors.ErrDatabaseNotSelected
	}

	// co, err := n.GetMasterConn()
	co, err := c.getBackendConn(n, false)
	defer c.closeConn(co, false)

	if err != nil {
		return fmt.Errorf("prepare error %s", err)
	}

	db := c.CurrentDB
	if len(n.Cfg.DBName) > 0 {
		db = n.Cfg.DBName
	}
	err = co.UseDB(db)
	if err != nil {
		// reset the database to null
		log.ErrorErrorf(err, "UseDB failed: %s", c.CurrentDB)
		// c.DefaultDB = ""
		return fmt.Errorf("prepare error %s", err)
	}

	t, err := co.Prepare(sql)
	if err != nil {
		return fmt.Errorf("prepare error %s", err)
	}
	s.params = t.ParamNum()
	s.columns = t.ColumnNum()

	s.id = c.stmtId
	c.stmtId++

	// 告诉Client, Statement创建成功
	if err = c.writePrepare(s); err != nil {
		return err
	}

	s.ResetParams()

	// Client获取一个Statement对象信息即可
	c.stmts[s.id] = s

	// 告诉MySQL: 关闭Statement??
	err = co.ClosePrepare(t.GetId())
	if err != nil {
		return err
	}

	return nil
}

func (c *ClientConn) writePrepare(s *Stmt) error {
	var err error
	data := make([]byte, 4, 128)
	total := make([]byte, 0, 1024)
	//status ok
	data = append(data, 0)
	//stmt id
	data = append(data, mysql.Uint32ToBytes(s.id)...)
	//number columns
	data = append(data, mysql.Uint16ToBytes(uint16(s.columns))...)
	//number params
	data = append(data, mysql.Uint16ToBytes(uint16(s.params))...)
	//filter [00]
	data = append(data, 0)
	//warning count
	data = append(data, 0, 0)

	total, err = c.writePacketBatch(total, data, false)
	if err != nil {
		return err
	}

	if s.params > 0 {
		for i := 0; i < s.params; i++ {
			data = data[0:4]
			data = append(data, []byte(paramFieldData)...)

			total, err = c.writePacketBatch(total, data, false)
			if err != nil {
				return err
			}
		}

		total, err = c.writeEOFBatch(total, c.status, false)
		if err != nil {
			return err
		}
	}

	if s.columns > 0 {
		for i := 0; i < s.columns; i++ {
			data = data[0:4]
			data = append(data, []byte(columnFieldData)...)

			total, err = c.writePacketBatch(total, data, false)
			if err != nil {
				return err
			}
		}

		total, err = c.writeEOFBatch(total, c.status, false)
		if err != nil {
			return err
		}

	}
	total, err = c.writePacketBatch(total, nil, true)
	total = nil
	if err != nil {
		return err
	}
	return nil
}

// https://dev.mysql.com/doc/internals/en/com-stmt-prepare.html
// https://dev.mysql.com/doc/internals/en/com-stmt-execute.html
//
func (c *ClientConn) handleStmtExecute(data []byte) error {
	if len(data) < 9 {
		return mysql.ErrMalformPacket
	}

	//COM_STMT_EXECUTE
	//  execute a prepared statement
	//
	//  direction: client -> server
	//  response: COM_STMT_EXECUTE Response
	//
	//  payload:
	//    1              [17] COM_STMT_EXECUTE
	//    4              stmt-id
	//    1              flags
	//    4              iteration-count
	//      if num-params > 0:
	//    n              NULL-bitmap, length: (num-params+7)/8
	//    1              new-params-bound-flag
	//      if new-params-bound-flag == 1:
	//    n              type of each parameter, length: num-params * 2
	//    n              value of each parameter

	pos := 0
	id := binary.LittleEndian.Uint32(data[0:4])
	pos += 4

	// 如果这个statement已经被prepare过了，则直接返回OK
	s, ok := c.stmts[id]
	if !ok {
		return mysql.NewDefaultError(mysql.ER_UNKNOWN_STMT_HANDLER,
			strconv.FormatUint(uint64(id), 10), "stmt_execute")
	}

	log.Debugf("Statement Exec: %s", s.sql)
	flag := data[pos]
	pos++
	//now we only support CURSOR_TYPE_NO_CURSOR flag
	//其他的: CURSOR_TYPE_READ_ONLY 等
	//
	if flag != 0 {
		return mysql.NewError(mysql.ER_UNKNOWN_ERROR, fmt.Sprintf("unsupported flag %d", flag))
	}

	//skip iteration-count, always 1
	pos += 4

	var nullBitmaps []byte
	var paramTypes []byte
	var paramValues []byte

	// 在prepare statement时就确定了
	paramNum := s.params

	if paramNum > 0 {
		// BitmapLen 向上取整： (params + 7) / 8
		nullBitmapLen := (s.params + 7) >> 3
		if len(data) < (pos + nullBitmapLen + 1) {
			return mysql.ErrMalformPacket
		}
		nullBitmaps = data[pos : pos+nullBitmapLen]
		pos += nullBitmapLen

		// new param bound flag
		if data[pos] == 1 {
			pos++
			if len(data) < (pos + (paramNum << 1)) {
				return mysql.ErrMalformPacket
			}

			// 这是什么概念呢?
			// 1. the type as in Protocol::ColumnType
			// 2. a flag byte which has the highest bit set if the type is unsigned [80]
			// 每个paramType两个字节的意义
			//
			paramTypes = data[pos : pos+(paramNum<<1)]
			pos += (paramNum << 1)

			paramValues = data[pos:]
		}

		// Statement绑定参数
		if err := c.bindStmtArgs(s, nullBitmaps, paramTypes, paramValues); err != nil {
			return err
		}
	}

	var err error

	switch stmt := s.s.(type) {
	case *sqlparser.Select:
		err = c.handlePrepareSelect(stmt, s.sql, s.args)
	case *sqlparser.Insert:
		err = c.handlePrepareExec(s.s, s.sql, s.args)
	case *sqlparser.Update:
		err = c.handlePrepareExec(s.s, s.sql, s.args)
	case *sqlparser.Delete:
		err = c.handlePrepareExec(s.s, s.sql, s.args)
	case *sqlparser.Replace:
		err = c.handlePrepareExec(s.s, s.sql, s.args)
	default:
		err = fmt.Errorf("command %T not supported now", stmt)
	}

	s.ResetParams()

	return err
}

// http://php.net/manual/en/pdo.prepare.php
// 如何处理呢?
func (c *ClientConn) getStatementSelectNode(stmt *sqlparser.Select,
	sql string, args []interface{}) (*backend.Node, error) {

	node, ok := c.schema.AllNodes[c.CurrentDB]
	if ok {
		return node, nil
	} else {
		return nil, errors.ErrDatabaseNotSelected
	}

}
func (c *ClientConn) getStatementExecNode(stmt sqlparser.Statement,
	sql string, args []interface{}) (*backend.Node, error) {
	node, ok := c.schema.AllNodes[c.CurrentDB]
	if ok {
		return node, nil
	} else {
		return nil, errors.ErrDatabaseNotSelected
	}

}

func (c *ClientConn) handlePrepareSelect(stmt *sqlparser.Select, sql string, args []interface{}) error {
	execNode, err1 := c.getStatementSelectNode(stmt, sql, args)
	if err1 != nil {
		return err1
	}
	// 如何Choose一个连接呢?
	//choose connection in slave DB first
	// 这里存在问题:
	conn, err := c.getBackendConn(execNode, true)

	defer c.closeConn(conn, false)
	if err != nil {
		return err
	}

	if conn == nil {
		r := c.newEmptyResultset(stmt)
		return c.writeResultset(c.status, r)
	}

	// 直接在指定的conn上执行sql,
	var rs []*mysql.Result
	rs, err = c.executeInNode(conn, sql, args)
	if err != nil {
		log.ErrorErrorf(err, "ClientConn handlePrepareSelect: %d", c.connectionId)
		return err
	}

	status := c.status | rs[0].Status
	if rs[0].Resultset != nil {
		err = c.writeResultset(status, rs[0].Resultset)
	} else {
		r := c.newEmptyResultset(stmt)
		err = c.writeResultset(status, r)
	}

	return err
}

func (c *ClientConn) handlePrepareExec(stmt sqlparser.Statement, sql string, args []interface{}) error {

	execNode, err1 := c.getStatementExecNode(stmt, sql, args)
	if err1 != nil {
		return err1
	}

	//execute in Master DB
	conn, err := c.getBackendConn(execNode, false)
	defer c.closeConn(conn, false)
	if err != nil {
		return err
	}

	if conn == nil {
		return c.writeOK(nil)
	}

	var rs []*mysql.Result
	rs, err = c.executeInNode(conn, sql, args)
	c.closeConn(conn, false)

	if err != nil {
		log.ErrorErrorf(err, "ClientConn handlePrepareExec: %d", c.connectionId)
		return err
	}

	status := c.status | rs[0].Status
	if rs[0].Resultset != nil {
		err = c.writeResultset(status, rs[0].Resultset)
	} else {
		err = c.writeOK(rs[0])
	}

	return err
}

//
// 如何绑定Stmt的参数呢?
//
func (c *ClientConn) bindStmtArgs(s *Stmt, nullBitmap, paramTypes, paramValues []byte) error {
	args := s.args

	pos := 0

	var v []byte
	var n int = 0
	var isNull bool
	var err error

	for i := 0; i < s.params; i++ {
		if nullBitmap[i>>3]&(1<<(uint(i)%8)) > 0 {
			args[i] = nil
			continue
		}

		tp := paramTypes[i<<1]
		isUnsigned := (paramTypes[(i<<1)+1] & 0x80) > 0

		switch tp {
		case mysql.MYSQL_TYPE_NULL:
			args[i] = nil
			continue

		case mysql.MYSQL_TYPE_TINY:
			if len(paramValues) < (pos + 1) {
				return mysql.ErrMalformPacket
			}

			if isUnsigned {
				args[i] = uint8(paramValues[pos])
			} else {
				args[i] = int8(paramValues[pos])
			}

			pos++
			continue

		case mysql.MYSQL_TYPE_SHORT, mysql.MYSQL_TYPE_YEAR:
			if len(paramValues) < (pos + 2) {
				return mysql.ErrMalformPacket
			}

			if isUnsigned {
				args[i] = uint16(binary.LittleEndian.Uint16(paramValues[pos : pos+2]))
			} else {
				args[i] = int16((binary.LittleEndian.Uint16(paramValues[pos : pos+2])))
			}
			pos += 2
			continue

		case mysql.MYSQL_TYPE_INT24, mysql.MYSQL_TYPE_LONG:
			if len(paramValues) < (pos + 4) {
				return mysql.ErrMalformPacket
			}

			if isUnsigned {
				args[i] = uint32(binary.LittleEndian.Uint32(paramValues[pos : pos+4]))
			} else {
				args[i] = int32(binary.LittleEndian.Uint32(paramValues[pos : pos+4]))
			}
			pos += 4
			continue

		case mysql.MYSQL_TYPE_LONGLONG:
			if len(paramValues) < (pos + 8) {
				return mysql.ErrMalformPacket
			}

			if isUnsigned {
				args[i] = binary.LittleEndian.Uint64(paramValues[pos : pos+8])
			} else {
				args[i] = int64(binary.LittleEndian.Uint64(paramValues[pos : pos+8]))
			}
			pos += 8
			continue

		case mysql.MYSQL_TYPE_FLOAT:
			if len(paramValues) < (pos + 4) {
				return mysql.ErrMalformPacket
			}

			args[i] = float32(math.Float32frombits(binary.LittleEndian.Uint32(paramValues[pos : pos+4])))
			pos += 4
			continue

		case mysql.MYSQL_TYPE_DOUBLE:
			if len(paramValues) < (pos + 8) {
				return mysql.ErrMalformPacket
			}

			args[i] = math.Float64frombits(binary.LittleEndian.Uint64(paramValues[pos : pos+8]))
			pos += 8
			continue

		case mysql.MYSQL_TYPE_DECIMAL, mysql.MYSQL_TYPE_NEWDECIMAL, mysql.MYSQL_TYPE_VARCHAR,
			mysql.MYSQL_TYPE_BIT, mysql.MYSQL_TYPE_ENUM, mysql.MYSQL_TYPE_SET, mysql.MYSQL_TYPE_TINY_BLOB,
			mysql.MYSQL_TYPE_MEDIUM_BLOB, mysql.MYSQL_TYPE_LONG_BLOB, mysql.MYSQL_TYPE_BLOB,
			mysql.MYSQL_TYPE_VAR_STRING, mysql.MYSQL_TYPE_STRING, mysql.MYSQL_TYPE_GEOMETRY,
			mysql.MYSQL_TYPE_DATE, mysql.MYSQL_TYPE_NEWDATE,
			mysql.MYSQL_TYPE_TIMESTAMP, mysql.MYSQL_TYPE_DATETIME, mysql.MYSQL_TYPE_TIME:
			if len(paramValues) < (pos + 1) {
				return mysql.ErrMalformPacket
			}

			v, isNull, n, err = mysql.LengthEnodedString(paramValues[pos:])
			pos += n
			if err != nil {
				return err
			}

			if !isNull {
				args[i] = v
				continue
			} else {
				args[i] = nil
				continue
			}
		default:
			return fmt.Errorf("Stmt Unknown FieldType %d", tp)
		}
	}
	return nil
}

// 参考: https://dev.mysql.com/doc/internals/en/com-stmt-send-long-data.html
func (c *ClientConn) handleStmtSendLongData(data []byte) error {
	if len(data) < 6 {
		return mysql.ErrMalformPacket
	}

	id := binary.LittleEndian.Uint32(data[0:4])

	s, ok := c.stmts[id]
	if !ok {
		return mysql.NewDefaultError(mysql.ER_UNKNOWN_STMT_HANDLER,
			strconv.FormatUint(uint64(id), 10), "stmt_send_longdata")
	}

	paramId := binary.LittleEndian.Uint16(data[4:6])
	if paramId >= uint16(s.params) {
		return mysql.NewDefaultError(mysql.ER_WRONG_ARGUMENTS, "stmt_send_longdata")
	}

	if s.args[paramId] == nil {
		s.args[paramId] = data[6:]
	} else {
		if b, ok := s.args[paramId].([]byte); ok {
			b = append(b, data[6:]...)
			s.args[paramId] = b
		} else {
			return fmt.Errorf("invalid param long data type %T", s.args[paramId])
		}
	}

	return nil
}

// https://dev.mysql.com/doc/internals/en/com-stmt-close.html
func (c *ClientConn) handleStmtReset(data []byte) error {
	if len(data) < 4 {
		return mysql.ErrMalformPacket
	}

	// 这里大量使用LittleEndian
	id := binary.LittleEndian.Uint32(data[0:4])

	s, ok := c.stmts[id]
	if !ok {
		return mysql.NewDefaultError(mysql.ER_UNKNOWN_STMT_HANDLER,
			strconv.FormatUint(uint64(id), 10), "stmt_reset")
	}

	s.ResetParams()

	return c.writeOK(nil)
}

func (c *ClientConn) handleStmtClose(data []byte) error {
	if len(data) < 4 {
		return nil
	}

	id := binary.LittleEndian.Uint32(data[0:4])

	delete(c.stmts, id)

	return nil
}
