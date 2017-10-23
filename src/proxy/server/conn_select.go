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
	"bytes"
	"core/errors"
	"core/hack"
	"fmt"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"mysql"
	"sqlparser"
	"strconv"
	"strings"
	"time"
	"utils"
)

const (
	MasterComment    = "/*master*/"
	SumFunc          = "sum"
	CountFunc        = "count"
	MaxFunc          = "max"
	MinFunc          = "min"
	LastInsertIdFunc = "last_insert_id"
	FUNC_EXIST       = 1
)

var funcNameMap = map[string]int{
	"sum":            FUNC_EXIST,
	"count":          FUNC_EXIST,
	"max":            FUNC_EXIST,
	"min":            FUNC_EXIST,
	"last_insert_id": FUNC_EXIST,
}

func (c *ClientConn) handleFieldList(data []byte) error {
	index := bytes.IndexByte(data, 0x00)
	table := string(data[0:index])
	wildcard := string(data[index+1:])

	if c.schema == nil {
		return mysql.NewDefaultError(mysql.ER_NO_DB_ERROR)
	}

	if len(c.CurrentDB) == 0 {
		return mysql.NewDefaultError(mysql.ER_BAD_DB_ERROR, "Current DB empty")
	}

	// log.Printf("handleFieldList for %s@%s", table, c.CurrentDB)
	// mysql -uxxx -hxxx -Pxxx 执行完毕show tables之后似乎会执行这个查询(每个表都执行一次)
	// 第一次连接时会这样
	// 使用第一个Node来获取状态
	n := c.schema.GetNode(c.CurrentDB, true)
	if n == nil {
		return mysql.NewDefaultError(mysql.ER_BAD_DB_ERROR, "Current DB Invalid: "+c.CurrentDB)
	}

	co, err := n.GetMasterConn()
	defer c.closeConn(co, false)
	if err != nil {
		return err
	}

	// 后端实际的Database， 则采用Node的DBName
	db := c.CurrentDB
	if len(n.Cfg.DBName) > 0 {
		db = n.Cfg.DBName
	}

	if err = co.UseDB(db); err != nil {
		log.ErrorErrorf(err, "Use DB for db: %s failed", db)
		return err
	}

	if fs, err := co.FieldList(table, wildcard); err != nil {
		return err
	} else {
		return c.writeFieldList(c.status, fs)
	}
}

func (c *ClientConn) writeFieldList(status uint16, fs []*mysql.Field) error {
	c.affectedRows = int64(-1)
	var err error
	total := make([]byte, 0, 1024)
	data := make([]byte, 4, 512)

	for _, v := range fs {
		data = data[0:4]
		data = append(data, v.Dump()...)
		total, err = c.writePacketBatch(total, data, false)
		if err != nil {
			return err
		}
	}

	_, err = c.writeEOFBatch(total, status, true)
	return err
}

//处理select语句
func (c *ClientConn) handleSelect(stmt *sqlparser.Select, args []interface{}) error {
	// 1. 处理Sharding模式下的select
	t0 := time.Now()
	var fromSlave bool = true
	plan, err := c.schema.Router.BuildPlan(c.CurrentDB, stmt)
	if err != nil {
		return err
	}

	t1 := time.Now()
	// 2. 识别comment
	//    /*master*/
	if 0 < len(stmt.Comments) {
		comment := string(stmt.Comments[0])
		if 0 < len(comment) && strings.ToLower(comment) == MasterComment {
			fromSlave = false
		}
	}

	// 获取多个conns
	// 控制ping的频率
	conns, err := c.getShardConns(fromSlave, plan)

	t2 := time.Now()

	if err != nil {
		log.ErrorErrorf(err, "ClientConn handleSelect connectionId: %d", c.connectionId)
		return err
	}

	// 如果没有conns, 则返回空结果
	if conns == nil {
		r := c.newEmptyResultset(stmt)
		return c.writeResultset(c.status, r)
	}

	var rs []*mysql.Result
	// 耗时不可控
	rs, err = c.executeInMultiNodes(conns, plan.RewrittenSqls, args)
	c.closeShardConns(conns, false)
	t3 := time.Now()
	if err != nil {
		log.ErrorErrorf(err, "ClientConn handleSelect: %d", c.connectionId)
		return err
	}

	// 合并来自多个表的结果
	err = c.mergeSelectResult(rs, stmt)
	if err != nil {
		log.ErrorErrorf(err, "ClientConn handleSelect: %d", c.connectionId)
	}
	t4 := time.Now()

	log.Debugf("Select Elapsed, plan: %.3fms, conns: %.3fms, exec: %.3fms, merge: %.3fms",
		utils.ElapsedMillSeconds(t0, t1),
		utils.ElapsedMillSeconds(t1, t2),
		utils.ElapsedMillSeconds(t2, t3),
		utils.ElapsedMillSeconds(t3, t4))
	return err
}

func (c *ClientConn) mergeSelectResult(rs []*mysql.Result, stmt *sqlparser.Select) error {
	var r *mysql.Result
	var err error

	// 如果没有GroupBy
	if len(stmt.GroupBy) == 0 {
		r, err = c.buildSelectOnlyResult(rs, stmt)
	} else {
		//group by
		r, err = c.buildSelectGroupByResult(rs, stmt)
	}
	if err != nil {
		return err
	}

	// 排序
	c.sortSelectResult(r.Resultset, stmt)
	//to do, add log here, sort may error because order by key not exist in resultset fields

	// Limit
	if err := c.limitSelectResult(r.Resultset, stmt); err != nil {
		return err
	}

	return c.writeResultset(r.Status, r.Resultset)
}

//only process last_inser_id
func (c *ClientConn) handleSimpleSelect(stmt *sqlparser.SimpleSelect) error {
	nonStarExpr, _ := stmt.SelectExprs[0].(*sqlparser.NonStarExpr)
	var name string = hack.String(nonStarExpr.As)
	if name == "" {
		name = "last_insert_id()"
	}
	var column = 1
	var rows [][]string
	var names []string = []string{
		name,
	}

	var t = fmt.Sprintf("%d", c.lastInsertId)
	rows = append(rows, []string{t})

	r := new(mysql.Resultset)

	var values [][]interface{} = make([][]interface{}, len(rows))
	for i := range rows {
		values[i] = make([]interface{}, column)
		for j := range rows[i] {
			values[i][j] = rows[i][j]
		}
	}

	r, _ = c.buildResultset(nil, names, values)
	return c.writeResultset(c.status, r)
}

//build select result with group by opt
// 如何分组呢?
func (c *ClientConn) buildSelectGroupByResult(rs []*mysql.Result,
	stmt *sqlparser.Select) (*mysql.Result, error) {
	var err error
	var r *mysql.Result
	var groupByIndexs []int

	fieldLen := len(rs[0].Fields)
	startIndex := fieldLen - len(stmt.GroupBy)
	for startIndex < fieldLen {
		groupByIndexs = append(groupByIndexs, startIndex)
		startIndex++
	}

	funcExprs := c.getFuncExprs(stmt)

	// 如果没有Funcs, 则如何Merge呢?
	if len(funcExprs) == 0 {
		r, err = c.mergeGroupByWithoutFunc(rs, groupByIndexs)
	} else {
		r, err = c.mergeGroupByWithFunc(rs, groupByIndexs, funcExprs)
	}
	if err != nil {
		return nil, err
	}

	//build result
	names := make([]string, 0, 2)
	if 0 < len(r.Values) {
		r.Fields = r.Fields[:groupByIndexs[0]]
		for i := 0; i < len(r.Fields) && i < groupByIndexs[0]; i++ {
			names = append(names, string(r.Fields[i].Name))
		}
		//delete group by columns in Values
		for i := 0; i < len(r.Values); i++ {
			r.Values[i] = r.Values[i][:groupByIndexs[0]]
		}
		r.Resultset, err = c.buildResultset(r.Fields, names, r.Values)
		if err != nil {
			return nil, err
		}
	} else {
		r.Resultset = c.newEmptyResultset(stmt)
	}

	return r, nil
}

//only merge result with aggregate function in group by opt
func (c *ClientConn) mergeGroupByWithFunc(rs []*mysql.Result, groupByIndexs []int,
	funcExprs map[int]string) (*mysql.Result, error) {
	r := rs[0]
	//load rs into a map, in order to make group
	resultMap, err := c.loadResultWithFuncIntoMap(rs, groupByIndexs, funcExprs)
	if err != nil {
		return nil, err
	}

	//set status
	status := c.status
	for i := 0; i < len(rs); i++ {
		status = status | rs[i].Status
	}

	//change map into Resultset
	r.Values = nil
	r.RowDatas = nil
	for _, v := range resultMap {
		r.Values = append(r.Values, v.Value)
		r.RowDatas = append(r.RowDatas, v.RowData)
	}
	r.Status = status

	return r, nil
}

//only merge result without aggregate function in group by opt
func (c *ClientConn) mergeGroupByWithoutFunc(rs []*mysql.Result,
	groupByIndexs []int) (*mysql.Result, error) {
	r := rs[0]
	//load rs into a map
	resultMap, err := c.loadResultIntoMap(rs, groupByIndexs)
	if err != nil {
		return nil, err
	}

	//set status
	status := c.status
	for i := 0; i < len(rs); i++ {
		status = status | rs[i].Status
	}

	//load map into Resultset
	r.Values = nil
	r.RowDatas = nil
	for _, v := range resultMap {
		r.Values = append(r.Values, v.Value)
		r.RowDatas = append(r.RowDatas, v.RowData)
	}
	r.Status = status

	return r, nil
}

type ResultRow struct {
	Value   []interface{}
	RowData mysql.RowData
}

func (c *ClientConn) generateMapKey(groupColumns []interface{}) (string, error) {
	bk := make([]byte, 0, 8)
	separatorBuf, err := formatValue("+")
	if err != nil {
		return "", err
	}

	for _, v := range groupColumns {
		b, err := formatValue(v)
		if err != nil {
			return "", err
		}
		bk = append(bk, b...)
		bk = append(bk, separatorBuf...)
	}

	return string(bk), nil
}

func (c *ClientConn) loadResultIntoMap(rs []*mysql.Result,
	groupByIndexs []int) (map[string]*ResultRow, error) {
	//load Result into map
	resultMap := make(map[string]*ResultRow)
	for _, r := range rs {
		for i := 0; i < len(r.Values); i++ {
			keySlice := r.Values[i][groupByIndexs[0]:]
			mk, err := c.generateMapKey(keySlice)
			if err != nil {
				return nil, err
			}

			resultMap[mk] = &ResultRow{
				Value:   r.Values[i],
				RowData: r.RowDatas[i],
			}
		}
	}

	return resultMap, nil
}

func (c *ClientConn) loadResultWithFuncIntoMap(rs []*mysql.Result,
	groupByIndexs []int, funcExprs map[int]string) (map[string]*ResultRow, error) {

	resultMap := make(map[string]*ResultRow)
	rt := new(mysql.Result)
	rt.Resultset = new(mysql.Resultset)
	rt.Fields = rs[0].Fields

	//change Result into map
	for _, r := range rs {
		for i := 0; i < len(r.Values); i++ {
			keySlice := r.Values[i][groupByIndexs[0]:]
			mk, err := c.generateMapKey(keySlice)
			if err != nil {
				return nil, err
			}

			if v, ok := resultMap[mk]; ok {
				//init rt
				rt.Values = nil
				rt.RowDatas = nil

				//append v and r into rt, and calculate the function value
				rt.Values = append(rt.Values, r.Values[i], v.Value)
				rt.RowDatas = append(rt.RowDatas, r.RowDatas[i], v.RowData)
				resultTmp := []*mysql.Result{rt}

				for funcIndex, funcName := range funcExprs {
					funcValue, err := c.calFuncExprValue(funcName, resultTmp, funcIndex)
					if err != nil {
						return nil, err
					}
					//set the function value in group by
					resultMap[mk].Value[funcIndex] = funcValue
				}
			} else {
				//key is not exist
				resultMap[mk] = &ResultRow{
					Value:   r.Values[i],
					RowData: r.RowDatas[i],
				}
			}
		}
	}

	return resultMap, nil
}

//build select result without group by opt
func (c *ClientConn) buildSelectOnlyResult(rs []*mysql.Result, stmt *sqlparser.Select) (*mysql.Result, error) {
	var err error
	r := rs[0].Resultset
	status := c.status | rs[0].Status

	funcExprs := c.getFuncExprs(stmt)

	if len(funcExprs) == 0 {
		// 遍历其他的数据集合
		for i := 1; i < len(rs); i++ {
			status |= rs[i].Status
			// 将Values/RowsDatas合并到rs[0]上
			for j := range rs[i].Values {
				// Values和RowDatas的区别?
				r.Values = append(r.Values, rs[i].Values[j])
				r.RowDatas = append(r.RowDatas, rs[i].RowDatas[j])
			}
		}
	} else {
		//result only one row, status doesn't need set
		r, err = c.buildFuncExprResult(stmt, rs, funcExprs)
		if err != nil {
			return nil, err
		}
	}
	return &mysql.Result{
		Status:    status,
		Resultset: r,
	}, nil
}

func (c *ClientConn) sortSelectResult(r *mysql.Resultset, stmt *sqlparser.Select) error {
	if stmt.OrderBy == nil {
		return nil
	}

	sk := make([]mysql.SortKey, len(stmt.OrderBy))

	for i, o := range stmt.OrderBy {
		sk[i].Name = nstring(o.Expr)
		sk[i].Direction = o.Direction
	}

	return r.Sort(sk)
}

//
// 如何处理Limit请求呢?
//
func (c *ClientConn) limitSelectResult(r *mysql.Resultset, stmt *sqlparser.Select) error {
	if stmt.Limit == nil {
		return nil
	}

	// 计算offset
	var offset, count int64
	var err error
	if stmt.Limit.Offset == nil {
		offset = 0
	} else {
		if o, ok := stmt.Limit.Offset.(sqlparser.NumVal); !ok {
			return fmt.Errorf("invalid select limit %s", nstring(stmt.Limit))
		} else {
			if offset, err = strconv.ParseInt(hack.String([]byte(o)), 10, 64); err != nil {
				return err
			}
		}
	}

	if o, ok := stmt.Limit.Rowcount.(sqlparser.NumVal); !ok {
		return fmt.Errorf("invalid limit %s", nstring(stmt.Limit))
	} else {
		if count, err = strconv.ParseInt(hack.String([]byte(o)), 10, 64); err != nil {
			return err
		} else if count < 0 {
			return fmt.Errorf("invalid limit %s", nstring(stmt.Limit))
		}
	}
	if offset > int64(len(r.Values)) {
		r.Values = nil
		r.RowDatas = nil
		return nil
	}

	if offset+count > int64(len(r.Values)) {
		count = int64(len(r.Values)) - offset
	}

	// 最终只获取其中的一段
	// TODO: 如何offset很大，则会存在性能问题
	r.Values = r.Values[offset : offset+count]
	r.RowDatas = r.RowDatas[offset : offset+count]

	return nil
}

func (c *ClientConn) buildFuncExprResult(stmt *sqlparser.Select, rs []*mysql.Result,
	funcExprs map[int]string) (*mysql.Resultset, error) {

	var names []string
	var err error
	r := rs[0].Resultset
	funcExprValues := make(map[int]interface{})

	// 在 rs上执行每一个函数，并且将结果保存到: funcExprValues 中
	for index, funcName := range funcExprs {
		funcExprValue, err := c.calFuncExprValue(
			funcName,
			rs,
			index,
		)
		if err != nil {
			return nil, err
		}
		funcExprValues[index] = funcExprValue
	}

	// 生成合并之后的结果
	r.Values, err = c.buildFuncExprValues(rs, funcExprValues)

	if 0 < len(r.Values) {
		for _, field := range rs[0].Fields {
			names = append(names, string(field.Name))
		}
		r, err = c.buildResultset(rs[0].Fields, names, r.Values)
		if err != nil {
			return nil, err
		}
	} else {
		r = c.newEmptyResultset(stmt)
	}

	return r, nil
}

//
// 返回Statement中支持的Funcs
//
//get the index of funcExpr, the value is function name
func (c *ClientConn) getFuncExprs(stmt *sqlparser.Select) map[int]string {
	var f *sqlparser.FuncExpr
	funcExprs := make(map[int]string)

	for i, expr := range stmt.SelectExprs {
		nonStarExpr, ok := expr.(*sqlparser.NonStarExpr)
		if !ok {
			continue
		}

		f, ok = nonStarExpr.Expr.(*sqlparser.FuncExpr)
		if !ok {
			continue
		} else {
			f = nonStarExpr.Expr.(*sqlparser.FuncExpr)
			funcName := strings.ToLower(string(f.Name))
			switch funcNameMap[funcName] {
			case FUNC_EXIST:
				funcExprs[i] = funcName
			}
		}
	}
	return funcExprs
}

// 将Result中指定的字段求和
func (c *ClientConn) getSumFuncExprValue(rs []*mysql.Result, index int) (interface{}, error) {
	var sumf float64
	var sumi int64
	var IsInt bool
	var err error
	var result interface{}

	// 不同的Result
	for _, r := range rs {
		// 每一个Result下有不同的Rows
		for k := range r.Values {
			// 从每一个Row中取出第index字段的Value, 然后再求和?
			result, err = r.GetValue(k, index)
			if err != nil {
				return nil, err
			}
			if result == nil {
				continue
			}

			switch v := result.(type) {
			case int:
				sumi = sumi + int64(v)
				IsInt = true
			case int32:
				sumi = sumi + int64(v)
				IsInt = true
			case int64:
				sumi = sumi + v
				IsInt = true
			case float32:
				sumf = sumf + float64(v)
			case float64:
				sumf = sumf + v
			case []byte:
				tmp, err := strconv.ParseFloat(string(v), 64)
				if err != nil {
					return nil, err
				}

				sumf = sumf + tmp
			default:
				return nil, errors.ErrSumColumnType
			}
		}
	}
	if IsInt {
		return sumi, nil
	} else {
		return sumf, nil
	}
}

func (c *ClientConn) getMaxFuncExprValue(rs []*mysql.Result,
	index int) (interface{}, error) {
	var max interface{}
	var findNotNull bool
	if len(rs) == 0 {
		return nil, nil
	} else {
		for _, r := range rs {
			for k := range r.Values {
				result, err := r.GetValue(k, index)
				if err != nil {
					return nil, err
				}
				if result != nil {
					max = result
					findNotNull = true
					break
				}
			}
			if findNotNull {
				break
			}
		}
	}
	for _, r := range rs {
		for k := range r.Values {
			result, err := r.GetValue(k, index)
			if err != nil {
				return nil, err
			}
			if result == nil {
				continue
			}
			switch result.(type) {
			case int64:
				if max.(int64) < result.(int64) {
					max = result
				}
			case uint64:
				if max.(uint64) < result.(uint64) {
					max = result
				}
			case float64:
				if max.(float64) < result.(float64) {
					max = result
				}
			case string:
				if max.(string) < result.(string) {
					max = result
				}
			}
		}
	}
	return max, nil
}

func (c *ClientConn) getMinFuncExprValue(
	rs []*mysql.Result, index int) (interface{}, error) {
	var min interface{}
	var findNotNull bool
	if len(rs) == 0 {
		return nil, nil
	} else {
		for _, r := range rs {
			for k := range r.Values {
				result, err := r.GetValue(k, index)
				if err != nil {
					return nil, err
				}
				if result != nil {
					min = result
					findNotNull = true
					break
				}
			}
			if findNotNull {
				break
			}
		}
	}
	for _, r := range rs {
		if r != nil {
			for k := range r.Values {
				result, err := r.GetValue(k, index)
				if err != nil {
					return nil, err
				}
				if result == nil {
					continue
				}
				switch result.(type) {
				case int64:
					if min.(int64) > result.(int64) {
						min = result
					}
				case uint64:
					if min.(uint64) > result.(uint64) {
						min = result
					}
				case float64:
					if min.(float64) > result.(float64) {
						min = result
					}
				case string:
					if min.(string) > result.(string) {
						min = result
					}
				}
			}
		}
	}
	return min, nil
}

//calculate the the value funcExpr(sum or count)
//
// 如何处理Sum, Max, Min, LastInsertId等数据呢?
//
func (c *ClientConn) calFuncExprValue(funcName string, rs []*mysql.Result, index int) (interface{}, error) {

	var num int64
	switch strings.ToLower(funcName) {
	case CountFunc:
		if len(rs) == 0 {
			return nil, nil
		}
		// 汇总数据
		for _, r := range rs {
			if r != nil {
				for k := range r.Values {
					result, err := r.GetInt(k, index)
					if err != nil {
						return nil, err
					}
					num += result
				}
			}
		}
		return num, nil
	case SumFunc:
		return c.getSumFuncExprValue(rs, index)
	case MaxFunc:
		return c.getMaxFuncExprValue(rs, index)
	case MinFunc:
		return c.getMinFuncExprValue(rs, index)
	case LastInsertIdFunc:
		return c.lastInsertId, nil
	default:
		if len(rs) == 0 {
			return nil, nil
		}
		//get a non-null value of funcExpr
		for _, r := range rs {
			if r != nil {
				for k := range r.Values {
					result, err := r.GetValue(k, index)
					if err != nil {
						return nil, err
					}
					if result != nil {
						return result, nil
					}
				}
			}
		}
	}

	return nil, nil
}

//build values of resultset,only build one row
func (c *ClientConn) buildFuncExprValues(rs []*mysql.Result,
	funcExprValues map[int]interface{}) ([][]interface{}, error) {
	values := make([][]interface{}, 0, 1)
	//build a row in one result
	for i := range rs {
		for j := range rs[i].Values {
			for k := range funcExprValues {
				rs[i].Values[j][k] = funcExprValues[k]
			}
			values = append(values, rs[i].Values[j])
			if len(values) == 1 {
				break
			}
		}
		break
	}

	//generate one row just for sum or count
	if len(values) == 0 {
		value := make([]interface{}, len(rs[0].Fields))
		for k := range funcExprValues {
			value[k] = funcExprValues[k]
		}
		values = append(values, value)
	}

	return values, nil
}
