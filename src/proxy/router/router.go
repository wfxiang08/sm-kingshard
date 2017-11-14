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

package router

import (
	"config"
	"core/errors"
	"fmt"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"sqlparser"
	"strings"
)

var (
	DefaultRuleType   = "default"
	HashRuleType      = "hash"
	SMRuleType        = "sm"
	RangeRuleType     = "range"
	DateYearRuleType  = "date_year"
	DateMonthRuleType = "date_month"
	DateDayRuleType   = "date_day"
	MinMonthDaysCount = 28
	MaxMonthDaysCount = 31
	MonthsCount       = 12
)

//-
//    db : kingshard
//    table: test_shard_hash
//    key: id
//    nodes: [node1, node2]
//    type: hash
//    locations: [4,4]

//
// 定义了Proxy的路由规则， 例如：<DB, Table> --> <Key, Type, Nodes等信息>
//
type Rule struct {
	Table string
	Keys  []string // 允许指定多个Keys, 只有Keys[0]是主键，其他的Keys和Keys[0]具有相同的Sharding规则

	Type  string // Rule的类型
	Nodes []string
	// 所有的子表的Index
	// TableToNode 每个子表对应的Node的index
	SubTableIndexs     []int       //SubTableIndexs store all the index of sharding sub-table
	TableToNode        map[int]int //key is table index, and value is node index
	Shard              Shard       // 如何分Shard呢?
	AllNodeSingleTable bool        // 是否一个Node只有一个表，如果是，则Table中不带有下标
}

// Router只负责"一个Sharding DB"的路由规则
// 目前规划情况如下:
// 1. shard_db(不支持跨库事务，不支持join，即便都在同一个shard内部)
// 2. 其他独立的N个db(独立的db上的操作基本上不受限制)
//
type Router struct {
	ShardDB string
	Rules   map[string]*Rule
	Nodes   []string //just for human saw
}

func NewDefaultRule(node string) *Rule {
	var r *Rule = &Rule{
		Type:        DefaultRuleType,
		Nodes:       []string{node},
		Shard:       new(DefaultShard), // 默认的Shard算法
		TableToNode: nil,
	}
	return r
}

// 给定的Rule, 如何获取对应的Node
func (r *Rule) FindNode(key interface{}) (string, error) {
	// Shard算法能找到tableIndex
	tableIndex, err := r.Shard.FindForKey(key)
	if err != nil {
		return "", err
	}

	// 再找到node, 然后找到node name
	nodeIndex := r.TableToNode[tableIndex]
	return r.Nodes[nodeIndex], nil
}

func (r *Rule) FindNodeIndex(key interface{}) (int, error) {
	tableIndex, err := r.Shard.FindForKey(key)
	if err != nil {
		return -1, err
	}

	// 获取Node的Index
	return r.TableToNode[tableIndex], nil
}

func (r *Rule) FindTableIndex(key interface{}) (int, error) {
	return r.Shard.FindForKey(key)
}

//UpdateExprs is the expression after set
func (r *Rule) checkUpdateExprs(exprs sqlparser.UpdateExprs) error {
	// UPDATE TABLE xxx SET
	if r.Type == DefaultRuleType {
		return nil
	} else if len(r.Nodes) == 1 {
		return nil
	}

	// Update语句中不能更新Key
	for _, e := range exprs {
		if listContains(string(e.Name.Name), r.Keys) {
			return errors.ErrUpdateKey
		}
	}
	return nil
}

func listContains(name string, keys []string) bool {
	for i := 0; i < len(keys); i++ {
		if keys[i] == name {
			return true
		}
	}
	return false
}

//NewRouter build router according to the config file
func NewRouter(schemaConfig *config.SchemaConfig) (*Router, error) {

	rt := new(Router)
	rt.Nodes = schemaConfig.Nodes //对应schema中的nodes
	rt.Rules = make(map[string]*Rule)
	rt.ShardDB = schemaConfig.ShardDB

	for _, shard := range schemaConfig.ShardRule {
		// 如果shard的Nodes不指定，则默认使用Schema的Nodes
		if len(shard.Nodes) == 0 {
			shard.Nodes = rt.Nodes
		}

		// 如果没有指定Locations, 那么每个Location默认为[1, 1, 1...]
		if len(shard.Locations) == 0 {
			shard.Locations = make([]int, len(shard.Nodes))
			shardLocLen := len(shard.Locations)
			for i := 0; i < shardLocLen; i++ {
				shard.Locations[i] = 1
			}
		} else {
			// 所有的location的长度必须相同
			shardLocLen := len(shard.Locations)
			for i := 1; i < shardLocLen; i++ {
				if (shard.Locations[i] != shard.Locations[0]) {
					return nil, fmt.Errorf("all locations should be the same")
				}
			}
		}

		// log.Debugf("Nodes: %v", shard.Nodes)
		// log.Debugf("Locations: %v", shard.Locations)

		// 解析每一个shard
		for _, node := range shard.Nodes {
			if !includeNode(rt.Nodes, node) {
				return nil, fmt.Errorf("shard table[%s] node[%s] not in the schema.nodes list:[%s]",
					shard.Table, node, strings.Join(shard.Nodes, ","))
			}
		}
		rule, err := parseRule(&shard)
		if err != nil {
			return nil, err
		}

		if rule.Type == DefaultRuleType {
			return nil, fmt.Errorf("[default-rule] duplicate, must only one")
		}
		rt.Rules[rule.Table] = rule
	}
	return rt, nil
}

//
// 如何获取Rule呢?
//
func (r *Router) GetRule(db, table string) *Rule {
	arry := strings.Split(table, ".")

	// table中如果指定了db, 则以table为准
	if len(arry) == 2 {
		table = strings.Trim(arry[1], "`")
		db = strings.Trim(arry[0], "`")
	}
	// log.Debugf("GetRule db: %s, table: %s", db, table)

	// 如果没有指定db, 或者db不存在sharding rules, 则返回nil
	if len(db) == 0 || db != r.ShardDB {
		log.Debugf("DB: %s, ShardDB: %s", db, r.ShardDB)
		return nil
	}

	if rule, ok := r.Rules[table]; ok {
		// log.Debugf("DB: %s, Table: %s, Rule not null", db, table)
		return rule
	} else {
		// log.Debugf("DB: %s, Table: %s, Rule is null", db, table)
		return nil
	}
}

func parseRule(cfg *config.ShardConfig) (*Rule, error) {
	r := new(Rule)
	r.Table = cfg.Table
	r.Keys = nil
	for i := 0; i < len(cfg.Keys); i++ {
		r.Keys = append(r.Keys, strings.ToLower(cfg.Keys[i])) //ignore case
	}
	r.Type = cfg.Type
	r.Nodes = cfg.Nodes //将ruleconfig中的nodes赋值给rule
	r.TableToNode = make(map[int]int, 0)

	switch r.Type {
	case HashRuleType, SMRuleType, RangeRuleType:
		var sumTables int
		// Hash和Range中的Location个数和Nodes必须保持一致
		if len(cfg.Locations) != len(r.Nodes) {
			return nil, errors.ErrLocationsCount
		}

		isAllSingleTable := true
		// 不同的Node, 每个Node对应不同的Locations
		for i := 0; i < len(cfg.Locations); i++ {
			// Location[i]什么概念呢?
			// 存在这么多个分表
			for j := 0; j < cfg.Locations[i]; j++ {
				// 子表索引，例如: 0, 1, 2, 3, 4, ..., 11
				r.SubTableIndexs = append(r.SubTableIndexs, j+sumTables)
				// 不同的Table所归属的Node可能不一样
				r.TableToNode[j+sumTables] = i
			}
			sumTables += cfg.Locations[i]
			if cfg.Locations[i] != 1 {
				isAllSingleTable = false
			}
		}
		r.AllNodeSingleTable = isAllSingleTable

	case DateDayRuleType:
		if len(cfg.DateRange) != len(r.Nodes) {
			return nil, errors.ErrDateRangeCount
		}

		// 不同的Node对应不同的DateRange
		for i := 0; i < len(cfg.DateRange); i++ {
			dayNumbers, err := ParseDayRange(cfg.DateRange[i])
			if err != nil {
				return nil, err
			}

			for _, v := range dayNumbers {
				r.SubTableIndexs = append(r.SubTableIndexs, v)
				r.TableToNode[v] = i
			}
		}
	case DateMonthRuleType:
		if len(cfg.DateRange) != len(r.Nodes) {
			return nil, errors.ErrDateRangeCount
		}
		for i := 0; i < len(cfg.DateRange); i++ {
			monthNumbers, err := ParseMonthRange(cfg.DateRange[i])
			if err != nil {
				return nil, err
			}
			for _, v := range monthNumbers {
				r.SubTableIndexs = append(r.SubTableIndexs, v)
				r.TableToNode[v] = i
			}
		}
	case DateYearRuleType:
		if len(cfg.DateRange) != len(r.Nodes) {
			return nil, errors.ErrDateRangeCount
		}
		for i := 0; i < len(cfg.DateRange); i++ {
			yearNumbers, err := ParseYearRange(cfg.DateRange[i])
			if err != nil {
				return nil, err
			}
			for _, v := range yearNumbers {
				r.TableToNode[v] = i
				r.SubTableIndexs = append(r.SubTableIndexs, v)
			}
		}
	}

	if err := parseShard(r, cfg); err != nil {
		return nil, err
	}

	return r, nil
}

func parseShard(r *Rule, cfg *config.ShardConfig) error {
	switch r.Type {
	case HashRuleType:
		r.Shard = &HashShard{ShardNum: len(r.TableToNode)}
	case SMRuleType:
		// 我们自己的Sharding算法
		locationNum := 0
		if len(cfg.Locations) > 0 {
			locationNum = cfg.Locations[0]
		}
		r.Shard = &SMHashShard{ShardNum: len(r.TableToNode), LocationNum: uint64(locationNum)}
	case RangeRuleType:
		rs, err := ParseNumSharding(cfg.Locations, cfg.TableRowLimit)
		if err != nil {
			return err
		}

		if len(rs) != len(r.TableToNode) {
			return fmt.Errorf("range space %d not equal tables %d", len(rs), len(r.TableToNode))
		}

		r.Shard = &NumRangeShard{Shards: rs}
	case DateDayRuleType:
		r.Shard = &DateDayShard{}
	case DateMonthRuleType:
		r.Shard = &DateMonthShard{}
	case DateYearRuleType:
		r.Shard = &DateYearShard{}
	default:
		r.Shard = &DefaultShard{}
	}

	return nil
}

func includeNode(nodes []string, node string) bool {
	for _, n := range nodes {
		if n == node {
			return true
		}
	}
	return false
}

// build a router plan
func (r *Router) BuildPlan(db string, statement sqlparser.Statement) (*Plan, error) {
	//因为实现Statement接口的方法都是指针类型，所以type对应类型也是指针类型
	switch stmt := statement.(type) {
	case *sqlparser.Insert:
		return r.buildInsertPlan(db, stmt)
	case *sqlparser.Replace:
		return r.buildReplacePlan(db, stmt)

	case *sqlparser.Select:
		return r.buildSelectPlan(db, stmt)

	case *sqlparser.Update:
		return r.buildUpdatePlan(db, stmt)
	case *sqlparser.Delete:
		return r.buildDeletePlan(db, stmt)
	case *sqlparser.Truncate:
		return r.buildTruncatePlan(db, stmt)
	}
	return nil, errors.ErrNoPlan
}

//
// Proxy不支持join操作
//
func (r *Router) buildSelectPlan(db string, statement sqlparser.Statement) (*Plan, error) {
	plan := &Plan{}
	var where *sqlparser.Where
	var err error
	var tableName string

	// SELECT * FROM xxx where id IN (xxx, xxx)
	stmt := statement.(*sqlparser.Select)
	switch v := (stmt.From[0]).(type) {
	case *sqlparser.AliasedTableExpr:
		tableName = sqlparser.String(v.Expr)
	case *sqlparser.JoinTableExpr:
		if ate, ok := (v.LeftExpr).(*sqlparser.AliasedTableExpr); ok {
			tableName = sqlparser.String(ate.Expr)
		} else {
			tableName = sqlparser.String(v)
		}
	default:
		tableName = sqlparser.String(v)
	}

	// fmt.Printf("DB: %s, TableName: %s\n", db, tableName)

	plan.Rule = r.GetRule(db, tableName) //根据表名获得分表规则
	if plan.Rule == nil {
		return nil, errors.ErrDatabaseNotSelected
	}

	where = stmt.Where

	// SELECT * FROM table WHERE id > 100
	if where != nil {
		// fmt.Printf("where: %v\n", where.Expr)

		plan.Criteria = where.Expr //路由条件
		err = plan.calRouteIndexs()

		if err != nil {
			log.ErrorErrorf(err, "Route BuildSelectPlan")
			return nil, err
		}
	} else {
		// 如果没有限定条件，则查询扩展到所有的Table上来执行
		//if shard select without where,send to all nodes and all tables
		plan.RouteTableIndexs = plan.Rule.SubTableIndexs
		plan.RouteNodeIndexs = makeList(0, len(plan.Rule.Nodes))
	}

	// 如果没有字表，并且存在非默认的Rule规则，则出错了
	if plan.Rule.Type != DefaultRuleType && len(plan.RouteTableIndexs) == 0 {
		log.ErrorErrorf(errors.ErrNoCriteria, "Route BuildSelectPlan")
		return nil, errors.ErrNoCriteria
	}

	//generate sql,如果routeTableindexs为空则表示不分表，不分表则发default node
	err = r.generateSelectSql(plan, stmt)
	if err != nil {
		return nil, err
	}
	return plan, nil
}

func (r *Router) buildInsertPlan(db string, statement sqlparser.Statement) (*Plan, error) {
	plan := &Plan{}
	plan.Rows = make(map[int]sqlparser.Values)
	stmt := statement.(*sqlparser.Insert)
	if _, ok := stmt.Rows.(sqlparser.SelectStatement); ok {
		return nil, errors.ErrSelectInInsert
	}

	if stmt.Columns == nil {
		return nil, errors.ErrIRNoColumns
	}

	//根据sql语句的表，获得对应的分片规则
	plan.Rule = r.GetRule(db, sqlparser.String(stmt.Table))
	if plan.Rule == nil {
		return nil, errors.ErrDatabaseNotSelected
	}
	err := plan.GetIRKeyIndex(stmt.Columns)
	if err != nil {
		return nil, err
	}

	if stmt.OnDup != nil {
		err := plan.Rule.checkUpdateExprs(sqlparser.UpdateExprs(stmt.OnDup))
		if err != nil {
			return nil, err
		}
	}

	plan.Criteria = plan.checkValuesType(stmt.Rows.(sqlparser.Values))

	err = plan.calRouteIndexs()
	if err != nil {
		log.ErrorErrorf(err, "Route BuildInsertPlan")
		return nil, err
	}

	err = r.generateInsertSql(plan, stmt)
	if err != nil {
		return nil, err
	}
	return plan, nil
}

func (r *Router) buildUpdatePlan(db string, statement sqlparser.Statement) (*Plan, error) {
	plan := &Plan{}
	var where *sqlparser.Where

	stmt := statement.(*sqlparser.Update)
	plan.Rule = r.GetRule(db, sqlparser.String(stmt.Table))
	if plan.Rule == nil {
		return nil, errors.ErrDatabaseNotSelected
	}

	err := plan.Rule.checkUpdateExprs(stmt.Exprs)
	if err != nil {
		return nil, err
	}

	where = stmt.Where
	if where != nil {
		plan.Criteria = where.Expr //路由条件
		err = plan.calRouteIndexs()
		if err != nil {
			log.ErrorErrorf(err, "Route BuildUpdatePlan")
			return nil, err
		}
	} else {
		//if shard update without where,send to all nodes and all tables
		plan.RouteTableIndexs = plan.Rule.SubTableIndexs
		plan.RouteNodeIndexs = makeList(0, len(plan.Rule.Nodes))
	}

	if plan.Rule.Type != DefaultRuleType && len(plan.RouteTableIndexs) == 0 {
		log.ErrorErrorf(errors.ErrNoCriteria, "Route BuildUpdatePlan")
		return nil, errors.ErrNoCriteria
	}
	//generate sql,如果routeTableindexs为空则表示不分表，不分表则发default node
	err = r.generateUpdateSql(plan, stmt)
	if err != nil {
		return nil, err
	}
	return plan, nil
}

// 删除Plan
func (r *Router) buildDeletePlan(db string, statement sqlparser.Statement) (*Plan, error) {
	plan := &Plan{}
	var where *sqlparser.Where
	var err error

	stmt := statement.(*sqlparser.Delete)

	// 获取Rule
	plan.Rule = r.GetRule(db, sqlparser.String(stmt.Table))
	if plan.Rule == nil {
		return nil, errors.ErrDatabaseNotSelected
	}
	where = stmt.Where

	if where != nil {
		plan.Criteria = where.Expr //路由条件
		err = plan.calRouteIndexs()
		if err != nil {
			log.ErrorErrorf(err, "Route BuildUpdatePlan")
			return nil, err
		}
	} else {
		//if shard delete without where,send to all nodes and all tables
		plan.RouteTableIndexs = plan.Rule.SubTableIndexs
		plan.RouteNodeIndexs = makeList(0, len(plan.Rule.Nodes))
	}

	if plan.Rule.Type != DefaultRuleType && len(plan.RouteTableIndexs) == 0 {
		log.ErrorErrorf(errors.ErrNoCriteria, "Route BuildDeletePlan")
		return nil, errors.ErrNoCriteria
	}
	//generate sql,如果routeTableindexs为空则表示不分表，不分表则发default node
	err = r.generateDeleteSql(plan, stmt)
	if err != nil {
		return nil, err
	}
	return plan, nil
}

func (r *Router) buildTruncatePlan(db string, statement sqlparser.Statement) (*Plan, error) {
	plan := &Plan{}
	var err error

	stmt := statement.(*sqlparser.Truncate)
	plan.Rule = r.GetRule(db, sqlparser.String(stmt.Table))
	if plan.Rule == nil {
		return nil, errors.ErrDatabaseNotSelected
	}
	//send to all nodes and all tables
	plan.RouteTableIndexs = plan.Rule.SubTableIndexs
	plan.RouteNodeIndexs = makeList(0, len(plan.Rule.Nodes))

	if plan.Rule.Type != DefaultRuleType && len(plan.RouteTableIndexs) == 0 {
		log.ErrorErrorf(errors.ErrNoCriteria, "Route buildTruncatePlan")
		return nil, errors.ErrNoCriteria
	}
	//generate sql,如果routeTableindexs为空则表示不分表，不分表则发default node
	err = r.generateTruncateSql(plan, stmt)
	if err != nil {
		return nil, err
	}
	return plan, nil
}

func (r *Router) buildReplacePlan(db string, statement sqlparser.Statement) (*Plan, error) {
	plan := &Plan{}
	plan.Rows = make(map[int]sqlparser.Values)

	stmt := statement.(*sqlparser.Replace)
	if _, ok := stmt.Rows.(sqlparser.SelectStatement); ok {
		panic(sqlparser.NewParserError("select in replace not allowed"))
	}

	if stmt.Columns == nil {
		return nil, errors.ErrIRNoColumns
	}

	plan.Rule = r.GetRule(db, sqlparser.String(stmt.Table))
	if plan.Rule == nil {
		return nil, errors.ErrDatabaseNotSelected
	}
	err := plan.GetIRKeyIndex(stmt.Columns)
	if err != nil {
		return nil, err
	}

	plan.Criteria = plan.checkValuesType(stmt.Rows.(sqlparser.Values))

	err = plan.calRouteIndexs()
	if err != nil {
		log.ErrorErrorf(err, "Route BuildReplacePlan")
		return nil, err
	}

	err = r.generateReplaceSql(plan, stmt)
	if err != nil {
		return nil, err
	}
	return plan, nil
}

// 假定代理不支持Join操作
//rewrite select sql
func (r *Router) rewriteSelectSql(plan *Plan, node *sqlparser.Select, tableIndex int) string {

	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Fprintf("select %v%s",
		node.Comments,
		node.Distinct,
	)

	var prefix string
	//rewrite select expr
	for _, expr := range node.SelectExprs {
		switch v := expr.(type) {

		case *sqlparser.StarExpr:
			//for shardTable.*,need replace table into shardTable_xxxx.
			if string(v.TableName) == plan.Rule.Table {
				if plan.Rule.AllNodeSingleTable {
					fmt.Fprintf(buf, "%s%s.*",
						prefix,
						plan.Rule.Table,
					)
				} else {
					fmt.Fprintf(buf, "%s%s_%04d.*",
						prefix,
						plan.Rule.Table,
						tableIndex,
					)
				}
			} else {
				buf.Fprintf("%s%v", prefix, expr)
			}
		case *sqlparser.NonStarExpr:
			//rewrite shardTable.column as a
			//into shardTable_xxxx.column as a
			if colName, ok := v.Expr.(*sqlparser.ColName); ok {
				if string(colName.Qualifier) == plan.Rule.Table {
					if plan.Rule.AllNodeSingleTable {
						fmt.Fprintf(buf, "%s%s.%s",
							prefix,
							plan.Rule.Table,
							string(colName.Name),
						)
					} else {
						fmt.Fprintf(buf, "%s%s_%04d.%s",
							prefix,
							plan.Rule.Table,
							tableIndex,
							string(colName.Name),
						)
					}
				} else {
					buf.Fprintf("%s%v", prefix, colName)
				}
				//if expr has as
				if v.As != nil {
					buf.Fprintf(" as %s", v.As)
				}
			} else {
				buf.Fprintf("%s%v", prefix, expr)
			}
		default:
			buf.Fprintf("%s%v", prefix, expr)
		}
		prefix = ", "
	}
	//insert the group columns in the first of select cloumns
	if len(node.GroupBy) != 0 {
		prefix = ","
		for _, n := range node.GroupBy {
			buf.Fprintf("%s%v", prefix, n)
		}
	}
	buf.Fprintf(" from ")
	switch v := (node.From[0]).(type) {
	case *sqlparser.AliasedTableExpr:
		if len(v.As) != 0 {
			if plan.Rule.AllNodeSingleTable {
				fmt.Fprintf(buf, "%s as %s",
					sqlparser.String(v.Expr),
					string(v.As),
				)
			} else {
				fmt.Fprintf(buf, "%s_%04d as %s",
					sqlparser.String(v.Expr),
					tableIndex,
					string(v.As),
				)
			}
		} else {
			tableName := sqlparser.String(v.Expr)
			_, tableName = sqlparser.GetDBTable(tableName)
			if plan.Rule.AllNodeSingleTable {
				fmt.Fprintf(buf, "%s", tableName, )
			} else {
				fmt.Fprintf(buf, "%s_%04d", tableName, tableIndex, )
			}
		}
	case *sqlparser.JoinTableExpr:
		if ate, ok := (v.LeftExpr).(*sqlparser.AliasedTableExpr); ok {
			if len(ate.As) != 0 {
				if plan.Rule.AllNodeSingleTable {
					fmt.Fprintf(buf, "%s as %s",
						sqlparser.String(ate.Expr),
						string(ate.As),
					)
				} else {
					fmt.Fprintf(buf, "%s_%04d as %s",
						sqlparser.String(ate.Expr),
						tableIndex,
						string(ate.As),
					)
				}
			} else {
				if plan.Rule.AllNodeSingleTable {
					fmt.Fprintf(buf, "%s",
						sqlparser.String(ate.Expr),
					)
				} else {
					fmt.Fprintf(buf, "%s_%04d",
						sqlparser.String(ate.Expr),
						tableIndex,
					)
				}
			}
		} else {
			if plan.Rule.AllNodeSingleTable {
				fmt.Fprintf(buf, "%s",
					sqlparser.String(v.LeftExpr),
				)
			} else {
				fmt.Fprintf(buf, "%s_%04d",
					sqlparser.String(v.LeftExpr),
					tableIndex,
				)
			}
		}
		buf.Fprintf(" %s %v", v.Join, v.RightExpr)
		if v.On != nil {
			buf.Fprintf(" on %v", v.On)
		}
	default:
		// 去掉shard的虚拟的db的名字
		tableName := sqlparser.String(node.From[0])
		_, tableName = sqlparser.GetDBTable(tableName)

		log.Debugf("Shard From TableName: %s", tableName)
		if plan.Rule.AllNodeSingleTable {
			fmt.Fprintf(buf, "%s", tableName)
		} else {
			fmt.Fprintf(buf, "%s_%04d", tableName, tableIndex)
		}
	}
	//append other tables
	prefix = ", "
	for i := 1; i < len(node.From); i++ {
		buf.Fprintf("%s%v", prefix, node.From[i])
	}

	newLimit, err := node.Limit.RewriteLimit()
	if err != nil {
		//do not change limit
		newLimit = node.Limit
	}
	//rewrite where
	oldright, err := plan.rewriteWhereIn(tableIndex)

	buf.Fprintf("%v%v%v%v%v%s",
		node.Where,
		node.GroupBy,
		node.Having,
		node.OrderBy,
		newLimit,
		node.Lock,
	)
	//restore old right
	if oldright != nil {
		plan.InRightToReplace.Right = oldright
	}
	return buf.String()
}

//
// 如何重新生成Select SQL呢?
//
func (r *Router) generateSelectSql(plan *Plan, stmt sqlparser.Statement) error {
	sqls := make(map[string][]string)

	node, ok := stmt.(*sqlparser.Select)

	if ok == false {
		return errors.ErrStmtConvert
	}
	if len(plan.RouteNodeIndexs) == 0 {
		return errors.ErrNoRouteNode
	}
	if len(plan.RouteTableIndexs) == 0 {
		buf := sqlparser.NewTrackedBuffer(nil)
		stmt.Format(buf)
		nodeName := r.Nodes[0]

		// 和Table无关的查询？ 任意找一个Node执行即可
		selectSql := buf.String()

		sqls[nodeName] = []string{selectSql}
		log.Debugf("                @%s: %s", nodeName, selectSql)

	} else {

		// 有这么多个表，不同的表可能分布在不同的node上
		tableCount := len(plan.RouteTableIndexs)
		for i := 0; i < tableCount; i++ {
			tableIndex := plan.RouteTableIndexs[i]
			nodeIndex := plan.Rule.TableToNode[tableIndex]
			nodeName := r.Nodes[nodeIndex]

			// 如何生成面向不同表的SQL呢?
			selectSql := r.rewriteSelectSql(plan, node, tableIndex)

			if _, ok := sqls[nodeName]; ok == false {
				sqls[nodeName] = make([]string, 0, tableCount)
			}

			log.Debugf("                @%s: %s", nodeName, selectSql)
			sqls[nodeName] = append(sqls[nodeName], selectSql)
		}
	}
	plan.RewrittenSqls = sqls
	return nil
}

// INSERT INTO table (a, b) values (1, 2), (3, 4)... 这种如何支持呢?
func (r *Router) generateInsertSql(plan *Plan, stmt sqlparser.Statement) error {
	sqls := make(map[string][]string)
	node, ok := stmt.(*sqlparser.Insert)
	if ok == false {
		return errors.ErrStmtConvert
	}
	if len(plan.RouteNodeIndexs) == 0 {
		return errors.ErrNoRouteNode
	}
	if len(plan.RouteTableIndexs) == 0 {
		buf := sqlparser.NewTrackedBuffer(nil)
		stmt.Format(buf)
		nodeName := r.Nodes[0]
		insertSQL := buf.String()
		log.Debugf("                @%s: %s", nodeName, insertSQL)
		sqls[nodeName] = []string{insertSQL}
	} else {
		tableCount := len(plan.RouteTableIndexs)
		for i := 0; i < tableCount; i++ {
			buf := sqlparser.NewTrackedBuffer(nil)
			tableIndex := plan.RouteTableIndexs[i]
			nodeIndex := plan.Rule.TableToNode[tableIndex]
			nodeName := r.Nodes[nodeIndex]

			buf.Fprintf("insert %v%s into %v", node.Comments, node.Ignore, node.Table)

			if !plan.Rule.AllNodeSingleTable {
				fmt.Fprintf(buf, "_%04d", plan.RouteTableIndexs[i])
			}
			buf.Fprintf("%v %v%v",
				node.Columns,
				plan.Rows[tableIndex],
				node.OnDup)

			if _, ok := sqls[nodeName]; ok == false {
				sqls[nodeName] = make([]string, 0, tableCount)
			}

			insertSQL := buf.String()
			log.Debugf("                @%s: %s", nodeName, insertSQL)
			sqls[nodeName] = append(sqls[nodeName], insertSQL)
		}

	}
	plan.RewrittenSqls = sqls
	return nil
}

func (r *Router) generateUpdateSql(plan *Plan, stmt sqlparser.Statement) error {
	sqls := make(map[string][]string)
	node, ok := stmt.(*sqlparser.Update)
	if ok == false {
		return errors.ErrStmtConvert
	}
	if len(plan.RouteNodeIndexs) == 0 {
		return errors.ErrNoRouteNode
	}
	if len(plan.RouteTableIndexs) == 0 {
		buf := sqlparser.NewTrackedBuffer(nil)
		stmt.Format(buf)
		nodeName := r.Nodes[0]

		updateSQL := buf.String()
		log.Debugf("                @%s: %s", nodeName, updateSQL)

		sqls[nodeName] = []string{updateSQL}
	} else {
		tableCount := len(plan.RouteTableIndexs)
		for i := 0; i < tableCount; i++ {
			buf := sqlparser.NewTrackedBuffer(nil)
			buf.Fprintf("update %v%v",
				node.Comments,
				node.Table,
			)

			if !plan.Rule.AllNodeSingleTable {
				fmt.Fprintf(buf, "_%04d", plan.RouteTableIndexs[i])
			}
			buf.Fprintf(" set %v%v%v%v",
				node.Exprs,
				node.Where,
				node.OrderBy,
				node.Limit,
			)
			tableIndex := plan.RouteTableIndexs[i]
			nodeIndex := plan.Rule.TableToNode[tableIndex]
			nodeName := r.Nodes[nodeIndex]
			if _, ok := sqls[nodeName]; ok == false {
				sqls[nodeName] = make([]string, 0, tableCount)
			}

			updateSQL := buf.String()
			log.Debugf("                @%s: %s", nodeName, updateSQL)
			sqls[nodeName] = append(sqls[nodeName], updateSQL)
		}

	}
	plan.RewrittenSqls = sqls
	return nil
}

func (r *Router) generateDeleteSql(plan *Plan, stmt sqlparser.Statement) error {
	sqls := make(map[string][]string)
	node, ok := stmt.(*sqlparser.Delete)
	if ok == false {
		return errors.ErrStmtConvert
	}
	if len(plan.RouteNodeIndexs) == 0 {
		return errors.ErrNoRouteNode
	}
	if len(plan.RouteTableIndexs) == 0 {
		buf := sqlparser.NewTrackedBuffer(nil)
		stmt.Format(buf)
		nodeName := r.Nodes[0]

		deleteSQL := buf.String()
		log.Debugf("                @%s: %s", nodeName, deleteSQL)

		sqls[nodeName] = []string{deleteSQL}

	} else {
		tableCount := len(plan.RouteTableIndexs)
		for i := 0; i < tableCount; i++ {
			buf := sqlparser.NewTrackedBuffer(nil)
			buf.Fprintf("delete %vfrom %v",
				node.Comments,
				node.Table,
			)
			if !plan.Rule.AllNodeSingleTable {
				fmt.Fprintf(buf, "_%04d", plan.RouteTableIndexs[i])
			}
			buf.Fprintf("%v%v%v",
				node.Where,
				node.OrderBy,
				node.Limit,
			)
			tableIndex := plan.RouteTableIndexs[i]
			nodeIndex := plan.Rule.TableToNode[tableIndex]
			nodeName := r.Nodes[nodeIndex]
			if _, ok := sqls[nodeName]; ok == false {
				sqls[nodeName] = make([]string, 0, tableCount)
			}

			deleteSQL := buf.String()
			log.Debugf("                @%s: %s", nodeName, deleteSQL)

			sqls[nodeName] = append(sqls[nodeName], deleteSQL)
		}

	}
	plan.RewrittenSqls = sqls
	return nil
}

func (r *Router) generateReplaceSql(plan *Plan, stmt sqlparser.Statement) error {
	sqls := make(map[string][]string)
	node, ok := stmt.(*sqlparser.Replace)
	if ok == false {
		return errors.ErrStmtConvert
	}
	if len(plan.RouteNodeIndexs) == 0 {
		return errors.ErrNoRouteNode
	}
	if len(plan.RouteTableIndexs) == 0 {
		buf := sqlparser.NewTrackedBuffer(nil)
		stmt.Format(buf)
		nodeName := r.Nodes[0]
		replaceSQL := buf.String()
		log.Debugf("                @%s: %s", nodeName, replaceSQL)

		sqls[nodeName] = []string{replaceSQL}
	} else {
		tableCount := len(plan.RouteTableIndexs)
		for i := 0; i < tableCount; i++ {
			tableIndex := plan.RouteTableIndexs[i]
			nodeIndex := plan.Rule.TableToNode[tableIndex]
			nodeName := r.Nodes[nodeIndex]

			buf := sqlparser.NewTrackedBuffer(nil)
			buf.Fprintf("replace %vinto %v.%v",
				node.Comments,
				node.Table,
			)
			if !plan.Rule.AllNodeSingleTable {
				fmt.Fprintf(buf, "_%04d", plan.RouteTableIndexs[i])
			}
			buf.Fprintf("%v %v",
				node.Columns,
				plan.Rows[tableIndex],
			)

			if _, ok := sqls[nodeName]; ok == false {
				sqls[nodeName] = make([]string, 0, tableCount)
			}

			replaceSQL := buf.String()
			log.Debugf("                @%s: %s", nodeName, replaceSQL)
			sqls[nodeName] = append(sqls[nodeName], replaceSQL)
		}

	}
	plan.RewrittenSqls = sqls
	return nil
}

func (r *Router) generateTruncateSql(plan *Plan, stmt sqlparser.Statement) error {
	sqls := make(map[string][]string)
	node, ok := stmt.(*sqlparser.Truncate)
	if ok == false {
		return errors.ErrStmtConvert
	}
	if len(plan.RouteNodeIndexs) == 0 {
		return errors.ErrNoRouteNode
	}
	if len(plan.RouteTableIndexs) == 0 {
		buf := sqlparser.NewTrackedBuffer(nil)
		stmt.Format(buf)
		nodeName := r.Nodes[0]
		truncateSQL := buf.String()
		log.Debugf("                @%s: %s", nodeName, truncateSQL)
		sqls[nodeName] = []string{truncateSQL}
	} else {
		tableCount := len(plan.RouteTableIndexs)
		for i := 0; i < tableCount; i++ {
			buf := sqlparser.NewTrackedBuffer(nil)
			buf.Fprintf("truncate %v%s%v",
				node.Comments,
				node.TableOpt,
				node.Table,
			)
			if !plan.Rule.AllNodeSingleTable {
				fmt.Fprintf(buf, "_%04d", plan.RouteTableIndexs[i])
			}
			tableIndex := plan.RouteTableIndexs[i]
			nodeIndex := plan.Rule.TableToNode[tableIndex]
			nodeName := r.Nodes[nodeIndex]
			if _, ok := sqls[nodeName]; ok == false {
				sqls[nodeName] = make([]string, 0, tableCount)
			}

			truncateSQL := buf.String()
			log.Debugf("                @%s: %s", nodeName, truncateSQL)

			sqls[nodeName] = append(sqls[nodeName], truncateSQL)
		}

	}
	plan.RewrittenSqls = sqls
	return nil
}
