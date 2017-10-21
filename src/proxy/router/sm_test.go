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
	"fmt"
	"testing"

	"gopkg.in/yaml.v2"

	"config"
	"sqlparser"
)

const kSMSchema = `
schema :
    nodes: [shard_sm_0,shard_sm_1,shard_sm_2,shard_sm_3,shard_sm_4,shard_sm_5,shard_sm_6, shard_sm_7,
            shard_sm_8,shard_sm_9,shard_sm_10,shard_sm_11,shard_sm_12,shard_sm_13,shard_sm_14,shard_sm_15,
            shard_sm_16,shard_sm_17,shard_sm_18,shard_sm_19,shard_sm_20,shard_sm_21,shard_sm_22,shard_sm_23,
            shard_sm_24,shard_sm_25,shard_sm_26,shard_sm_27,shard_sm_28,shard_sm_29,shard_sm_30,shard_sm_31]
    default: shard_sm_0
    shard:
    -
        db : shard_sm
        table: recording
        keys: [id, user_id]
        nodes: [shard_sm_0,shard_sm_1,shard_sm_2,shard_sm_3,shard_sm_4,shard_sm_5,shard_sm_6, shard_sm_7,
            shard_sm_8,shard_sm_9,shard_sm_10,shard_sm_11,shard_sm_12,shard_sm_13,shard_sm_14,shard_sm_15,
            shard_sm_16,shard_sm_17,shard_sm_18,shard_sm_19,shard_sm_20,shard_sm_21,shard_sm_22,shard_sm_23,
            shard_sm_24,shard_sm_25,shard_sm_26,shard_sm_27,shard_sm_28,shard_sm_29,shard_sm_30,shard_sm_31]
        type: sm
        locations: [1, 1, 1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1, 1,
                    1, 1, 1, 1, 1, 1, 1, 1]
`

// Rule的解析以及测试
// go test proxy/router -v -run "TestParseSMRule"
func TestParseSMRule(t *testing.T) {
	var s = kSMSchema
	var cfg config.Config
	if err := yaml.Unmarshal([]byte(s), &cfg); err != nil {
		t.Fatal(err)
	}

	rt, err := NewRouter(&cfg.Schema)
	if err != nil {
		t.Fatal(err)
	}
	if rt.DefaultRule.Nodes[0] != "shard_sm_0" {
		t.Fatal("default rule parse not correct.")
	}

	{
		hashRule := rt.GetRule("shard_sm", "recording")
		if hashRule.Type != SMRuleType {
			t.Fatal(hashRule.Type)
		}

		if len(hashRule.Nodes) != 32 || hashRule.Nodes[0] != "shard_sm_0" || hashRule.Nodes[1] != "shard_sm_1" {
			t.Fatal("parse nodes not correct.")
		}

		if n, _ := hashRule.FindNode(uint64(450591873)); n != "node2" {
			t.Fatal(n)
		}

	}
}

func newSMTestRouter() *Router {
	var s = kSMSchema

	cfg, err := config.ParseConfigData([]byte(s))
	if err != nil {
		println(err.Error())
		panic(err)
	}

	var r *Router

	r, err = NewRouter(&cfg.Schema)
	if err != nil {
		println(err.Error())
		panic(err)
	}

	return r
}

// go test proxy/router -v -run "TestSMBadUpdateExpr"
func TestSMBadUpdateExpr(t *testing.T) {
	var sql string
	var db string
	r := newSMTestRouter()
	db = "shard_sm"
	sql = "insert into test1 (id) values (5) on duplicate key update  id = 10"

	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		t.Fatal(err.Error())
	}

	if _, err := r.BuildPlan(db, stmt); err == nil {
		t.Fatal("must err")
	}

	sql = "update test1 set id = 10 where id = 5"

	stmt, err = sqlparser.Parse(sql)
	if err != nil {
		t.Fatal(err.Error())
	}

	if _, err := r.BuildPlan(db, stmt); err == nil {
		t.Fatal("must err")
	}
}

//db: kingshard
//table: test1
//keys: [id]
//nodes: [node1,node2,node3]
//locations: [4,4,4]
//type: hash
//
// go test kingshard/proxy/router -v -run "TestSMWhereInPartitionByTableIndex"
func TestSMWhereInPartitionByTableIndex(t *testing.T) {
	var sql string
	//2016-08-02 13:37:26
	sql = "/*node2*/select * from recording where id in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22) "
	checkSMPlan(t, sql,
		[]int{0},
		[]int{0},
	)
	// ensure no impact for or operator in where
	sql = "select * from recording where id in (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21) or name='test'"
	checkSMPlan(t, sql,
		[]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31},
		[]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31},
	)

	// ensure no impact for not in
	// 这是什么情况呢?
	sql = "select * from recording where id not in (0,1,2,3,4,5,6,7)"
	checkSMPlan(t, sql,
		[]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31},
		[]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31},
	)

}

//db: kingshard
//table: test1
//keys: [id]
//nodes: [node1,node2,node3]
//locations: [4,4,4]
//type: hash
//
// go test proxy/router -v -run "TestSelectPlan"
func TestSMSelectPlan(t *testing.T) {
	var sql string
	t1 := makeList(0, 12)

	sql = "select/*master*/ * from test1 where id = 5"
	checkSMPlan(t, sql, []int{5}, []int{1}) //table_5 node1

	sql = "select * from test1 where id in (5, 8)"
	checkSMPlan(t, sql, []int{5, 8}, []int{1, 2})

	// hash shard对range没有特殊支持
	sql = "select * from test1 where id > 5"

	checkSMPlan(t, sql, t1, []int{0, 1, 2})

	sql = "select * from test1 where id in (5, 6) and id in (5, 6, 7)"
	checkSMPlan(t, sql, []int{5, 6}, []int{1})

	sql = "select * from test1 where id in (5, 6) or id in (5, 6, 7,8)"
	checkSMPlan(t, sql, []int{5, 6, 7, 8}, []int{1, 2})

	sql = "select * from test1 where id not in (5, 6) or id in (5, 6, 7,8)"
	checkSMPlan(t, sql, t1, []int{0, 1, 2})

	sql = "select * from test1 where id not in (5, 6)"
	checkSMPlan(t, sql, t1, []int{0, 1, 2})

	sql = "select * from test1 where id in (5, 6) or (id in (5, 6, 7,8) and id in (1,5,7))"
	checkSMPlan(t, sql, []int{5, 6, 7}, []int{1})

	//-
	//  db: kingshard
	//  table: test2
	//  keys: [id]
	//  type: range
	//  nodes: [node1,node2,node3]
	//  locations: [4,4,4]
	//  table_row_limit: 10000
	//
	sql = "select * from test2 where id = 10000"
	checkSMPlan(t, sql, []int{1}, []int{0})

	sql = "select * from test2 where id between 10000 and 20000"
	checkSMPlan(t, sql, []int{1, 2}, []int{0})

	// id < 1000 or id > 100000
	sql = "select * from test2 where id not between 1000 and 100000"
	checkSMPlan(t, sql, []int{0, 10, 11}, []int{0, 2})

	sql = "select * from test2 where id > 10000"
	checkSMPlan(t, sql, makeList(1, 12), []int{0, 1, 2})

	sql = "select * from test2 where id >= 9999"
	checkSMPlan(t, sql, t1, []int{0, 1, 2})

	sql = "select * from test2 where id <= 10000"
	checkSMPlan(t, sql, []int{0, 1}, []int{0})

	sql = "select * from test2 where id < 10000"
	checkSMPlan(t, sql, []int{0}, []int{0})

	sql = "select * from test2 where id >= 10000 and id <= 30000"
	checkSMPlan(t, sql, []int{1, 2, 3}, []int{0})

	sql = "select * from test2 where (id >= 10000 and id <= 30000) or id < 100"
	checkSMPlan(t, sql, []int{0, 1, 2, 3}, []int{0})

	sql = "select * from test2 where id in (1, 10000)"
	checkSMPlan(t, sql, []int{0, 1}, []int{0})

	sql = "select * from test2 where id not in (1, 10000)"
	checkSMPlan(t, sql, makeList(0, 12), []int{0, 1, 2})
}

// go test proxy/router -v -run "TestValueSharding"
func TestSMValueSharding(t *testing.T) {
	var sql string

	sql = "insert into test1 (id) values (5)"
	checkSMPlan(t, sql, []int{5}, []int{1})

	sql = "insert into test2 (id) values (10000)"
	checkSMPlan(t, sql, []int{1}, []int{0})

	sql = "insert into test2 (id) values (20000)"
	checkSMPlan(t, sql, []int{2}, []int{0})

	sql = "update test1 set a =10 where id =12"
	checkSMPlan(t, sql, []int{0}, []int{0})

	sql = "update test2 set a =10 where id < 30000 and 10000< id"
	checkSMPlan(t, sql, []int{1, 2}, []int{0})

	sql = "delete from test2 where id < 30000 and 10000< id"
	checkSMPlan(t, sql, []int{1, 2}, []int{0})

	sql = "replace into test1(id) values(5)"
	checkSMPlan(t, sql, []int{5}, []int{1})
}


// 如何验证Plan呢?
func checkSMPlan(t *testing.T, sql string, tableIndexs []int, nodeIndexs []int) {
	r := newSMTestRouter()
	db := "shard_sm"

	// 解析stmt
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		t.Fatal(err.Error())
	}

	// DB + stmt就也能生成一个plan
	plan, err := r.BuildPlan(db, stmt)
	if err != nil {
		t.Fatal(err.Error())
	}

	// 需要在哪些表中查询呢? RouteTableIndexs
	if isListEqual(plan.RouteTableIndexs, tableIndexs) == false {
		err := fmt.Errorf("RouteTableIndexs=%v but tableIndexs=%v",
			plan.RouteTableIndexs, tableIndexs)
		t.Fatal(err.Error())
	}

	// 需要在哪些Node汇总查询呢?
	if isListEqual(plan.RouteNodeIndexs, nodeIndexs) == false {
		err := fmt.Errorf("RouteNodeIndexs=%v but nodeIndexs=%v",
			plan.RouteNodeIndexs, nodeIndexs)
		t.Fatal(err.Error())
	}

	fmt.Printf("------------\n")
	fmt.Printf("ORIGIN: %s\n", sql)
	for key, sqls := range plan.RewrittenSqls {
		for _, sql := range sqls {
			fmt.Printf("New SQL: %s --> %s\n", key, sql)
		}
	}

}