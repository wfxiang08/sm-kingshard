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
	"mysql"
)

//
// 如何处理 use database_name; 这种需求呢？
//
func (c *ClientConn) handleUseDB(dbName string) error {

	if len(dbName) == 0 {
		return fmt.Errorf("must have database, the length of dbName is zero")
	}
	if c.schema == nil {
		return mysql.NewDefaultError(mysql.ER_NO_DB_ERROR)
	}

	// 1. 要么访问某个指定的Node
	//    https://github.com/flike/kingshard/commit/7a2ff664d7cef46e01057087e77d6c8e0725a440
	//    use_db 不对后端进行近操作, 直接修改ClientConn状态
	//
	if node := c.schema.GetNode(dbName, true); node != nil {
		c.CurrentDB = dbName
		return c.writeOK(nil)
	} else {
		// 3. 直接报错: BadDB
		return mysql.NewDefaultError(mysql.ER_BAD_DB_ERROR, "database name invalid: " + dbName)
	}
}
