package server

import (
	"fmt"
	"strings"

	"backend"
	"core/errors"
	"core/hack"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"mysql"
	"proxy/router"
	"sort"
	"sqlparser"
)

const (
	kSQLShowDatabase = "show databases"
)

//
// 这里的交互都是针对没有做Sharding的情况
//
type ExecuteDB struct {
	ExecNode *backend.Node
	IsSlave  bool
	sql      string
}

func (c *ClientConn) isBlacklistSql(sql string) bool {
	// fingerprint如何处理呢?
	fingerprint := mysql.GetFingerprint(sql)

	md5 := mysql.GetMd5(fingerprint)

	// 通过sql的md5来处理
	if _, ok := c.proxy.blacklistSqls[c.proxy.blacklistSqlsIndex].sqls[md5]; ok {
		return true
	}
	return false
}

//
//预处理sql
//
func (c *ClientConn) preHandleShard(sql string) (bool, error) {
	var rs []*mysql.Result
	var err error
	var executeDB *ExecuteDB

	if len(sql) == 0 {
		return false, errors.ErrCmdUnsupport
	}

	// 1. 如何禁止某个SQL语句呢?
	if c.proxy.blacklistSqls[c.proxy.blacklistSqlsIndex].sqlsLen != 0 {
		// 如果Blacklist，则返回
		if c.isBlacklistSql(sql) {
			log.Infof("Forbidden: %s:%s", c.c.RemoteAddr(), sql)
			err := mysql.NewError(mysql.ER_UNKNOWN_ERROR, "sql in blacklist.")
			return false, err
		}
	}

	// 2. 将sql分解成为tokens
	tokens := strings.FieldsFunc(sql, hack.IsSqlSep)

	if len(tokens) == 0 {
		return false, errors.ErrCmdUnsupport
	}

	// 3. 获取要执行的DB
	if c.isInTransaction() {
		executeDB, err = c.GetTransExecDB(tokens, sql)
	} else {
		executeDB, err = c.GetExecDB(tokens, sql)
	}

	if err != nil {
		// 如果出现错误，则直接返回给Client, 不用继续到backend去执行SQL
		if err == errors.ErrIgnoreSQL {
			err = c.writeOK(nil)
			if err != nil {
				return false, err
			}
			return true, nil
		}
		return false, err
	}

	// need shard sql
	if executeDB == nil {
		log.Debugf("Need shard sql")
		return false, nil
	} else {
		log.Debugf("Execute sql in node: %s", executeDB.ExecNode.Master.Addr())
	}

	//作为Proxy需要将请求转发给后端!!!!
	//get connection in DB
	conn, err := c.getBackendConn(executeDB.ExecNode, executeDB.IsSlave)
	defer c.closeConn(conn, false)

	if err != nil {
		return false, err
	}
	// execute.sql may be rewritten in getShowExecDB
	rs, err = c.executeInNode(conn, executeDB.sql, nil)
	if err != nil {
		return false, err
	}

	if len(rs) == 0 {
		msg := fmt.Sprintf("result is empty")
		log.Errorf("ClientConn handleUnsupport: %s, sql: %s", msg, sql)
		return false, mysql.NewError(mysql.ER_UNKNOWN_ERROR, msg)
	}

	c.lastInsertId = int64(rs[0].InsertId)
	c.affectedRows = int64(rs[0].AffectedRows)

	if rs[0].Resultset != nil {
		if kSQLShowDatabase == strings.ToLower(sql) {
			return c.handleShowDatabases(rs[0].Resultset, rs[0].Fields)
		} else {
			err = c.writeResultset(c.status, rs[0].Resultset)
		}
	} else {
		err = c.writeOK(rs[0])
	}

	if err != nil {
		return false, err
	}

	return true, nil
}

func (c *ClientConn) handleShowDatabases(resultSet *mysql.Resultset, f []*mysql.Field) (bool, error) {
	// 1. 重新定义结果(直接返回配置文件中的定义)

	resultSet.RowDatas = nil

	// 2. 对配置文件中的节点进行排序
	var databases []string
	for k, _ := range c.schema.AllNodes {
		databases = append(databases, k)
	}
	databases = append(databases, c.schema.ShardDB)

	log.Debugf("Databases: %s", strings.Join(databases, ", "))
	sort.Sort(StringArray(databases))

	// 3. 按照MySQL协议格式化show databases的结果
	for _, k := range databases {
		data, err := mysql.FormatBinary(f, []interface{}{k})
		if err != nil {
			log.ErrorErrorf(err, "Format database name failed")
			continue
		}
		resultSet.RowDatas = append(resultSet.RowDatas, mysql.RowData(data))
	}

	// 4. 返回数据
	err := c.writeResultset(c.status, resultSet)

	if err != nil {
		return false, err
	} else {
		return true, nil
	}
}

type StringArray []string

func (s StringArray) Len() int {
	return len(s)
}
func (s StringArray) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s StringArray) Less(i, j int) bool {
	return s[i] < s[j]
}

// ByWeight implements sort.Interface by providing Less and using the Len and

func (c *ClientConn) PrintResultset(r *mysql.Resultset) {
	for _, v := range r.Fields {
		fmt.Printf("Name: %s\n", v.Name)
	}
	for _, v := range r.Values {
		for _, item := range v {
			fmt.Printf("%v ", item)
		}
		fmt.Printf("\n")
	}
}

func (c *ClientConn) GetTransExecDB(tokens []string, sql string) (*ExecuteDB, error) {
	var err error
	tokensLen := len(tokens)

	// 获取执行的DB
	executeDB := new(ExecuteDB)
	executeDB.sql = sql

	//transaction execute in master db
	executeDB.IsSlave = false

	if 2 <= tokensLen {
		// /*node2*/ --> *node2*
		if tokens[0][0] == mysql.COMMENT_PREFIX {
			nodeName := strings.Trim(tokens[0], mysql.COMMENT_STRING)

			// 指定了节点
			if c.schema.AllNodes[nodeName] != nil {
				executeDB.ExecNode = c.schema.AllNodes[nodeName]
			}
		}
	}

	// 如果没有指定node, 那么也就是都是标准的mysql, 例如: insert, select等等
	if executeDB.ExecNode == nil {
		executeDB, err = c.GetExecDB(tokens, sql)
		if err != nil {
			return nil, err
		}
		if executeDB == nil {
			return nil, nil
		}
		return executeDB, nil
	}

	// 不能跨表支持事务
	if len(c.txConns) == 1 && c.txConns[executeDB.ExecNode] == nil {
		return nil, errors.ErrTransInMulti
	}
	return executeDB, nil
}

//
// 如果不需要Shard, 则返回DB; 否则返回nil
//
func (c *ClientConn) GetExecDB(tokens []string, sql string) (*ExecuteDB, error) {
	tokensLen := len(tokens)

	// 这里不考虑/*node*/这种情况
	if 0 < tokensLen {
		tokenId, ok := mysql.PARSE_TOKEN_MAP[strings.ToLower(tokens[0])]
		if ok == true {
			// 第一个token应该为各种动作
			// select, delete, insert, update, replace, set, show, truncate
			// 好像不支持create table, alter table等?
			//
			switch tokenId {
			case mysql.TK_ID_SELECT:
				// 如果不需要Shard, 则返回DB; 否则返回nil
				return c.getSelectExecDB(sql, tokens, tokensLen)
			case mysql.TK_ID_DELETE:
				return c.getDeleteExecDB(sql, tokens, tokensLen)
			case mysql.TK_ID_INSERT, mysql.TK_ID_REPLACE:
				return c.getInsertOrReplaceExecDB(sql, tokens, tokensLen)
			case mysql.TK_ID_UPDATE:
				return c.getUpdateExecDB(sql, tokens, tokensLen)
			case mysql.TK_ID_SET:
				return c.getSetExecDB(sql, tokens, tokensLen)

			case mysql.TK_ID_SHOW:
				return c.getShowExecDB(sql, tokens, tokensLen)

			case mysql.TK_ID_TRUNCATE:
				return c.getTruncateExecDB(sql, tokens, tokensLen)
			default:
				return nil, nil
			}
		}
	}

	// 如果没有给定Tokens, 然后呢?
	executeDB := new(ExecuteDB)
	executeDB.sql = sql
	err := c.setExecuteNode(tokens, tokensLen, executeDB)
	if err != nil {
		return nil, err
	}
	return executeDB, nil
}

//
// preshard文件中所涉及到的<db, table>都是不做shard处理的
//
func (c *ClientConn) setExecuteNode(tokens []string, tokensLen int, executeDB *ExecuteDB) error {
	ruleDB := c.CurrentDB // 如果没有特别说明，就使用当前默认的DB
	return c.setExecuteNodeFromAll(ruleDB, tokens, tokensLen, executeDB, true)
}
func (c *ClientConn) setExecuteNodeFromAll(ruleDB string, tokens []string, tokensLen int, executeDB *ExecuteDB, includeShardNode bool) error {
	if 2 <= tokensLen {
		//for /*node1*/
		if 1 < len(tokens) && tokens[0][0] == mysql.COMMENT_PREFIX {
			nodeName := strings.Trim(tokens[0], mysql.COMMENT_STRING)
			if c.schema.AllNodes[nodeName] != nil {
				executeDB.ExecNode = c.schema.AllNodes[nodeName]
			}
			//for /*node1*/ select
			if strings.ToLower(tokens[1]) == mysql.TK_STR_SELECT {
				executeDB.IsSlave = true
			}
		}
	}

	// 如果SQL中没有指定DB, 则考虑使用Client当前的DB; 否则报错
	if executeDB.ExecNode == nil {
		if len(ruleDB) > 0 {
			node := c.schema.GetNode(ruleDB, includeShardNode)
			if node == nil {
				return errors.ErrDatabaseNotSelected
			}
			executeDB.ExecNode = node
		} else {
			if includeShardNode {
				// 如果不是针对具体Table的查询，则交给默认的Database
				executeDB.ExecNode = c.schema.ShardDBDefaultNode
			} else {
				return errors.ErrDatabaseNotSelected
			}
		}
	}
	return nil

}

//
//获取Select语句的ExecDB
//
func (c *ClientConn) getSelectExecDB(sql string, tokens []string, tokensLen int) (*ExecuteDB, error) {
	var ruleDB string
	var usedTableName string
	executeDB := new(ExecuteDB)
	executeDB.sql = sql
	executeDB.IsSlave = true

	schema := c.proxy.schema
	schemaRouter := schema.Router
	rules := schemaRouter.Rules

	if len(rules) != 0 {
		for i := 1; i < tokensLen; i++ {
			if strings.ToLower(tokens[i]) == mysql.TK_STR_FROM {
				if i+1 < tokensLen {
					// 分析Select语句，获取 <db, table> 从而知道是否需要Shard, 如果需要，则返回nil,nil
					// 如果不需要Shard, 则返回DB; 否则返回nil
					dbName, tableName := sqlparser.GetDBTable(tokens[i+1])

					// 两种DBName的获取方式，如果SQL中explicitly指定database, 那么以SQL为准；
					// 否则使用当前connection默认的DB
					// 一个DBConnection一次只能使用一个DB?
					// if the token[i+1] like this:kingshard.test_shard_hash
					if dbName != "" {
						ruleDB = dbName
					} else {
						ruleDB = c.CurrentDB
					}
					usedTableName = tableName

					// 如果DB/Table不存在Rule, 则在直接返回
					if schemaRouter.GetRule(ruleDB, tableName) != nil {
						return nil, nil
					} else {
						//if the table is not shard table,send the sql
						//to default db
						break
					}
				}
			}

			// select last_insert_id(); 这个如何执行呢? 确实是一个难点
			if strings.ToLower(tokens[i]) == mysql.TK_STR_LAST_INSERT_ID {
				return nil, nil
			}
		}
	}

	//if send to master
	if 2 < tokensLen {
		if strings.ToLower(tokens[1]) == mysql.TK_STR_MASTER_HINT {
			executeDB.IsSlave = false
		}
	}

	// 这里是什么情况呢?
	// 1. 要么非Sharding DB
	// 2. 要么Sharding DB, 但是不涉及到Table Name
	err := c.setExecuteNodeFromAll(ruleDB, tokens, tokensLen, executeDB, len(usedTableName) == 0)
	if err != nil {
		return nil, err
	}
	return executeDB, nil
}

//get the execute database for delete sql
func (c *ClientConn) getDeleteExecDB(sql string, tokens []string, tokensLen int) (*ExecuteDB, error) {
	var ruleDB string
	executeDB := new(ExecuteDB)
	executeDB.sql = sql
	schema := c.proxy.schema
	schemaRouter := schema.Router
	rules := schemaRouter.Rules

	if len(rules) != 0 {
		for i := 1; i < tokensLen; i++ {
			if strings.ToLower(tokens[i]) == mysql.TK_STR_FROM {
				if i+1 < tokensLen {
					// 两种SQL格式：
					//   select * from recording where id=xxx;
					//   select * from shard_sm.recording where id=xxx;
					//  注意默认的DB的状态的维护
					dbName, tableName := sqlparser.GetDBTable(tokens[i+1])
					//if the token[i+1] like this:kingshard.test_shard_hash
					if dbName != "" {
						ruleDB = dbName
					} else {
						ruleDB = c.CurrentDB
					}

					// 如果需要Shard, 则直接返回
					if schemaRouter.GetRule(ruleDB, tableName) != nil {
						return nil, nil
					} else {
						break
					}
				}
			}
		}
	}

	err := c.setExecuteNode(tokens, tokensLen, executeDB)
	if err != nil {
		return nil, err
	}

	return executeDB, nil
}

//get the execute database for insert or replace sql
func (c *ClientConn) getInsertOrReplaceExecDB(sql string, tokens []string, tokensLen int) (*ExecuteDB, error) {
	var ruleDB string
	executeDB := new(ExecuteDB)
	executeDB.sql = sql
	schema := c.proxy.schema
	schemaRouter := schema.Router
	rules := schemaRouter.Rules

	if len(rules) != 0 {
		for i := 0; i < tokensLen; i++ {
			if strings.ToLower(tokens[i]) == mysql.TK_STR_INTO {
				if i+1 < tokensLen {
					dbName, tableName := sqlparser.GetInsertDBTable(tokens[i+1])
					//if the token[i+1] like this:kingshard.test_shard_hash
					if dbName != "" {
						ruleDB = dbName
					} else {
						ruleDB = c.CurrentDB
					}
					if schemaRouter.GetRule(ruleDB, tableName) != nil {
						return nil, nil
					} else {
						break
					}
				}
			}
		}
	}

	err := c.setExecuteNode(tokens, tokensLen, executeDB)
	if err != nil {
		return nil, err
	}

	return executeDB, nil
}

//get the execute database for update sql
func (c *ClientConn) getUpdateExecDB(sql string, tokens []string, tokensLen int) (*ExecuteDB, error) {
	var ruleDB string
	executeDB := new(ExecuteDB)
	executeDB.sql = sql
	schema := c.proxy.schema
	schemaRouter := schema.Router
	rules := schemaRouter.Rules

	if len(rules) != 0 {
		for i := 0; i < tokensLen; i++ {
			if strings.ToLower(tokens[i]) == mysql.TK_STR_SET {
				dbName, tableName := sqlparser.GetDBTable(tokens[i-1])
				//if the token[i+1] like this:kingshard.test_shard_hash
				if dbName != "" {
					ruleDB = dbName
				} else {
					ruleDB = c.CurrentDB
				}
				if schemaRouter.GetRule(ruleDB, tableName) != nil {
					return nil, nil
				} else {
					break
				}
			}
		}
	}

	err := c.setExecuteNode(tokens, tokensLen, executeDB)
	if err != nil {
		return nil, err
	}

	return executeDB, nil
}

//get the execute database for set sql
func (c *ClientConn) getSetExecDB(sql string, tokens []string, tokensLen int) (*ExecuteDB, error) {
	executeDB := new(ExecuteDB)
	executeDB.sql = sql

	//handle three styles:
	//set autocommit= 0
	//set autocommit = 0
	//set autocommit=0
	if 2 <= len(tokens) {
		before := strings.Split(sql, "=")
		//uncleanWorld is 'autocommit' or 'autocommit '
		uncleanWord := strings.Split(before[0], " ")
		secondWord := strings.ToLower(uncleanWord[1])
		if _, ok := mysql.SET_KEY_WORDS[secondWord]; ok {
			return nil, nil
		}

		//SET [gobal/session] TRANSACTION ISOLATION LEVEL SERIALIZABLE
		//ignore this sql
		if 3 <= len(uncleanWord) {
			if strings.ToLower(uncleanWord[1]) == mysql.TK_STR_TRANSACTION ||
				strings.ToLower(uncleanWord[2]) == mysql.TK_STR_TRANSACTION {
				return nil, errors.ErrIgnoreSQL
			}
		}
	}

	err := c.setExecuteNode(tokens, tokensLen, executeDB)
	if err != nil {
		return nil, err
	}

	return executeDB, nil
}

//get the execute database for show sql
//choose slave preferentially
//tokens[0] is show
func (c *ClientConn) getShowExecDB(sql string, tokens []string, tokensLen int) (*ExecuteDB, error) {
	executeDB := new(ExecuteDB)
	executeDB.IsSlave = true
	executeDB.sql = sql

	//handle show columns/fields
	err := c.handleShowColumns(sql, tokens, tokensLen, executeDB)
	if err != nil {
		return nil, err
	}

	err = c.setExecuteNode(tokens, tokensLen, executeDB)
	if err != nil {
		return nil, err
	}

	return executeDB, nil
}

//handle show columns/fields
func (c *ClientConn) handleShowColumns(sql string, tokens []string,
	tokensLen int, executeDB *ExecuteDB) error {
	var ruleDB string
	for i := 0; i < tokensLen; i++ {
		tokens[i] = strings.ToLower(tokens[i])
		//handle SQL:
		//SHOW [FULL] COLUMNS FROM tbl_name [FROM db_name] [like_or_where]
		if (strings.ToLower(tokens[i]) == mysql.TK_STR_FIELDS ||
			strings.ToLower(tokens[i]) == mysql.TK_STR_COLUMNS) &&
			i+2 < tokensLen {
			if strings.ToLower(tokens[i+1]) == mysql.TK_STR_FROM {
				tableName := strings.Trim(tokens[i+2], "`")
				//get the ruleDB
				if i+4 < tokensLen && strings.ToLower(tokens[i+1]) == mysql.TK_STR_FROM {
					ruleDB = strings.Trim(tokens[i+4], "`")
				} else {
					ruleDB = c.CurrentDB
				}
				showRouter := c.schema.Router
				showRule := showRouter.GetRule(ruleDB, tableName)
				//this SHOW is sharding SQL
				if showRule.Type != router.DefaultRuleType {
					if 0 < len(showRule.SubTableIndexs) {
						tableIndex := showRule.SubTableIndexs[0]
						nodeIndex := showRule.TableToNode[tableIndex]
						nodeName := showRule.Nodes[nodeIndex]
						tokens[i+2] = fmt.Sprintf("%s_%04d", tableName, tableIndex)
						executeDB.sql = strings.Join(tokens, " ")
						executeDB.ExecNode = c.schema.AllNodes[nodeName]
						return nil
					}
				}
			}
		}
	}
	return nil
}

//get the execute database for truncate sql
//sql: TRUNCATE [TABLE] tbl_name
func (c *ClientConn) getTruncateExecDB(sql string, tokens []string, tokensLen int) (*ExecuteDB, error) {
	var ruleDB string
	executeDB := new(ExecuteDB)
	executeDB.sql = sql
	schema := c.proxy.schema
	schemaRouter := schema.Router
	rules := schemaRouter.Rules
	if len(rules) != 0 && tokensLen >= 2 {
		dbName, tableName := sqlparser.GetDBTable(tokens[tokensLen-1])
		//if the token[i+1] like this:kingshard.test_shard_hash
		if dbName != "" {
			ruleDB = dbName
		} else {
			ruleDB = c.CurrentDB
		}
		if schemaRouter.GetRule(ruleDB, tableName) != nil {
			return nil, nil
		}

	}

	err := c.setExecuteNode(tokens, tokensLen, executeDB)
	if err != nil {
		return nil, err
	}

	return executeDB, nil
}
