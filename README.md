# DbProxy:  
    * https://github.com/wfxiang08/sm_kingshard
    * DBProxy用法上和MySQL基本相同

## 运维:
* 下载代码
```bash
git clone https://github.com/wfxiang08/sm_kingshard.git
cd sm_kingshard/src
```
* 编译代码
```bash
source start_env.sh
glide install
go build cmds/cmds/service_sm_proxy.go
```
* 部署
```bash
# 拷贝&通过systemctl运维
bash scripts/scp_dbproxy.sh host_name

# 不影响现有服务，直接进行更新
systemctl reload dbproxy

Graceful Restart

pidof service_sm_prox
# 启动新的进程：可以更新binary/conf, 并且继承当前进程的listeners; 
# 新的请求交给新的进程，旧的请求在旧的进程中处理，直到所有的旧的请求处理完毕才推出或者10s后强制退出
kill -USR2 `pidof service_sm_prox`

# 如果旧的请求不退出，查看到底谁在使用当前的Proxy
kill -USR1 `pidof service_sm_prox`
kill -USR1 `pidof service_sm_prox` && tail -n 10  /data/logs/service.log-`date +"%Y%m%d"`

Golang中定义的Signal和Linux的Signal不太一样，不要通过数字来代替 USR1, USR2

```

## 访问
```
mysql -u root -pxxx -P9900 -h172.1.2.12
use shard_sm

或
mysql -u root -pxxx -P9900 -h172.1.2.12  shard_sm
```

## Prepared Statement
* 不建议使用Prepared Statement, 在Sharding模式下禁用
* golang中推荐使用:
  * user:password@tcp(db.hostname:3306)/db_name?charset=utf8mb4,utf8&interpolateParams=true&parseTime=True&loc=UTC
  * interpolateParams=true, 表示不直接使用prepared statement

## 配置文件

```
addr : 0.0.0.0:9696

user :  kingshard
password : kingshard

web_addr : 0.0.0.0:9601
web_user : root
web_password : zzz

# debug info
log_level : debug
log_sql: on
# 20ms以上算慢请求
slow_log_time: 20
allow_ips : 127.0.0.1,192.168.0.14
nodes :
-
    name : node1
    db_name: kingshard_test
    max_conns_limit : 32
    user :  root
    password :
    master : 127.0.0.1:3306
    down_after_noalive : 32

# schema defines sharding rules, the db is the sharding table database.
schema :
    nodes: [node1]
    default: node1
    shard:
    -
        db : db_test
        table: recording
        keys: [id]
        # nodes为空，则和schema的nodes一致，locations为空，则表示[1, 1, 1 ...] 长度和nodes一致
        nodes: []
        type: sm
        locations: []
```