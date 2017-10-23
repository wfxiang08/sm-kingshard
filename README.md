# SMDbProxy:  
    * https://github.com/wfxiang08/sm_kingshard
    * SMDbProxy用法上和MySQL基本相同

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
