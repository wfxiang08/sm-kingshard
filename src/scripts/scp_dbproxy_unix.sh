#!/usr/bin/env bash
if [ "$#" -ne 1 ]; then
    echo "Please input hostname"
    exit -1
fi

host_name=$1

# 更新配置
ssh root@${host_name} "mkdir -p /usr/local/db/"
ssh root@${host_name} "mkdir -p /data/tmp_db"

scp conf/sm*.yaml root@${host_name}:/usr/local/db/


# 更新dbproxy
ssh root@${host_name} "rm -f /usr/local/db/service_kingshard"
scp service_kingshard root@${host_name}:/usr/local/db/service_kingshard

# 创建工作目录
ssh root@${host_name} "chown -R worker.worker /data/tmp_db"
ssh root@${host_name} "chown -R worker.worker /usr/local/db"

# 拷贝systemctl
scp scripts/dbproxy_unix.service root@${host_name}:/lib/systemd/system/dbproxy_unix.service

# 启动服务
ssh root@${host_name} "systemctl daemon-reload"
ssh root@${host_name} "systemctl restart dbproxy_unix"

