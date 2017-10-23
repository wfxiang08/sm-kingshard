#!/usr/bin/env bash
if [ "$#" -ne 1 ]; then
    echo "Please input hostname"
    exit -1
fi

host_name=$1

# 更新配置
ssh root@${host_name} "mkdir -p /usr/local/service/dbproxy"
ssh root@${host_name} "mkdir -p /data/logs/"

scp conf/sm*.yaml root@${host_name}:/usr/local/service/dbproxy/


# 更新dbproxy
ssh root@${host_name} "rm -f /usr/local/service/dbproxy/service_sm_proxy"
scp service_sm_proxy root@${host_name}:/usr/local/service/dbproxy/service_sm_proxy

# 创建工作目录
ssh root@${host_name} "chown -R worker.worker /data/logs"
ssh root@${host_name} "chown -R worker.worker /usr/local/service/dbproxy/"


# 拷贝systemctl
# scp scripts/dbproxy.service root@${host_name}:/lib/systemd/system/dbproxy.service

# 启动服务
# ssh root@${host_name} "systemctl daemon-reload"
# ssh root@${host_name} "systemctl restart dbproxy"