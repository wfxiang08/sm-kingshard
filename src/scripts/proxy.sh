#!/usr/bin/env bash
CONFIG=/usr/local/service/dbproxy/sm_readonly.yaml
PIDFILE=/usr/local/service/dbproxy/dbproxy.pid
CMD="nohup /usr/local/service/dbproxy/service_sm_proxy -config $CONFIG  -log-path /data/logs/proxy_service.log -log-level=debug -pidfile $PIDFILE &> /data/logs/proxy_stdout.log &"
app=dbproxy

function check_pid() {
    if [ -f $PIDFILE ];then
        pid=`cat $PIDFILE`
        if [ -n $pid ]; then
            running=`ps -p $pid|grep -v "PID TTY" |wc -l`
            return $running
        fi
    fi
    return 0
}

function start() {
    check_pid
    running=$?
    if [ $running -gt 0 ];then
        echo -n "$app now is running already, pid="
        cat $PIDFILE
        return 1
    fi

    $CMD
    echo "$app started..., pid=$!"
}

function stop() {
	check_pid
	running=$?
	# 由于lb等需要graceful stop, 因此stop过程需要等待
	if [ $running -gt 0 ];then
	    pid=`cat $PIDFILE`
		kill -15 $pid
		status="0"
		while [ "$status" == "0" ];do
			echo "Waiting for process ${pid} ..."
			sleep 1
			ps -p$pid 2>&1 > /dev/null
			status=$?
		done
	    echo "$app stoped..."
	else
		echo "$app already stoped..."
	fi
}

function reload() {
    check_pid
    running=$?
    if [ $running -gt 0 ];then
        pid=`cat $PIDFILE`
        RELOAD="/bin/kill -USR2 $pid"
    else
        start
    fi
}

# 查看当前的进程的状态
function status() {
    check_pid
    running=$?
    if [ $running -gt 0 ];then
        echo started
    else
        echo stoped
    fi
}


if [ "$1" == "" ]; then
    help
elif [ "$1" == "stop" ];then
    stop
elif [ "$1" == "start" ];then
    start
elif [ "$1" == "reload" ];then
    restart
else
    help
fi