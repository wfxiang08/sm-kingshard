#!/usr/bin/env bash

go build cmds/service_sm_proxy.go && rm /usr/local/service/dbproxy/service_sm_proxy && cp service_sm_proxy /usr/local/service/dbproxy/service_sm_proxy && kill -USR2 `cat  /usr/local/db/readonly.pid`