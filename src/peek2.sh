#!/usr/bin/env bash
kill -USR1 `cat /data/tmp_db/dbproxy.pid` && tail -n 10  /data/tmp_db/service.log-`date +"%Y%m%d"`