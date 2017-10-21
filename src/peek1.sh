#!/usr/bin/env bash
kill -USR1 `pidof service_kingshard` && tail -n 10  /data/tmp_db/service.log-`date +"%Y%m%d"`