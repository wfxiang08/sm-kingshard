package utils

import (
	"github.com/wfxiang08/cyutils/utils"
	log "github.com/wfxiang08/cyutils/utils/rolling_log"
	"os/exec"
	"strconv"
	"strings"
)

func Lsof(sock string) int64 {
	if !utils.FileExist(sock) {
		return 0
	}

	// 如果不存在，则
	if !utils.FileExist("/usr/sbin/lsof") {
		log.Printf(Red("/usr/sbin/lsof not found"))
		return 1
	} else if !utils.FileExist("/usr/bin/wc") {
		log.Printf(Red("/usr/bin/wc not found"))
		return 1
	}

	lsof := exec.Command("/usr/sbin/lsof", sock)
	wc := exec.Command("/usr/bin/wc", "-l")
	lsofOut, _ := lsof.StdoutPipe()
	lsof.Start()
	wc.Stdin = lsofOut
	out, _ := wc.Output()

	outStr := string(out)
	outStr = strings.TrimSpace(outStr)

	log.Printf("Sock: %s --> %s", sock, out)
	result, err := strconv.ParseInt(outStr, 10, 64)
	if err != nil {
		return 0
	} else if result > 1 {
		return result - 1
	} else {
		return 0
	}
}
