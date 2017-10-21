//// Copyright 2015 Spring Rain Software Compnay LTD. All Rights Reserved.
//// Licensed under the MIT (MIT-LICENSE.txt) license.
package sockets

import (
	"testing"
)


//
// go test kingshard -v -run "TestSocketPermissionChange"
//
func TestSocketPermissionChange(t *testing.T) {

	socketFile := "aaa.sock"
	s, _ := NewTServerUnixDomain(socketFile)
	s.Listen()

}
