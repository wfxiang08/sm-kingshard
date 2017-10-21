package router

import (
	"fmt"
	"testing"
)

// go test proxy/router -v -run "TestSMShard"
func TestSMShard(t *testing.T) {

	fmt.Printf("Shard: %d\n", SMShard(7895866))
	fmt.Printf("Shard: %d\n", SMShard(4508938748297216))


	fmt.Printf("Shard: %d\n", SMShard(4758286310834176))
	fmt.Printf("Shard: %d\n", SMShard(4785074227644368))

}
