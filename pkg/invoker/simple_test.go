package invoker

import (
	"fmt"
	"github.com/yaronha/job-runner/pkg/common"
	"testing"
)

func TestSimple(t *testing.T) {

	log, err := common.NewLogger("debug")
	if err != nil {
		t.Fail()
	}

	cfg := ClientConfig{Logger: log, LogLevel: "debug",
		Name: "localhost:30000", Method: "PUT"}

	c, _ := NewSimpleClient(&cfg)

	for i := 0; i < 15; i++ {
		task := &TaskRequest{Sequence: int64(i), Body: []byte("msg")}
		msg, err := c.Submit(task)
		if err != nil {
			t.Fail()
		}
		fmt.Printf("Message %d from %s w%d:\n%s\n%s  - %v\n\n",
			msg.Request.Sequence, msg.Address, msg.Worker, msg.Body, msg.Status, msg.Err)
		rows, _ := LogToStrings([]byte(msg.Log))
		fmt.Println(rows)
	}
}
