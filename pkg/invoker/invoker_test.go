package invoker

import (
	"fmt"
	"github.com/yaronha/job-runner/pkg/common"
	"testing"
	"time"
)

func TestName(t *testing.T) {

	log, err := common.NewLogger("debug")
	if err != nil {
		t.Fail()
	}

	fi, _ := NewFunctionInvoker(log, "f1", 2)
	fi.UpdateEndpoints(map[string]string{"func1": "localhost:30000"})

	for i := 0; i < 15; i++ {
		tasks := []*TaskRequest{{Sequence: int64(i), Body: []byte("msg")}}
		fi.Submit(tasks)
	}

	go func() {
		for msg := range fi.RespChan {
			fmt.Printf("Message %d from %s w%d:\n%s\n%s  - %v\nlog: %s\n\n",
				msg.Request.Sequence, msg.Address, msg.Worker, msg.Body, msg.Status, msg.Err, msg.Log)
		}
	}()

	time.Sleep(3 * time.Second)
	//fi.DelInstance("func1")
	time.Sleep(15 * time.Second)
	//fi.UpdateEndpoints(map[string]string{"func1":"localhost:30000"})
	time.Sleep(10 * time.Second)

}
