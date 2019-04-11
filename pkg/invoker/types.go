package invoker

import (
	"encoding/json"
	"fmt"
	"github.com/nuclio/logger"
	"time"
)

const (
	requestChanSize = 1000
	respChanSize    = 1000

	DefaultMaxRetry   = 5
	DefaultRetryMinMs = 50
)

type FunctionClient interface {
	Submit(task *TaskRequest) (*TaskResp, error)
	SubmitAsync(task *TaskRequest) error
	C() chan *TaskResp
}

type ClientConfig struct {
	Logger   logger.Logger
	LogLevel string
	Name     string
	Workers  int
	Method   string
}

type logEntry struct {
	Level string
	Time  float64
	Name  string
}

type TaskRequest struct {
	Sequence int64
	Body     []byte
	Retries  int
	RespChan chan *TaskResp
}

type TaskResp struct {
	Address    string
	Worker     int
	Request    *TaskRequest
	Body       []byte
	Log        string
	StatusCode int
	Status     string
	Err        error
}

func LogToStrings(body []byte) ([]string, error) {
	jsonMsg := []map[string]interface{}{}
	err := json.Unmarshal(body, &jsonMsg)
	if err != nil {
		return nil, err
	}

	rows := make([]string, len(jsonMsg))
	for i, m := range jsonMsg {
		level := m["level"].(string)
		t := m["time"].(float64)
		ms := int64(t) % 1000
		tstr := time.Unix(int64(t/1000), ms*1000000)
		name := m["name"].(string)
		message := m["message"].(string)
		delete(m, "level")
		delete(m, "time")
		delete(m, "name")
		delete(m, "message")
		row := fmt.Sprintf("%s  [%s]  %s   %s ", tstr, level, name, message)
		for k, v := range m {
			row += fmt.Sprintf(", %s: %v", k, v)
		}
		rows[i] = row
	}

	return rows, nil
}
