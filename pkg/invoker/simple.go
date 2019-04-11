package invoker

import (
	"fmt"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/nuclio/logger"
)

type SimpleClient struct {
	logger      logger.Logger
	logLevel    string
	name        string
	workers     int
	caller      *FunctionCaller
	requestChan chan *TaskRequest
	RespChan    chan *TaskResp
	httpClient  *retryablehttp.Client
}

func NewSimpleClient(cfg *ClientConfig) (FunctionClient, error) {
	newClient := SimpleClient{logger: cfg.Logger, name: cfg.Name, workers: cfg.Workers}
	method := "PUT"
	if cfg.Method != "" {
		method = cfg.Method
	}
	newClient.caller = &FunctionCaller{logger: cfg.Logger, name: cfg.Name,
		address: cfg.Name, method: method, logLevel: cfg.LogLevel}

	if cfg.Workers > 0 {
		newClient.requestChan = make(chan *TaskRequest, requestChanSize)
		newClient.RespChan = make(chan *TaskResp, respChanSize)
		newClient.caller.StartAsync(cfg.Workers, newClient.requestChan)
	} else {
		newClient.httpClient = getHttpClient(cfg.LogLevel == "debug")
	}
	return &newClient, nil
}

func (c *SimpleClient) Submit(task *TaskRequest) (*TaskResp, error) {
	if c.workers > 0 {
		return nil, fmt.Errorf("Cannot use Submit with async client (workers>0)")
	}
	return c.caller.Invoke(c.httpClient, task)
}

func (c *SimpleClient) SubmitAsync(task *TaskRequest) error {
	if c.workers == 0 {
		return fmt.Errorf("Cannot use SubmitAsync with sync client (workers=0)")
	}
	task.RespChan = c.RespChan
	c.requestChan <- task
	return nil
}

func (c *SimpleClient) C() chan *TaskResp {
	return c.RespChan
}
