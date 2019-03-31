package invoker

import (
	"fmt"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/nuclio/logger"
	"io/ioutil"
	"sync"
	"time"
)

const (
	requestChanSize = 1000
	respChanSize    = 1000

	DefaultMaxRetry   = 5
	DefaultRetryMinMs = 50
)

type FunctionInvoker struct {
	logger      logger.Logger
	logLevel    string
	name        string
	workers     int
	instances   map[string]*functionInstance
	requestChan chan *TaskRequest

	RespChan chan *TaskResp
}

type functionInstance struct {
	name    string
	address string
	quits   []chan bool
	wg      sync.WaitGroup
}

type TaskRequest struct {
	Sequence int64
	Body     []byte
	Retries  int
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

func NewFunctionInvoker(logger logger.Logger, name string, workers int) (*FunctionInvoker, error) {
	newInvoker := FunctionInvoker{logger: logger, name: name, workers: workers}
	newInvoker.instances = map[string]*functionInstance{}
	newInvoker.requestChan = make(chan *TaskRequest, requestChanSize)
	newInvoker.RespChan = make(chan *TaskResp, respChanSize)
	return &newInvoker, nil
}

func (fi *FunctionInvoker) UpdateEndpoints(endpoints map[string]string) {

	var newItems, delItems []string
	for name, addr := range endpoints {
		if _, ok := fi.instances[name]; !ok {
			newItems = append(newItems, name)
			fi.addInstance(name, addr)
		}
	}

	if len(fi.instances)+len(newItems) != len(endpoints) {
		for name, _ := range fi.instances {
			if _, ok := endpoints[name]; !ok {
				delItems = append(delItems, name)
				fi.DelInstance(name)
			}
		}
	}
	fi.logger.InfoWith("updated endpoints", "added", newItems, "deleted", delItems)
}

func (fi *FunctionInvoker) Submit(tasks []*TaskRequest) {
	for _, task := range tasks {
		fi.requestChan <- task
	}
}

func (fi *FunctionInvoker) addInstance(name, address string) {

	quits := make([]chan bool, fi.workers)
	wg := sync.WaitGroup{}
	newInstance := functionInstance{name: name, address: address, quits: quits, wg: wg}

	for i := 0; i < fi.workers; i++ {
		quit := make(chan bool, 2)
		newInstance.quits[i] = quit
		newInstance.wg.Add(1)

		go func(i int, address string, quit chan bool) {
			defer newInstance.wg.Done()

			client := retryablehttp.NewClient()
			client.RetryWaitMin = DefaultRetryMinMs * time.Millisecond
			client.RetryWaitMax = DefaultRetryMinMs * time.Millisecond * 20
			client.RetryMax = DefaultMaxRetry

			for {
				select {
				case task := <-fi.requestChan:

					req, err := retryablehttp.NewRequest("PUT", "http://"+address, task.Body)
					if err != nil {
						fi.logger.Error("failed to create HTTP request (%s)", err)
						fi.RespChan <- errResp(err, task, i, address)
						continue
					}

					if fi.logLevel != "" {
						req.Header.Set("x-nuclio-log-level", fi.logLevel)
					}
					resp, err := client.Do(req)
					if err != nil {
						fi.logger.Error("error in HTTP request (%s)", err)
						fi.RespChan <- errResp(err, task, i, address)
						continue
					}

					fmt.Println(resp.StatusCode, resp.Status)
					bodyBytes, err := ioutil.ReadAll(resp.Body)
					if err != nil {
						fi.logger.Error("error in reading request body (%s)", err)
						fi.RespChan <- errResp(err, task, i, address)
						continue
					}

					logs := resp.Header.Get("X-Nuclio-Logs")

					taskResp := TaskResp{
						Address:    address,
						Worker:     i,
						Body:       bodyBytes,
						Request:    task,
						Status:     resp.Status,
						StatusCode: resp.StatusCode,
						Log:        logs,
					}

					fi.RespChan <- &taskResp

				case _ = <-quit:
					fi.logger.DebugWith("worker done", "name", name, "worker", i)
					return
				}
			}

		}(i, address, quit)
	}

	fi.instances[name] = &newInstance
	fi.logger.DebugWith("added instance", "name", name, "workers", fi.workers)
}

func errResp(err error, req *TaskRequest, worker int, address string) *TaskResp {
	req.Retries += 1
	return &TaskResp{Err: err, Request: req, Worker: worker, Address: address}
}

func (fi *FunctionInvoker) DelInstance(name string) {

	if _, ok := fi.instances[name]; !ok {
		return
	}

	for _, doneChan := range fi.instances[name].quits {
		doneChan <- true
	}
	fi.logger.DebugWith("delete instance", "name", name, "workers",
		len(fi.instances[name].quits))

	fi.instances[name].wg.Wait()
	delete(fi.instances, name)
}
