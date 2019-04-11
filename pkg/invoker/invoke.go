package invoker

import (
	"github.com/hashicorp/go-retryablehttp"
	"github.com/nuclio/logger"
	"io/ioutil"
	"sync"
	"time"
)

type FunctionCaller struct {
	logger   logger.Logger
	name     string
	address  string
	quits    []chan bool
	wg       sync.WaitGroup
	logLevel string
	method   string
}

func (f *FunctionCaller) StartAsync(workers int, requestChan chan *TaskRequest) error {

	f.quits = make([]chan bool, workers)
	f.wg = sync.WaitGroup{}

	for i := 0; i < workers; i++ {

		quit := make(chan bool, 2)
		f.quits[i] = quit
		f.wg.Add(1)

		go func(i int, quit chan bool) {
			defer f.wg.Done()
			client := getHttpClient(f.logLevel == "debug")

			for {
				select {
				case task := <-requestChan:
					resp, err := f.Invoke(client, task)
					if err != nil {
						task.Retries++
						task.RespChan <- &TaskResp{Err: err, Request: task,
							Worker: i, Address: f.address}
						continue
					}

					resp.Worker = i
					task.RespChan <- resp

				case _ = <-quit:
					f.logger.DebugWith("worker done", "name", f.name, "worker", i)
					return
				}
			}

		}(i, quit)

	}

	return nil
}

func (f *FunctionCaller) StopAsync() error {
	for _, doneChan := range f.quits {
		doneChan <- true
	}

	f.logger.DebugWith("stop function client", "name", f.name, "workers",
		len(f.quits))

	f.wg.Wait()
	return nil
}

func (f *FunctionCaller) Invoke(client *retryablehttp.Client, task *TaskRequest) (*TaskResp, error) {

	req, err := retryablehttp.NewRequest(f.method, "http://"+f.address, task.Body)
	if err != nil {
		f.logger.Error("failed to create HTTP request (%s)", err)
		return &TaskResp{}, err
	}

	if f.logLevel != "" {
		req.Header.Set("x-nuclio-log-level", f.logLevel)
	}
	resp, err := client.Do(req)
	if err != nil {
		f.logger.Error("error in HTTP request (%s)", err)
		return &TaskResp{}, err
	}

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		f.logger.Error("error in reading request body (%s)", err)
		return &TaskResp{}, err
	}

	logs := resp.Header.Get("X-Nuclio-Logs")

	taskResp := TaskResp{
		Address:    f.address,
		Body:       bodyBytes,
		Request:    task,
		Status:     resp.Status,
		StatusCode: resp.StatusCode,
		Log:        logs,
	}

	return &taskResp, nil
}

func getHttpClient(debug bool) *retryablehttp.Client {
	client := retryablehttp.NewClient()
	client.RetryWaitMin = DefaultRetryMinMs * time.Millisecond
	client.RetryWaitMax = DefaultRetryMinMs * time.Millisecond * 20
	client.RetryMax = DefaultMaxRetry
	if !debug {
		client.Logger = nil
	}
	return client
}
