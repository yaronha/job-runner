package invoker

import (
	"github.com/nuclio/logger"
)

type FunctionInvoker struct {
	logger      logger.Logger
	logLevel    string
	name        string
	workers     int
	instances   map[string]*FunctionCaller
	requestChan chan *TaskRequest

	RespChan chan *TaskResp
}

func NewFunctionInvoker(logger logger.Logger, name string, workers int) (*FunctionInvoker, error) {
	newInvoker := FunctionInvoker{logger: logger, name: name, workers: workers}
	newInvoker.instances = map[string]*FunctionCaller{}
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
	newInstance := FunctionCaller{name: name, address: address}
	newInstance.StartAsync(fi.workers, fi.requestChan)
	fi.instances[name] = &newInstance
	fi.logger.DebugWith("added instance", "name", name, "workers", fi.workers)
}

func (fi *FunctionInvoker) DelInstance(name string) {

	if _, ok := fi.instances[name]; !ok {
		return
	}

	fi.instances[name].StopAsync()
	delete(fi.instances, name)
}
