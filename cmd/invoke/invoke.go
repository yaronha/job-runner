package main

import (
	"flag"
	"fmt"
	"github.com/yaronha/job-runner/pkg/common"
	"github.com/yaronha/job-runner/pkg/invoker"
	"io/ioutil"
	"os"
)

func main() {
	err := Run()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}
}

func Run() error {

	address := flag.String("a", "", "function address (URL)")
	method := flag.String("m", "", "HTTP Method")
	logLevel := flag.String("l", "info", "log level: info | debug")
	message := flag.String("b", "", "message body")
	flag.Parse()

	log, err := common.NewLogger(*logLevel)
	if err != nil {
		return err
	}

	cfg := invoker.ClientConfig{Logger: log, LogLevel: *logLevel,
		Name: *address, Method: *method}

	c, err := invoker.NewSimpleClient(&cfg)
	if err != nil {
		return err
	}

	// TODO: read body rows from file/list
	i := 0

	task := &invoker.TaskRequest{Sequence: int64(i), Body: []byte(*message)}
	msg, err := c.Submit(task)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile("/tmp/output", msg.Body, 0644)
	if err != nil {
		return err
	}
	fmt.Printf("%s\n%s\n",
		msg.Body, msg.Status)
	rows, err := invoker.LogToStrings([]byte(msg.Log))
	if err != nil {
		return err
	}

	for _, row := range rows {
		fmt.Println(row)
	}

	return nil
}
