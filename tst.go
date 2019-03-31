/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Note: the example only works with the code within the same release/branch.
package main

import (
	"flag"
	"fmt"
	"github.com/yaronha/job-runner/pkg/common"
	"github.com/yaronha/job-runner/pkg/epwatch"
	"time"
	//
	// Uncomment to load all auth plugins
	// _ "k8s.io/client-go/plugin/pkg/client/auth"
	//
	// Or uncomment to load specific auth plugins
	// _ "k8s.io/client-go/plugin/pkg/client/auth/azure"
	// _ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	// _ "k8s.io/client-go/plugin/pkg/client/auth/oidc"
	// _ "k8s.io/client-go/plugin/pkg/client/auth/openstack"
)

func main() {

	kubeconfig := flag.String("kubeconfig", "config2", "absolute path to the kubeconfig file")
	flag.Parse()

	logger, _ := common.NewLogger("debug")
	requests := make(chan *epwatch.AsyncRequests, 100)
	client, err := epwatch.NewKubeClient(logger, *kubeconfig, "", requests)
	// use the current context in kubeconfig
	if err != nil {
		panic(err.Error())
	}

	err = client.NewEPWatcher()
	if err != nil {
		panic(err.Error())
	}

	go func() {
		for {

			select {

			case req := <-requests:
				fmt.Printf("%s   %s\n", req.EPs.Name, time.Now())
				fmt.Printf("  Labels: %+v\n", req.EPs.Labels)
				fmt.Printf("  Pods: %+v\n\n", req.EPs.Pods)

			}
		}
	}()

	time.Sleep(200 * time.Second)
}
