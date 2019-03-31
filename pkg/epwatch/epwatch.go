package epwatch

import (
	"fmt"
	"github.com/nuclio/logger"
	"github.com/pkg/errors"
	"k8s.io/api/core/v1"
	k8err "k8s.io/apimachinery/pkg/api/errors"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"time"
)

const NUCLIO_SELECTOR = "nuclio.io/class=function"

func NewKubeClient(logger logger.Logger, kubeconf, namespace string, reqChannel chan *AsyncRequests) (*kubeClient, error) {
	newClient := &kubeClient{logger: logger, namespace: namespace, kubeconf: kubeconf}
	newClient.reqChannel = reqChannel

	err := newClient.newClientConf()

	return newClient, err
}

type kubeClient struct {
	logger     logger.Logger
	kubeClient *kubernetes.Clientset
	restConfig *rest.Config
	namespace  string
	kubeconf   string
	reqChannel chan *AsyncRequests
}

type EventChangeType int

const (
	EndPointUpdated EventChangeType = iota
	EndPointDeleted
)

type AsyncRequests struct {
	Type    EventChangeType
	EPs     *FunctionEndPoints
	ErrChan chan error
}

func (kc *kubeClient) newClientConf() error {

	var err error
	kc.restConfig, err = clientcmd.BuildConfigFromFlags("", kc.kubeconf)
	if err != nil {
		return errors.Wrap(err, "Failed to create REST config")
	}

	kc.kubeClient, err = kubernetes.NewForConfig(kc.restConfig)
	if err != nil {
		return err
	}

	// create a client for function custom resources
	return nil
}

func (kc *kubeClient) GetPods() {
	clientset := kc.kubeClient
	for {
		pods, err := clientset.CoreV1().Pods("").List(meta_v1.ListOptions{})
		if err != nil {
			panic(err.Error())
		}
		fmt.Printf("There are %d pods in the cluster\n", len(pods.Items))

		for _, pod := range pods.Items {
			fmt.Println(pod)
		}

		// Examples for error handling:
		// - Use helper functions like e.g. errors.IsNotFound()
		// - And/or cast to StatusError and use its properties like e.g. ErrStatus.Message
		pod := "etcd-yaronh"
		_, err = clientset.CoreV1().Pods(kc.namespace).Get(pod, meta_v1.GetOptions{})
		if k8err.IsNotFound(err) {
			fmt.Printf("Pod %s in namespace %s not found\n", pod, kc.namespace)
		} else if statusError, isStatus := err.(*k8err.StatusError); isStatus {
			fmt.Printf("Error getting pod %s in namespace %s: %v\n",
				pod, kc.namespace, statusError.ErrStatus.Message)
		} else if err != nil {
			panic(err.Error())
		} else {
			fmt.Printf("Found pod %s in namespace %s\n", pod, kc.namespace)
		}

		time.Sleep(10 * time.Second)
	}

}

type FunctionEndPoints struct {
	Namespace string
	Name      string
	Version   string
	Labels    map[string]string
	Pods      []EndPoint
}

type EndPoint struct {
	Name string
	Addr string
}

func (kc *kubeClient) processEP(ep *v1.Endpoints, change EventChangeType) {

	if len(ep.Subsets) == 0 {
		return
	}

	epItem := FunctionEndPoints{Namespace: ep.Namespace, Name: ep.Name, Labels: ep.Labels}
	eps := []EndPoint{}
	for _, subset := range ep.Subsets {
		if len(subset.Ports) == 0 {
			continue
		}
		// assumes a function has a single port, if not need the getPorts below
		for _, addr := range subset.Addresses {
			ep := EndPoint{}
			if addr.TargetRef != nil {
				ep.Name = addr.TargetRef.Name
			}
			ep.Addr = fmt.Sprintf("%s:%d", addr.IP, subset.Ports[0].Port)
			eps = append(eps, ep)
		}
	}
	epItem.Pods = eps

	kc.reqChannel <- &AsyncRequests{Type: change, EPs: &epItem}

}

func getPorts(subset v1.EndpointSubset) []int32 {
	ports := []int32{}
	for _, port := range subset.Ports {
		if port.Protocol == "TCP" {
			ports = append(ports, port.Port)
		}
	}
	return ports
}

func (kc *kubeClient) NewEPWatcher() error {

	logger := kc.logger.GetChild("epWatcher").(logger.Logger)
	logger.Debug("Watching for Endpoint changes")

	opts := meta_v1.ListOptions{
		LabelSelector: NUCLIO_SELECTOR,
	}

	listWatch := &cache.ListWatch{
		ListFunc: func(options meta_v1.ListOptions) (runtime.Object, error) {
			return kc.kubeClient.CoreV1().Endpoints(kc.namespace).List(opts)
		},
		WatchFunc: func(options meta_v1.ListOptions) (watch.Interface, error) {
			return kc.kubeClient.CoreV1().Endpoints(kc.namespace).Watch(opts)
		},
	}

	_, controller := cache.NewInformer(
		listWatch,
		&v1.Endpoints{},
		time.Minute*10,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				kc.processEP(obj.(*v1.Endpoints), EndPointUpdated)
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				kc.processEP(newObj.(*v1.Endpoints), EndPointUpdated)
			},
			DeleteFunc: func(obj interface{}) {
				fmt.Println("DELETED: ")
				kc.processEP(obj.(*v1.Endpoints), EndPointDeleted)
			},
		},
	)

	// run the watcher. TODO: pass a channel that can receive stop requests, when stop is supported
	go controller.Run(make(chan struct{}))

	return nil
}
