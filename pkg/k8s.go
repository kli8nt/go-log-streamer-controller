package pkg

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

type Client struct {
	clientSet *kubernetes.Clientset
}

func ToPod(obj interface{}) *corev1.Pod {
	return obj.(*corev1.Pod)
}

func GetAppNameFromPod(pod *corev1.Pod) string {
	return pod.Labels["app"]
}

var K8s Client

func init() {
	err := K8s.Init()

	if err != nil {
		panic(err)
	}
}

func (client *Client) Init() error {
	withKubeconfig := os.Getenv("WITH_KUBECONFIG")
	if withKubeconfig != "" {
		// use the current context in kubeconfig
		config, err := clientcmd.BuildConfigFromFlags("", withKubeconfig)
		if err != nil {
			return err
		}

		// create the Clientset
		clientSet, err := kubernetes.NewForConfig(config)
		client.clientSet = clientSet
		if err != nil {
			return err
		}

		log.Println("Initiated Kubernetes Client")
		// Retrieve the CA certificate data
		return nil
	} else {
		// creates the in-cluster config
		config, err := rest.InClusterConfig()
		if err != nil {
			return err
		}

		// create the Clientset
		clientSet, err := kubernetes.NewForConfig(config)
		if err != nil {
			return err
		}
		client.clientSet = clientSet
		log.Println("Initiated Kubernetes Client")
		return nil
	}
}

func (client *Client) Stream(pod *corev1.Pod, onError func(error, string)) (func(), func()) {
	podName := pod.Name
	namespace := os.Getenv("NAMESPACE")

	deploymentName := GetAppNameFromPod(pod)
	writer := Kafka.Writer(deploymentName)
	

	if namespace == "" {
		namespace = "default"
	}

	logsOptions := &corev1.PodLogOptions{
		Follow: true,
	}

	request := client.clientSet.
		CoreV1().
		Pods(namespace).
		GetLogs(podName, logsOptions)

	ctx := context.Background()

	stream, err := request.Stream(ctx)
	if err != nil {
		onError(err, "Pod Error")
		return func() {}, func() {}
	}

	stop := func() {
		err := stream.Close()
		if err != nil {
			onError(err, "Stream Error")
		}
	}

	start := func() {
		log.Printf("Started Streaming Logs from %s [namespace: %s]\n", podName, namespace)

		buf := make([]byte, 1024)

		for {
			n, err := stream.Read(buf)
			if err != nil {
				onError(err, "Stream Error")
				return
			}
			fmt.Println("sending", string(buf[:n]))
			data := buf[:n]


			writer.Write(deploymentName, data)
			time.Sleep(1 * time.Second)
		}
	}

	return start, stop

}

func (client *Client) ListenForPods(
	onAdd func(interface{}),
	onUpdate func(interface{}, interface{}),
	onDelete func(interface{}),
) {
	stopper := make(chan struct{})
	defer close(stopper)

	namespace := os.Getenv("NAMESPACE")
	if namespace == "" {
		namespace = "default"
	}

	factory := informers.NewSharedInformerFactoryWithOptions(client.clientSet, 10*time.Second, informers.WithNamespace(namespace))
	podInformer := factory.Core().V1().Pods().Informer()

	defer runtime.HandleCrash()

	// start informer ->
	go factory.Start(stopper)

	if !cache.WaitForCacheSync(stopper, podInformer.HasSynced) {
		runtime.HandleError(fmt.Errorf("Timed out waiting for caches to sync"))
		return
	}

	podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    onAdd,
		UpdateFunc: onUpdate,
		DeleteFunc: onDelete,
	})

	<-stopper
}
