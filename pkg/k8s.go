package pkg

import (
	"context"
	"fmt"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type Client struct {
	clientSet *kubernetes.Clientset
}


func (client *Client) Init() (error) {
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

		fmt.Println("Initiated Kubernetes Client")
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
		fmt.Println("Initiated Kubernetes Client")
		return nil
	}
}

func (client *Client) Stream(podName string, onError func(error)) {
	namespace := os.Getenv("NAMESPACE") 
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
		onError(err)
		return
	}


	defer stream.Close()

	fmt.Printf("Started Streaming Logs from %s [namespace: %s]\n", podName, namespace)

	buf := make([]byte, 1024)
	// TODO: to Ticker
	for {
		n, err := stream.Read(buf)
		if err != nil {
			onError(err)
			return
		}

		data := buf[:n]
		fmt.Println(string(data))
		time.Sleep(1 * time.Second)
	}
}