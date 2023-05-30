package main

import (
	_ "kli8nt/log-streamer/pkg"
	client "kli8nt/log-streamer/pkg"

	"k8s.io/klog/v2"
)

func main() {

	var pods map[string]func() = make(map[string]func())

	onAdd := func(obj interface{}) {
		pod := client.ToPod(obj)
		klog.Infof("POD CREATED: %s/%s", pod.Namespace, pod.Name)
		appName := client.GetAppNameFromPod(pod)
		client.Kafka.CreateTopic(appName, client.OnError)
	}

	onUpdate := func(oldObj interface{}, newObj interface{}) {
		pod := client.ToPod(oldObj)
		newPod := client.ToPod(newObj)
		klog.Infof(
			"POD UPDATED. %s/%s %s",
			pod.Namespace, pod.Name, newPod.Status.Phase,
		)

		if newPod.Status.Phase == "Running" {
			if _, ok := pods[pod.Name]; ok {
				return
			} else {
				start, stop := client.K8s.Stream(
					pod,
					client.OnError,
				)
				pods[pod.Name] = stop
				go start()
			}
		}
	}

	onDelete := func(obj interface{}) {
		pod := client.ToPod(obj)
		klog.Infof("POD DELETED: %s/%s", pod.Namespace, pod.Name)
		if _, ok := pods[pod.Name]; ok {
			pods[pod.Name]()
			delete(pods, pod.Name)
			client.Kafka.DeleteTopic(pod.Name, client.OnError)
		} else {
			return
		}
	}

	client.K8s.ListenForPods(
		onAdd,
		onUpdate,
		onDelete,
	)

	// sleep forever
	var forever chan struct{}
	<-forever
}
