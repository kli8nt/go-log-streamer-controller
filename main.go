package main

import (
	_ "kli8nt/log-streamer/pkg"
	k8s "kli8nt/log-streamer/pkg"
)

func main() {

	client := k8s.Client{}
	err := client.Init()

	if err != nil {
		panic(err)
	}

	pod := "nginx-ts-57b689c7fd-7xpcs"

	go client.Stream(pod, func(err error) {
		panic(err)
	})



	// sleep forever
	var forever chan struct{}
	<-forever
}