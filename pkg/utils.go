package pkg

import (
	"encoding/json"
	"log"
)

type Message struct {
	Pod       string `json:"pod"`
	Container string `json:"container"`
}

func OnError(err error, msg string) {
	if err != nil {
		// print stack
		
		log.Printf("%s: %s", msg, err)
	}
}

func DecodeMessage(msg []byte) Message {
	message := Message{}
	err := json.Unmarshal(msg, &message)
	if err != nil {
		OnError(err, "Failed to unmarshal message")
	}
	return message
}
