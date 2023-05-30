package pkg

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	KF "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/segmentio/kafka-go"
)

type KConfig struct {
	host string
	port string
	deadline int 
	topic string
}

type Kf struct {
	config *KF.ConfigMap
	topic string
	dialer *kafka.Dialer
	cleanups []func()
}

type KReader struct {
	topic string
	connection *kafka.Reader
}

type KWriter struct {
	topic string
	p 	  *KF.Producer
}

var Kafka Kf


func init() {
	kafkaPort := os.Getenv("KAFKA_PORT")
	kafkaHost := os.Getenv("KAFKA_HOST")
	kafkaUser := os.Getenv("KAFKA_USERNAME")
	kafkaPass := os.Getenv("KAFKA_PASSWORD")

	config := &KF.ConfigMap{
        "bootstrap.servers":                     fmt.Sprintf("%s:%s", kafkaHost, kafkaPort),
        "group.id":                              "myGroup",
        "security.protocol":                     "SASL_SSL",
        "sasl.mechanisms":                       "PLAIN",
        "sasl.username":                         kafkaUser,
        "sasl.password":                         kafkaPass,
        "session.timeout.ms":                    "45000",
        "auto.offset.reset":                     "earliest",
	}
	
	log.Println("Connecting to Kafka...")
	
	Kafka.Init(config)
	defer Kafka.Close()
	
	
}

func (kf *Kf) Init(config *KF.ConfigMap) {
	kf.config = config
}


func (kf *Kf) CreateTopic(topic string, onError func(error, string)) {
	url := os.Getenv("KAFKA_REST_API") + "/topics"
	key := os.Getenv("KAFKA_REST_API_KEY")

	data := []byte(`{"topic_name": "` + topic + `", "partitions_count": 1, "replication_factor": 3}`)
    request, error := http.NewRequest("POST", url, bytes.NewBuffer(data))
    request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Basic " + key)

    client := &http.Client{}
    response, error := client.Do(request)

    if error != nil {
		onError(error, "Failed to create topic")
		return
    }

    responseBody, error := io.ReadAll(response.Body)

    if error != nil {
		onError(error, "Failed to read response body")
		return
    }

	log.Println(string(responseBody))
	 
	if strings.Contains(string(responseBody), "error_code") && strings.Contains(string(responseBody), "40002") {
		onError(error, "Topic already exists")
	} else if strings.Contains(string(responseBody), "error_code") {
		onError(error, "Failed to create topic")
	} else {
		log.Println("Topic '" + topic + "' created successfully")
	}

    defer response.Body.Close()
}


func (kf *Kf) DeleteTopic(topic string, onError func(error, string)) {
	if topic == "" {
		onError(nil, "Topic is required")
		return
	}

	url := os.Getenv("KAFKA_REST_API") + "/topics/" + topic
	key := os.Getenv("KAFKA_REST_API_KEY")
	data := []byte(`{"topic_name": "` + topic + `"}`)
    request, error := http.NewRequest("DELETE", url, bytes.NewBuffer(data))
    request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Basic " + key)

    client := &http.Client{}
    response, error := client.Do(request)

    if error != nil {
		onError(error, "Failed to create topic")
		return
    }

    responseBody, error := io.ReadAll(response.Body)

    if error != nil {
		onError(error, "Failed to read response body")
		return
    }

	log.Println(string(responseBody))
	 
	if strings.Contains(string(responseBody), "error_code") && strings.Contains(string(responseBody), "40002") {
		onError(error, "Topic already exists")
	} else if strings.Contains(string(responseBody), "error_code") {
		onError(error, "Failed to create topic")
	} else {
		log.Println("Topic '" + topic + "' deleted successfully")
	}


    defer response.Body.Close()
}

func (kf *Kf) Writer(topic string) (KWriter) {
	p, err := KF.NewProducer(kf.config)

	OnError(err, "Failed to create producer")

	kf.cleanups = append(kf.cleanups, func() {
		p.Close()
	})

	kWriter := KWriter{
		topic: topic,
		p: p,
	}
	return kWriter
}

// func (kf *Kf) Reader(offset int64) (KReader) {
// 	topic := kf.config.topic
// 	uri := fmt.Sprintf("%s:%s", kf.config.host, kf.config.port)

// 	connection := kafka.NewReader(kafka.ReaderConfig{
// 		Brokers:   []string{uri},
// 		Topic:     topic,
// 		Partition: 0,
// 		MaxBytes:  10e6, // 10MB
// 		Dialer:   kf.dialer,
// 	})
// 	connection.SetOffset(offset)

// 	kReader := KReader{connection: connection, topic: topic}
// 	kf.cleanups = append(kf.cleanups, func() {
// 		kReader.connection.Close()
// 	})

// 	log.Println("Connected to Kafka")
// 	return kReader
// }



// func (kReader *KReader) Read(cb func(key string, msg []byte)) {
// 	log.Printf(" [*] Waiting for messages. for Topic %s", kReader.topic)

// 	for {
// 		message, err := kReader.connection.ReadMessage(context.Background())
// 		OnError(err, "Failed to listen to this message")

// 		key := string(message.Key)
// 		value :=message.Value
// 		cb(key, value)	
// 	}
// }



func (kWriter *KWriter) Write(key string, body []byte) {
	topic := kWriter.topic

	fmt.Println(topic)
	
	message := &KF.Message{
		TopicPartition: KF.TopicPartition{Topic: &topic},
		Key:   []byte(key),
		Value: body,
	}

	err := kWriter.p.Produce(message, nil)
	OnError(err, "Failed to write message")
}

func (kf *Kf) Close() {
	for _, cleanup := range kf.cleanups {
		cleanup()
	}
}