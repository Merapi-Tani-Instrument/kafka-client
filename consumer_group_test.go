package kafkaClient_test

import (
	"fmt"
	"testing"
	"time"

	kafkaClient "github.com/Merapi-Tani-Instrument/kafka-client"
)

func TestConsumerMessage(t *testing.T) {
	cofig := kafkaClient.NewConfig()
	consumer, err := kafkaClient.NewConsumerGroup("", cofig)
	if err != nil {
		fmt.Println("New Consumer")
		panic(err)
	}

	fmt.Println("Start Subscribe")
	err = consumer.Subscribe([]string{"mrt-record"}, "tester")
	if err != nil {
		fmt.Println("Subscribe Consumer")
		panic(err)
	}

	fmt.Println("Start pool")
	response, err := consumer.Pool(time.Duration(10) * time.Second)
	if response == nil && err == nil {
		fmt.Println("No data")
	} else if err != nil {
		panic(err)
	} else {
		for _, d := range response.ConsumerPartitons {
			fmt.Println("Topic  ", d.Topic, " value ", string(d.Value), " commit err ", consumer.Commit(d))
		}
	}
	response, err = consumer.Pool(time.Duration(10) * time.Second)
	if response == nil && err == nil {
		fmt.Println("No data1")
	} else if err != nil {
		panic(err)
	} else {
		for _, d := range response.ConsumerPartitons {
			fmt.Println("Topic1  ", d.Topic, " value ", string(d.Value))
		}
	}

	consumer.Close()
}
