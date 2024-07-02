package kafkaClient_test

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	kafkaClient "github.com/Merapi-Tani-Instrument/kafka-client"
)

func TestConsumerMessage(t *testing.T) {
	kafkaClient.Logger = log.New(os.Stdout, "[Test]", 1)
	cofig := kafkaClient.NewConfig()
	consumer, err := kafkaClient.NewConsumerGroup("127.0.01:9092", cofig)
	if err != nil {
		fmt.Println("New Consumer")
	}

	hasSubscribe := false
	for {
		if !hasSubscribe {
			fmt.Println("Start Subscribe")
			err = consumer.Subscribe([]string{"mrt-record"}, "tester")
		}

		if err != nil {
			fmt.Println("Subscribe Consumer Error", err)
			time.Sleep(time.Duration(2) * time.Second)
			hasSubscribe = false
		} else {
			fmt.Println("Has subscribe")
			hasSubscribe = true
		}
		if !hasSubscribe {
			fmt.Println("Error and continue")
			continue
		}

		response, err := consumer.Pool(time.Duration(10) * time.Second)
		if response == nil && err == nil {
			fmt.Println("No data")
		} else if err != nil {
			fmt.Println("Pool error: ", err)
			time.Sleep(time.Duration(2) * time.Second)

			consumer.Close()
			hasSubscribe = false
		} else {
			for _, d := range response.ConsumerPartitons {
				fmt.Println("Topic  ", d.Topic, " value ", string(d.Value), " commit err ", consumer.Commit(d))
			}
		}
	}
}
