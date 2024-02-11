package kafkaClient_test

import (
	"fmt"
	"testing"
	"time"

	kafkaClient "github.com/Merapi-Tani-Instrument/kafka-client"
)

func TestSendMessage(t *testing.T) {
	cofig := kafkaClient.NewConfig()
	producer, err := kafkaClient.NewProducer("34.128.82.87:9092", cofig)
	if err != nil {
		panic(err)
	}
	go func() {
		msg1 := kafkaClient.NewProducerMessage(kafkaClient.StringEncoder("StringEncoder20"), "mrt-record")
		if err = producer.Send(msg1); err != nil {
			panic(err)
		}
		fmt.Println("Result1 ", <-msg1.Result)
	}()
	go func() {
		msg2 := kafkaClient.NewProducerMessage(kafkaClient.StringEncoder("StringEncoder21"), "mrt-record")
		if err = producer.Send(msg2); err != nil {
			panic(err)
		}
		fmt.Println("Result2 ", <-msg2.Result)
	}()
	go func() {
		msg3 := kafkaClient.NewProducerMessage(kafkaClient.StringEncoder("StringEncoder22"), "mrt-record")
		if err = producer.Send(msg3); err != nil {
			panic(err)
		}
		fmt.Println("Result3 ", <-msg3.Result)
	}()
	time.Sleep(time.Duration(5) * time.Second)
	producer.Close()
}
