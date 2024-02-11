package kafkaClient

type ProducerMessage struct {
	Data      Encoder
	Topic     string
	Partition int
	Result    chan *ProducerMessageResult
}

func NewProducerMessage(message Encoder, topic string) *ProducerMessage {
	return &ProducerMessage{
		Result:    make(chan *ProducerMessageResult),
		Data:      message,
		Topic:     topic,
		Partition: 0,
	}
}

type ProducerMessageResult struct {
	Err error
}

type producerMetadataResponse struct {
	res *MetadataResponse
	err error
}
type producerMetadataRequest struct {
	req *MetadataRequest
	res chan *producerMetadataResponse
}
