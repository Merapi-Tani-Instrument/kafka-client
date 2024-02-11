package kafkaClient

type RecordMetadata struct {
	Offset    int64
	Timestamp int64
	Error     KError
	Done      chan bool
}
