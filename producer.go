package kafkaClient

import (
	"errors"
	"slices"
	"time"
)

const (
	batchDelta = time.Duration(1) * time.Millisecond
)

var ErrNoResponse = errors.New("No Response")

type Encoder interface {
	Encode() []byte
	Length() int
}

type StringEncoder string

func (s StringEncoder) Encode() []byte {
	return []byte(s)
}

func (s StringEncoder) Length() int {
	return len(s)
}

type ProducerContext struct {
	config          *Config
	broker          *Broker
	stop            bool
	done            chan bool
	accumulator     *RecordAccumulator
	topicPartition  *ProducerTopicPartition
	produceRequest  *ProduceRequest
	metadataRequest chan *producerMetadataRequest
}

type Producer interface {
	Send(data *ProducerMessage) error
	Close()
}

func NewProducer(addr string, config *Config) (Producer, error) {
	broker := NewBroker(addr)
	if err := broker.Open(config); err != nil {
		return nil, err
	}
	ctx := &ProducerContext{
		broker:          broker,
		config:          config,
		accumulator:     newRecordAccumulator(config),
		topicPartition:  newProducerTopicPartition(),
		produceRequest:  newProducerRequest(),
		done:            make(chan bool),
		metadataRequest: make(chan *producerMetadataRequest),
		stop:            false,
	}
	go ctx.task()
	return ctx, nil
}

func newProducerRequest() *ProduceRequest {
	return &ProduceRequest{
		RequiredAcks: WaitForAll,
		Timeout:      500,
		Version:      2,
	}
}

func (ctx *ProducerContext) task() {
	for !ctx.stop {
		now := time.Now()
		select {
		case metadata := <-ctx.metadataRequest:
			producerRequestMetadata(ctx, metadata)
		default:
			ctx.sendProducerData(now)
			time.Sleep(time.Duration(100) * time.Millisecond)
		}
	}
	ctx.broker.Close()
	ctx.done <- true
}

func (ctx *ProducerContext) sendProducerData(nowS time.Time) {
	ms := nowS.UnixMilli()
	sendable := ctx.accumulator.Full() || (ctx.accumulator.Ready() && ctx.accumulator.WaitTime(ms) > ctx.config.Producer.lingerMs.Milliseconds())
	if sendable {
		ctx.accumulator.UpdateLastAttemp(ms)
		records := ctx.accumulator.Drain()
		recordGroups := records.groupByTopic()
		ctx.newMessageBatch(recordGroups)
		res, err := ctx.broker.Produce(ctx.produceRequest)
		if err != nil {
			records.SendResultError(err)
		} else {
			allTopicRecord := recordGroups.GetAllTopic()
			for topic, block := range res.Blocks {
				for _, response := range block {
					errItem := &ProducerMessageResult{
						Err: response.Err,
					}
					if rg, ok := recordGroups[topic]; ok {
						for _, rgItem := range rg {
							rgItem.Result <- errItem
						}
						allTopicRecord = slices.DeleteFunc[[]string, string](allTopicRecord, func(s string) bool { return s == topic })
					}
				}
			}
			for _, topic := range allTopicRecord {
				records.SendResultErrorByTopic(topic, ErrNoResponse)
			}
		}
		ctx.produceRequest = nil
	}
}

func (ctx *ProducerContext) Close() {
	ctx.stop = true
	<-ctx.done
}

func (ctx *ProducerContext) Send(data *ProducerMessage) error {
	calculatePartition := data.Partition < 0
	p, err := ctx.topicPartition.partitionForTopic(data.Topic, ctx.metadataRequest, calculatePartition)
	if err != nil {
		return err
	}
	if calculatePartition {
		data.Partition = p
	}
	ctx.accumulator.Append(data)
	return nil
}

func (ctx *ProducerContext) newMessageBatch(buffer map[string][]*ProducerMessage) {
	for t, rec := range buffer {
		batch := &RecordBatch{
			Version:          2,
			Codec:            CompressionNone,
			CompressionLevel: CompressionLevelDefault,
			ProducerID:       -1,
			ProducerEpoch:    -1,
		}
		partition := 0
		timestampDelta := batchDelta
		for _, msg := range rec {
			batch.Records = append(batch.Records, &Record{
				Value:          msg.Data.Encode(),
				TimestampDelta: batchDelta,
			})
			timestampDelta += 1
			partition = msg.Partition
		}
		for _, msg := range rec {
			msg.Partition = partition
		}
		for i, rec := range batch.Records {
			rec.OffsetDelta = int64(i)
		}

		batch.LastOffsetDelta = int32(len(batch.Records)) - 1
		ctx.produceRequest.AddBatch(t, int32(partition), batch)
	}
}
