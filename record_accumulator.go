package kafkaClient

import (
	"sync"
	"time"
)

type RecordBuffer struct {
	data []*ProducerMessage
	size int
}

type RecordAccumulator struct {
	config       *Config
	lock         sync.Mutex
	buffer       RecordBuffer
	lastAttempMs int64
}

func newRecordAccumulator(config *Config) *RecordAccumulator {
	return &RecordAccumulator{
		config:       config,
		lastAttempMs: time.Now().UnixMilli(),
	}
}

type RecordBufferGroup map[string][]*ProducerMessage

func (r RecordBufferGroup) GetAllTopic() []string {
	var topics []string
	for t := range r {
		topics = append(topics, t)
	}
	return topics
}

func (r *RecordBuffer) groupByTopic() RecordBufferGroup {
	res := make(map[string][]*ProducerMessage)
	for _, pm := range r.data {
		if appender, ok := res[pm.Topic]; ok {
			appender = append(appender, pm)
			res[pm.Topic] = appender
		} else {
			res[pm.Topic] = []*ProducerMessage{pm}
		}
	}
	return res
}

func (r *RecordAccumulator) WaitTime(nowMs int64) int64 {
	w := nowMs - r.lastAttempMs
	if w < 0 {
		return 0
	}
	return w
}

func (r *RecordAccumulator) UpdateLastAttemp(nowMs int64) {
	r.lastAttempMs = nowMs
}
func (r *RecordAccumulator) Append(data *ProducerMessage) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.buffer.data = append(r.buffer.data, data)
	r.buffer.size += data.Data.Length()
}

func (r *RecordAccumulator) Full() bool {
	return r.buffer.size >= r.config.Producer.MaxMessageBytes
}

func (r *RecordAccumulator) Ready() bool {
	return r.buffer.size > 0
}

func (r *RecordAccumulator) Drain() *RecordBuffer {
	r.lock.Lock()
	defer r.lock.Unlock()
	rb := new(RecordBuffer)
	for _, d := range r.buffer.data {
		rb.data = append(rb.data, d)
	}
	rb.size = r.buffer.size
	r.buffer.size = 0
	r.buffer.data = r.buffer.data[:0]
	return rb
}

func (r *RecordBuffer) SendResultError(err error) {
	res := &ProducerMessageResult{
		Err: err,
	}
	for _, rb := range r.data {
		rb.Result <- res
	}
}

func (r *RecordBuffer) SendResultErrorByTopic(topic string, err error) {
	res := &ProducerMessageResult{
		Err: err,
	}
	for _, rb := range r.data {
		if rb.Topic == topic {
			rb.Result <- res
		}
	}
}
