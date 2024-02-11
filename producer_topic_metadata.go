package kafkaClient

import (
	"errors"
	"slices"
	"sync"
	"sync/atomic"
)

var (
	MetadataForTopic = errors.New("No Metadata for topic")
)

type TopicPartition struct {
	topicName    string
	partitionIds []int
}

type ProducerTopicPartition struct {
	topicPartitions []*TopicPartition
	selector        int32
	lock            sync.Mutex
}

func newProducerTopicPartition() *ProducerTopicPartition {
	return &ProducerTopicPartition{}
}

func (p *ProducerTopicPartition) partitionForTopic(topic string, req chan *producerMetadataRequest, calculatePartition bool) (int, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	var tp *TopicPartition = nil
	for _, t := range p.topicPartitions {
		if t.topicName == topic {
			tp = t
		}
	}
	if tp == nil {
		res := make(chan *producerMetadataResponse)
		metadataReq := &producerMetadataRequest{
			req: NewMetadataRequest(V2_0_0_0, []string{topic}),
			res: res,
		}
		req <- metadataReq
		metadataResponse := <-metadataReq.res
		if metadataResponse.err != nil {
			return 0, metadataResponse.err
		} else if metadataResponse.res.Topics == nil || len(metadataResponse.res.Topics) == 0 {
			return 0, MetadataForTopic
		}
		for _, topicMeta := range metadataResponse.res.Topics {
			if resParse := p.parseFromTopicMetadata(topicMeta); resParse != nil && topicMeta.Name == topic {
				tp = resParse
			}
		}
		if tp == nil {
			return 0, MetadataForTopic
		}
	}

	if !calculatePartition {
		return 0, nil
	}

	selectPartition := atomic.AddInt32(&p.selector, 1)
	selectPartition = selectPartition % int32(len(tp.partitionIds))
	return tp.partitionIds[selectPartition], nil
}

func (p *ProducerTopicPartition) parseFromTopicMetadata(tm *TopicMetadata) *TopicPartition {
	if tm.Err != ErrNoError {
		return nil
	}
	var tp *TopicPartition
	if i := slices.IndexFunc[[]*TopicPartition, *TopicPartition](p.topicPartitions, func(t *TopicPartition) bool {
		return t.topicName == tm.Name
	}); i > 0 {
		tp = p.topicPartitions[i]
	} else {
		tp = &TopicPartition{
			topicName: tm.Name,
		}
	}
	tp.partitionIds = make([]int, len(tm.Partitions))
	for v, m := range tm.Partitions {
		tp.partitionIds[v] = int(m.ID)
	}
	p.topicPartitions = append(p.topicPartitions, tp)
	return tp
}

func producerRequestMetadata(ctx *ProducerContext, pm *producerMetadataRequest) {
	res, err := ctx.broker.GetMetadata(pm.req)
	pm.res <- &producerMetadataResponse{
		res: res,
		err: err,
	}
}
