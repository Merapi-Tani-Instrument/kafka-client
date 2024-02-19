package kafkaClient

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"
)

var ErrClosedConsumerGroup = errors.New("kafka: tried to use a consumer group that was closed")
var ErrAlreadyConsume = errors.New("kafka: already consume")
var ErrTopicConsumerNotProvide = errors.New("no topics provided")
var ErrNoJobs = errors.New("No Jobs")

type ConsumerMultiError []string

type ConsumerMessage struct {
	Headers        []*RecordHeader // only set if kafka is version 0.11+
	Timestamp      time.Time       // only set if kafka is version 0.10+, inner message timestamp
	BlockTimestamp time.Time       // only set if kafka is version 0.10+, outer (compressed) block timestamp

	Key, Value []byte
	Topic      string
	Partition  int32
	Offset     int64
}

func (c ConsumerMultiError) Error() string {
	return strings.Join(c, ", ")
}

type ConsumerGroup interface {
	Subscribe(topics []string, groupId string) error
	Pool(duration time.Duration) (*ConsumerResult, error)
	Commit(result *ConsumerResultPartition) error
	Close() error
}

type ConsumerGroupContext struct {
	config                *Config
	broker                *Broker
	ready                 bool
	lock                  sync.Mutex
	session               *consumerGroupSession
	brokerMetadata        []*Broker
	topicLeaderAndReplica map[string]map[int32]*topicLeaderAndReplica //[topic][partition][data]
	groupId               string
}

func NewConsumerGroup(addr string, config *Config) (ConsumerGroup, error) {
	broker := NewBroker(addr)
	if err := broker.Open(config); err != nil {
		return nil, err
	}
	res := &ConsumerGroupContext{
		config: config,
		broker: broker,
		ready:  false,
	}
	return res, nil
}

func (c *ConsumerGroupContext) Pool(duration time.Duration) (*ConsumerResult, error) {
	expired := time.Now().Add(duration)
	for expired.After(time.Now()) {
		select {
		case result := <-c.session.result:
			return result, nil
		default:
		}
		if c.session.closed {
			return nil, ErrNotConnected
		}
		time.Sleep(time.Duration(200) * time.Millisecond)
	}

	return nil, nil
}

func (c *ConsumerGroupContext) Commit(result *ConsumerResultPartition) error {
	req := &OffsetCommitRequest{
		Version:                 4,
		ConsumerGroup:           c.groupId,
		GroupInstanceId:         c.session.groupInstanceId,
		ConsumerID:              c.session.memberID,
		ConsumerGroupGeneration: c.session.generationId,
	}
	req.AddBlock(result.Topic, result.Partition, result.Offset+1, result.Timestamp.Unix(), "")
	res, err := result.broker.CommitOffset(req)

	if err != nil {
		return err
	}

	if errTopic, ok := res.Errors[result.Topic]; ok {
		if errPartition, ok := errTopic[result.Partition]; ok {
			return errPartition
		}
	}
	return ErrUnknown
}

func (c *ConsumerGroupContext) Subscribe(topics []string, groupId string) error {
	if c.broker.opened != 1 {
		return ErrClosedConsumerGroup
	} else if c.ready {
		return ErrAlreadyConsume
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	if len(topics) == 0 {
		return ErrTopicConsumerNotProvide
	}
	c.groupId = groupId

	c.broker.Open(c.config)

	if err := c.refreshMetadata(topics); err != nil {
		return err
	}

	if c.session == nil {
		c.session = newSession(c)
	}
	err := c.session.startSession(groupId, topics)
	if err != nil {
		c.ready = false
	} else {
		c.ready = true
	}

	return err
}

func (c *ConsumerGroupContext) refreshMetadata(topic []string) error {
	req := NewMetadataRequest(V2_0_0_0, topic)
	req.AllowAutoTopicCreation = true
	res, err := c.broker.GetMetadata(req)
	if err != nil {
		return err
	}

	var ce ConsumerMultiError
	c.topicLeaderAndReplica = newTopicLeaderAndReplica()
	for _, t := range topic {
		idx := slices.IndexFunc[[]*TopicMetadata, *TopicMetadata](res.Topics, func(tm *TopicMetadata) bool { return tm.Name == t })
		if idx < 0 {
			ce = append(ce, fmt.Sprintf("No topic meta for topic %s", t))
		} else if res.Topics[idx].Err != ErrNoError {
			ce = append(ce, fmt.Sprintf("Error topic meta for topic %s", res.Topics[idx].Err.Error()))
		} else {
			for _, p := range res.Topics[idx].Partitions {
				if _, ok := c.topicLeaderAndReplica[res.Topics[idx].Name]; !ok {
					c.topicLeaderAndReplica[res.Topics[idx].Name] = make(map[int32]*topicLeaderAndReplica)
				}
				c.topicLeaderAndReplica[res.Topics[idx].Name][p.ID] = &topicLeaderAndReplica{
					leader:      p.Leader,
					leaderEpoch: p.LeaderEpoch,
				}
			}
		}
	}
	c.brokerMetadata = res.Brokers

	if len(ce) > 0 {
		return ce
	}
	return nil
}
func (c *ConsumerGroupContext) Close() error {
	c.broker.Close()
	c.session.closed = true
	for _, b := range c.brokerMetadata {
		b.Close()
	}
	return nil
}
