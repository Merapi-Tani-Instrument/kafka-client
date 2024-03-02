package kafkaClient

import (
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"time"
)

const partitionConsumersBatchTimeout = time.Duration(100) * time.Millisecond

type topicLeaderAndReplica struct {
	leader      int32
	leaderEpoch int32
}

type offsetManager struct {
	partitionsOffsetManager map[string]map[int32]int64 //[topic][partition]offset
}

type fetchLoop struct {
	topic       string
	partition   int32
	broker      *Broker
	offset      int64
	leaderEpoch int32
}

type fetchJob []*fetchLoop
type fetchJobArray []*fetchLoop
type fetchGroup map[*Broker]fetchJobArray

func newTopicLeaderAndReplica() map[string]map[int32]*topicLeaderAndReplica {
	return make(map[string]map[int32]*topicLeaderAndReplica)
}

func (t *topicLeaderAndReplica) findBroker(brokers []*Broker, coordinator *Broker) *Broker {
	idx := slices.IndexFunc[[]*Broker, *Broker](brokers, func(b *Broker) bool {
		return b.id == t.leader
	})
	if idx < 0 {
		return nil
	}
	b := brokers[idx]
	if b.id == coordinator.id {
		return coordinator
	}
	return b
}

func (c *consumerGroupSession) newFetcher(topics []string, groupID string, generationID int32, claims map[string][]int32) error {
	go c.heartbeatLoop(groupID, c.memberID, generationID)

	if c.offsetManager == nil {
		c.offsetManager = &offsetManager{
			partitionsOffsetManager: make(map[string]map[int32]int64),
		}
	}
	c.offsetManager.clear()
	if err := c.offsetManager.requestInitialOffset(groupID, c.coordinatorBroker, claims); err != nil {
		Logger.Println("[Fetcher] Error initial Offset")
		c.closed = true
		return err
	}
	offsetManager, err := c.offsetManager.chooseStartingOffset(c.coordinatorBroker)
	if err != nil {
		c.closed = true
		Logger.Println("[Fetcher] Error initialize starting offset")
		return err
	}

	if len(c.fetchJob) > 0 {
		c.fetchJob = c.fetchJob[:0]
	}
	for topic, partitions := range offsetManager {
		for partition, offset := range partitions {
			if partitionLeaderAndReplica, ok := c.parent.topicLeaderAndReplica[topic]; ok {
				if tr, ok := partitionLeaderAndReplica[partition]; ok {
					broker := tr.findBroker(c.parent.brokerMetadata, c.coordinatorBroker)
					if broker == nil {
						c.closed = true
						Logger.Println("[Fetcher] Broker not found from partition ", partition, " with topic ", topic)
						return ErrBrokerNotAvailable
					} else {
						c.fetchJob = append(c.fetchJob, &fetchLoop{
							topic:       topic,
							partition:   partition,
							broker:      broker,
							offset:      offset,
							leaderEpoch: tr.leaderEpoch,
						})
					}
				} else {
					c.closed = true
					Logger.Println("[Fetcher] Invalid partition ", partition, " with topic ", topic)
					return ErrInvalidPartition
				}
			} else {
				c.closed = true
				Logger.Println("[Fetcher] No partition ", partition, " with topic ", topic)
				return ErrInvalidPartition
			}
		}
	}

	if len(c.fetchJob) > 0 {
		Logger.Println("[Fetcher] Start Job Fetcher in total ", len(c.fetchJob))
		c.fetchJob.startFetchJob(c)
	} else {
		c.closed = true
		return ErrNoJobs
	}

	return nil
}

func (c fetchJob) startFetchJob(cg *consumerGroupSession) {
	grouping := make(fetchGroup)
	for _, f := range c {
		grouping[f.broker] = append(grouping[f.broker], f)
	}

	for broker, fetchArray := range grouping {
		go fetchArray.subscribe(broker, cg)
	}
}

func (f fetchJobArray) subscribe(broker *Broker, c *consumerGroupSession) {
	_ = broker.Open(c.coordinatorBroker.conf)
	fetchSize := c.coordinatorBroker.conf.Consumer.Fetch.Default
	for !c.closed {
		resp, err := f.fetchMessage(broker, c, fetchSize)
		if err == io.EOF || err == io.ErrClosedPipe {
			c.closed = false
			break
		} else if err != nil {
			time.Sleep(partitionConsumersBatchTimeout)
			continue
		}
		data := &ConsumerResult{}
		for _, fetch := range f {
			block := resp.GetBlock(fetch.topic, fetch.partition)
			if block == nil {
				data.ConsumerPartitons = append(data.ConsumerPartitons, &ConsumerResultPartition{
					Topic:     fetch.topic,
					Partition: fetch.partition,
					Error:     ErrIncompleteResponse,
				})
				continue
			} else if !errors.Is(block.Err, ErrNoError) {
				data.ConsumerPartitons = append(data.ConsumerPartitons, &ConsumerResultPartition{
					Topic:     fetch.topic,
					Partition: fetch.partition,
					Error:     block.Err,
				})
				continue
			} else {
				nRecs, err := block.numRecords()
				if err != nil {
					data.ConsumerPartitons = append(data.ConsumerPartitons, &ConsumerResultPartition{
						Topic:     fetch.topic,
						Partition: fetch.partition,
						Error:     err,
					})
					continue
				} else {
					if nRecs == 0 {
						partialTrailingMessage, err := block.isPartial()
						if err != nil {
							data.ConsumerPartitons = append(data.ConsumerPartitons, &ConsumerResultPartition{
								Topic:     fetch.topic,
								Partition: fetch.partition,
								Error:     err,
							})
						} else if partialTrailingMessage {
							if c.coordinatorBroker.conf.Consumer.Fetch.Max > 0 && c.coordinatorBroker.conf.Consumer.Fetch.Max == fetchSize {
								// we can't ask for more data, we've hit the configured limit
								data.ConsumerPartitons = append(data.ConsumerPartitons, &ConsumerResultPartition{
									Topic:     fetch.topic,
									Partition: fetch.partition,
									Error:     ErrMessageSizeTooLarge,
								})
								fetch.offset++ // skip this one so we can keep processing future messages
							} else {
								fetchSize *= 2
								// check int32 overflow
								if fetchSize < 0 {
									fetchSize = math.MaxInt32
								}
								if c.coordinatorBroker.conf.Consumer.Fetch.Max > 0 && fetchSize > c.coordinatorBroker.conf.Consumer.Fetch.Max {
									fetchSize = c.coordinatorBroker.conf.Consumer.Fetch.Max
								}
							}
						} else if block.LastRecordsBatchOffset != nil && *block.LastRecordsBatchOffset < block.HighWaterMarkOffset {
							// check last record offset to avoid stuck if high watermark was not reached
							Logger.Printf("consumer/broker/%d received batch with zero records but high watermark was not reached, topic %s, partition %d, offset %d\n", broker.id, fetch.topic, fetch.partition, *block.LastRecordsBatchOffset)
							fetch.offset = *block.LastRecordsBatchOffset + 1
						}
						continue
					}
				}
			}
			fetchSize = c.coordinatorBroker.conf.Consumer.Fetch.Default

			abortedProducerIDs := make(map[int64]struct{}, len(block.AbortedTransactions))
			abortedTransactions := block.getAbortedTransactions()

			var messages []*ConsumerMessage
			for _, records := range block.RecordsSet {
				switch records.recordsType {
				case legacyRecords:
					messageSetMessages, err := parseMessages(records.MsgSet, fetch)
					if err != nil {
						data.ConsumerPartitons = append(data.ConsumerPartitons, &ConsumerResultPartition{
							Topic:     fetch.topic,
							Partition: fetch.partition,
							Error:     err,
						})
						continue
					}
					messages = append(messages, messageSetMessages...)
				case defaultRecords:
					// Consume remaining abortedTransaction up to last offset of current batch
					for _, txn := range abortedTransactions {
						if txn.FirstOffset > records.RecordBatch.LastOffset() {
							break
						}
						abortedProducerIDs[txn.ProducerID] = struct{}{}
						// Pop abortedTransactions so that we never add it again
						abortedTransactions = abortedTransactions[1:]
					}

					recordBatchMessages, err := parseRecords(records.RecordBatch, fetch)
					if err != nil {
						data.ConsumerPartitons = append(data.ConsumerPartitons, &ConsumerResultPartition{
							Topic:     fetch.topic,
							Partition: fetch.partition,
							Error:     err,
						})
						continue
					}

					// Parse and commit offset but do not expose messages that are:
					// - control records
					// - part of an aborted transaction when set to `ReadCommitted`

					// control record
					isControl, err := records.isControl()
					if err != nil {
						// I don't know why there is this continue in case of error to begin with
						// Safe bet is to ignore control messages if ReadUncommitted
						// and block on them in case of error and ReadCommitted
						// if child.conf.Consumer.IsolationLevel == ReadCommitted {
						// 	return nil, err
						// }
						data.ConsumerPartitons = append(data.ConsumerPartitons, &ConsumerResultPartition{
							Topic:     fetch.topic,
							Partition: fetch.partition,
							Error:     err,
						})
						continue
					}
					if isControl {
						controlRecord, err := records.getControlRecord()
						if err != nil {
							data.ConsumerPartitons = append(data.ConsumerPartitons, &ConsumerResultPartition{
								Topic:     fetch.topic,
								Partition: fetch.partition,
								Error:     err,
							})
							continue
						}

						if controlRecord.Type == ControlRecordAbort {
							delete(abortedProducerIDs, records.RecordBatch.ProducerID)
						}
						continue
					}

					// filter aborted transactions
					if c.coordinatorBroker.conf.Consumer.IsolationLevel == ReadCommitted {
						_, isAborted := abortedProducerIDs[records.RecordBatch.ProducerID]
						if records.RecordBatch.IsTransactional && isAborted {
							continue
						}
					}

					messages = append(messages, recordBatchMessages...)
				default:
					fmt.Printf("unknown records type: %v\n", records.recordsType)
				}
			}
			data.ConsumerPartitons = append(data.ConsumerPartitons, converterConsumerMessage(messages).convertToConsumerResult(broker)...)
		}
		if len(data.ConsumerPartitons) > 0 {
			c.result <- data
		}
		time.Sleep(partitionConsumersBatchTimeout)
	}
}

func parseRecords(batch *RecordBatch, fl *fetchLoop) ([]*ConsumerMessage, error) {
	messages := make([]*ConsumerMessage, 0, len(batch.Records))

	for _, rec := range batch.Records {
		offset := batch.FirstOffset + rec.OffsetDelta
		if offset < fl.offset {
			continue
		}
		timestamp := batch.FirstTimestamp.Add(rec.TimestampDelta)
		if batch.LogAppendTime {
			timestamp = batch.MaxTimestamp
		}
		messages = append(messages, &ConsumerMessage{
			Topic:     fl.topic,
			Partition: fl.partition,
			Key:       rec.Key,
			Value:     rec.Value,
			Offset:    offset,
			Timestamp: timestamp,
			Headers:   rec.Headers,
		})
		fl.offset = offset + 1
	}
	if len(messages) == 0 {
		fl.offset++
	}
	return messages, nil
}

func parseMessages(msgSet *MessageSet, fl *fetchLoop) ([]*ConsumerMessage, error) {
	var messages []*ConsumerMessage
	for _, msgBlock := range msgSet.Messages {
		for _, msg := range msgBlock.Messages() {
			offset := msg.Offset
			timestamp := msg.Msg.Timestamp
			if msg.Msg.Version >= 1 {
				baseOffset := msgBlock.Offset - msgBlock.Messages()[len(msgBlock.Messages())-1].Offset
				offset += baseOffset
				if msg.Msg.LogAppendTime {
					timestamp = msgBlock.Msg.Timestamp
				}
			}
			if offset < fl.offset {
				continue
			}
			messages = append(messages, &ConsumerMessage{
				Topic:          fl.topic,
				Partition:      fl.partition,
				Key:            msg.Msg.Key,
				Value:          msg.Msg.Value,
				Offset:         offset,
				Timestamp:      timestamp,
				BlockTimestamp: msgBlock.Msg.Timestamp,
			})
			fl.offset = offset + 1
		}
	}
	if len(messages) == 0 {
		fl.offset++
	}
	return messages, nil
}

func (f fetchJobArray) fetchMessage(broker *Broker, c *consumerGroupSession, fetchSize int32) (*FetchResponse, error) {
	request := &FetchRequest{
		MinBytes:    c.coordinatorBroker.conf.Consumer.Fetch.Min,
		MaxWaitTime: int32(c.coordinatorBroker.conf.Consumer.MaxWaitTime / time.Millisecond),
	}
	// Version 1 is the same as version 0.
	if c.coordinatorBroker.conf.Version.IsAtLeast(V0_9_0_0) {
		request.Version = 1
	}
	// Starting in Version 2, the requestor must be able to handle Kafka Log
	// Message format version 1.
	if c.coordinatorBroker.conf.Version.IsAtLeast(V0_10_0_0) {
		request.Version = 2
	}
	// Version 3 adds MaxBytes.  Starting in version 3, the partition ordering in
	// the request is now relevant.  Partitions will be processed in the order
	// they appear in the request.
	if c.coordinatorBroker.conf.Version.IsAtLeast(V0_10_1_0) {
		request.Version = 3
		request.MaxBytes = MaxResponseSize
	}
	// Version 4 adds IsolationLevel.  Starting in version 4, the reqestor must be
	// able to handle Kafka log message format version 2.
	// Version 5 adds LogStartOffset to indicate the earliest available offset of
	// partition data that can be consumed.
	if c.coordinatorBroker.conf.Version.IsAtLeast(V0_11_0_0) {
		request.Version = 5
		request.Isolation = c.coordinatorBroker.conf.Consumer.IsolationLevel
	}
	// Version 6 is the same as version 5.
	if c.coordinatorBroker.conf.Version.IsAtLeast(V1_0_0_0) {
		request.Version = 6
	}
	// Version 7 adds incremental fetch request support.
	if c.coordinatorBroker.conf.Version.IsAtLeast(V1_1_0_0) {
		request.Version = 7
		// We do not currently implement KIP-227 FetchSessions. Setting the id to 0
		// and the epoch to -1 tells the broker not to generate as session ID we're going
		// to just ignore anyway.
		request.SessionID = 0
		request.SessionEpoch = -1
	}
	// Version 8 is the same as version 7.
	if c.coordinatorBroker.conf.Version.IsAtLeast(V2_0_0_0) {
		request.Version = 8
	}
	// Version 9 adds CurrentLeaderEpoch, as described in KIP-320.
	// Version 10 indicates that we can use the ZStd compression algorithm, as
	// described in KIP-110.
	if c.coordinatorBroker.conf.Version.IsAtLeast(V2_1_0_0) {
		request.Version = 10
	}
	// Version 11 adds RackID for KIP-392 fetch from closest replica
	if c.coordinatorBroker.conf.Version.IsAtLeast(V2_3_0_0) {
		request.Version = 11
		request.RackID = c.coordinatorBroker.conf.RackID
	}

	for _, fetch := range f {
		request.AddBlock(fetch.topic, fetch.partition, fetch.offset, fetchSize, fetch.leaderEpoch)

	}

	// avoid to fetch when there is no block
	if len(request.blocks) == 0 {
		return nil, nil
	}

	return broker.Fetch(request)

}

func (o *offsetManager) chooseStartingOffset(coordiator *Broker) (map[string]map[int32]int64, error) { //[topic][partition]startingOffset
	offsetManager := make(map[string]map[int32]int64)
	req := &OffsetRequest{
		Version: 3,
	}
	for topic, partitions := range o.partitionsOffsetManager {
		for partitionID, offset := range partitions {
			req.AddBlock(topic, int32(partitionID), -2, 1)
			res, err := coordiator.GetAvailableOffsets(req)
			if err != nil {
				return nil, err
			}
			oldestOffset, err := res.getOffset(topic, partitionID)
			if err != nil {
				return nil, err
			}
			if _, ok := offsetManager[topic]; !ok {
				offsetManager[topic] = make(map[int32]int64)
			}
			if oldestOffset > offset {
				offsetManager[topic][partitionID] = oldestOffset
			} else {
				offsetManager[topic][partitionID] = offset
			}
		}
	}

	return offsetManager, nil
}

func (o *offsetManager) clear() {
	for t := range o.partitionsOffsetManager {
		delete(o.partitionsOffsetManager, t)
	}
}

func (o *offsetManager) requestInitialOffset(groupID string, coordinator *Broker, claims map[string][]int32) error {
	req := &OffsetFetchRequest{
		Version:       4,
		ConsumerGroup: groupID,
		partitions:    make(map[string][]int32),
	}
	for topic, partitions := range claims {
		for _, partition := range partitions {
			req.partitions[topic] = []int32{partition}
			offset, _, _, err := o.fetchInitialOffset(coordinator, req, topic, partition, coordinator.conf.Consumer.Retry.Max)
			if err != nil {
				return err
			}
			if _, ok := o.partitionsOffsetManager[topic]; !ok {
				o.partitionsOffsetManager[topic] = make(map[int32]int64)
			}
			o.partitionsOffsetManager[topic][partition] = offset
		}
		delete(req.partitions, topic)
	}

	return nil
}

func (o *offsetManager) fetchInitialOffset(coordinator *Broker, req *OffsetFetchRequest, topic string, partition int32, retries int) (int64, int32, string, error) {
	resp, err := coordinator.FetchOffset(req)
	if (err != nil && retries <= 0) || resp == nil {
		return 0, 0, "", err
	}
	block := resp.GetBlock(topic, partition)
	if block == nil {
		return 0, 0, "", ErrIncompleteResponse
	}
	switch block.Err {
	case ErrNoError:
		return block.Offset, block.LeaderEpoch, block.Metadata, nil
	case ErrNotCoordinatorForConsumer:
		return 0, 0, "", ErrNotCoordinatorForConsumer
	case ErrOffsetsLoadInProgress:
		if retries <= 0 {
			return 0, 0, "", block.Err
		}
		backoff := o.computeBackoff(coordinator)
		<-time.After(backoff)
		return o.fetchInitialOffset(coordinator, req, topic, partition, retries-1)
	default:
		return 0, 0, "", block.Err
	}
}

func (o *offsetManager) computeBackoff(coordinator *Broker) time.Duration {
	return coordinator.conf.Consumer.Retry.Backoff
}

func (c *consumerGroupSession) heartbeatLoop(groupID, memberID string, generationId int32) {
	pause := time.NewTicker(c.coordinatorBroker.conf.Consumer.Group.Heartbeat.Interval)
	defer pause.Stop()
	Logger.Println("[Heartbeat] Member id ", memberID)

	retries := c.coordinatorBroker.conf.Metadata.Retry.Max
	for !c.closed {
		resp, err := c.heartbeatRequest(groupID, memberID, generationId)
		if err != nil {
			if retries <= 0 {
				Logger.Printf("[Heartbeat] Close coordinator with err %v\n", err)
				_ = c.coordinatorBroker.Close()
				c.closed = true
				return
			}
			retries--
			continue
		}
		switch resp.Err {
		case ErrNoError:
			retries = c.coordinatorBroker.conf.Metadata.Retry.Max
		case ErrRebalanceInProgress:
			Logger.Printf("[Heartbeat] Rebalance")
			c.closed = true
			return
		case ErrUnknownMemberId, ErrIllegalGeneration:
			Logger.Printf("[Heartbeat] Unknow member id")
			return
		case ErrFencedInstancedId:
			if c.groupInstanceId != nil {
				Logger.Printf("[Heartbeat] JoinGroup failed: group instance id %s has been fenced\n", *c.groupInstanceId)
			}
			return
		default:
			Logger.Printf("[Heartbeat] unknow error ", resp.Err)
			c.closed = true
			return
		}

		<-pause.C
	}
	Logger.Println("[Heartbeat] Stop")
}

func (c *consumerGroupSession) heartbeatRequest(groupID, memberID string, generationID int32) (*HeartbeatResponse, error) {
	req := &HeartbeatRequest{
		GroupId:      groupID,
		MemberId:     memberID,
		GenerationId: generationID,
	}

	// Version 1 and version 2 are the same as version 0.
	if c.coordinatorBroker.conf.Version.IsAtLeast(V0_11_0_0) {
		req.Version = 1
	}
	if c.coordinatorBroker.conf.Version.IsAtLeast(V2_0_0_0) {
		req.Version = 2
	}
	// Starting from version 3, we add a new field called groupInstanceId to indicate member identity across restarts.
	if c.coordinatorBroker.conf.Version.IsAtLeast(V2_3_0_0) {
		req.Version = 3
		req.GroupInstanceId = c.groupInstanceId
	}

	return c.coordinatorBroker.Heartbeat(req)
}
