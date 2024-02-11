package kafkaClient

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

type consumerGroupSession struct {
	parent            *ConsumerGroupContext
	coordinatorBroker *Broker
	lock              sync.RWMutex
	closed            bool
	userData          []byte
	memberID          string
	groupInstanceId   *string
	offsetManager     *offsetManager
	fetchJob          fetchJob
	generationId      int32
	result            chan *ConsumerResult
}

type ConsumerResult struct {
	ConsumerPartitons []*ConsumerResultPartition
}

type ConsumerResultPartition struct {
	Topic      string
	Partition  int32
	Value, Key []byte
	Error      error
	broker     *Broker
	Offset     int64
	Timestamp  time.Time
}

type converterConsumerMessage []*ConsumerMessage

func (c converterConsumerMessage) convertToConsumerResult(broker *Broker) []*ConsumerResultPartition {
	var d []*ConsumerResultPartition
	for _, val := range c {
		d = append(d, &ConsumerResultPartition{
			Topic:     val.Topic,
			Partition: val.Partition,
			Value:     val.Value,
			Key:       val.Key,
			Error:     nil,
			broker:    broker,
			Offset:    val.Offset,
			Timestamp: val.Timestamp,
		})
	}

	return d
}

func newSession(c *ConsumerGroupContext) *consumerGroupSession {
	return &consumerGroupSession{
		parent: c,
		result: make(chan *ConsumerResult),
	}
}

func (c *consumerGroupSession) startSession(groupId string, topics []string) error {
	coordinator, err := c.coordinator(groupId)
	if err != nil {
		return err
	}
	c.coordinatorBroker = coordinator

	join, err := c.joinGroupRequest(groupId, c.coordinatorBroker, topics)
	if err != nil {
		_ = c.coordinatorBroker.Close()
		return err
	}

	switch join.Err {
	case ErrNoError:
		c.memberID = join.MemberId
	case ErrUnknownMemberId, ErrIllegalGeneration:
		// reset member ID and retry immediately
		c.memberID = ""
		return ErrUnknownMemberId
	case ErrNotCoordinatorForConsumer, ErrRebalanceInProgress, ErrOffsetsLoadInProgress:
		// retry after backoff
		return join.Err
	case ErrMemberIdRequired:
		c.memberID = join.MemberId
		return join.Err
	case ErrFencedInstancedId:
		if c.groupInstanceId != nil {
			Logger.Printf("JoinGroup failed: group instance id %s has been fenced\n", *c.groupInstanceId)
		}
		return join.Err
	default:
		return join.Err
	}
	c.generationId = join.GenerationId

	var strategy BalanceStrategy
	var ok bool
	if strategy = c.coordinatorBroker.conf.Consumer.Group.Rebalance.Strategy; strategy == nil {
		strategy, ok = c.findStrategy(join.GroupProtocol, c.coordinatorBroker.conf.Consumer.Group.Rebalance.GroupStrategies)
		if !ok {
			// this case shouldn't happen in practice, since the leader will choose the protocol
			// that all the members support
			return errors.New(fmt.Sprintf("unable to find selected strategy: %s", join.GroupProtocol))
		}
	}

	var plan BalanceStrategyPlan
	var members map[string]ConsumerGroupMemberMetadata
	if join.LeaderId == join.MemberId {
		members, err = join.GetMembers()
		if err != nil {
			return err
		}

		_, _, plan, err = c.balance(strategy, members)
		if err != nil {
			return err
		}
	}

	syncGroupResponse, err := c.syncGroupRequest(coordinator, members, plan, join.GenerationId, strategy, groupId)

	if err != nil {
		_ = coordinator.Close()
		return err
	}

	switch syncGroupResponse.Err {
	case ErrNoError:
	case ErrUnknownMemberId, ErrIllegalGeneration:
		// reset member ID and retry immediately
		c.memberID = ""
	case ErrNotCoordinatorForConsumer, ErrRebalanceInProgress, ErrOffsetsLoadInProgress:
		// retry after backoff
		return syncGroupResponse.Err
	case ErrFencedInstancedId:
		if c.groupInstanceId != nil {
			Logger.Printf("JoinGroup failed: group instance id %s has been fenced\n", *c.groupInstanceId)
		}
		return syncGroupResponse.Err
	default:
		return syncGroupResponse.Err
	}

	var claims map[string][]int32 // topic, partitions
	if len(syncGroupResponse.MemberAssignment) > 0 {
		members, err := syncGroupResponse.GetMemberAssignment()
		if err != nil {
			return err
		}
		claims = members.Topics

		// in the case of stateful balance strategies, hold on to the returned
		// assignment metadata, otherwise, reset the statically defined consumer
		// group metadata
		if members.UserData != nil {
			c.userData = members.UserData
		} else {
			c.userData = c.coordinatorBroker.conf.Consumer.Group.Member.UserData
		}

		for _, partitions := range claims {
			sort.Sort(int32Slice(partitions))
		}
	}

	return c.newFetcher(topics, groupId, join.GenerationId, claims)
}

func (c *consumerGroupSession) balance(strategy BalanceStrategy, members map[string]ConsumerGroupMemberMetadata) (map[string][]int32, []string, BalanceStrategyPlan, error) {
	topicPartitions := make(map[string][]int32)
	for _, meta := range members {
		for _, topic := range meta.Topics {
			topicPartitions[topic] = nil
		}
	}

	allSubscribedTopics := make([]string, 0, len(topicPartitions))
	for topic := range topicPartitions {
		allSubscribedTopics = append(allSubscribedTopics, topic)
	}

	// refresh metadata for all the subscribed topics in the consumer group
	// to avoid using stale metadata to assigning partitions
	res, err := c.coordinatorBroker.GetMetadata(NewMetadataRequest(V2_0_0_0, allSubscribedTopics))
	if err != nil {
		return nil, nil, nil, err
	}

	for topic := range topicPartitions {
		partitions, err := res.partitionsFromTopic(topic)
		if err != nil {
			return nil, nil, nil, err
		}
		topicPartitions[topic] = partitions
	}

	plan, err := strategy.Plan(members, topicPartitions)
	return topicPartitions, allSubscribedTopics, plan, err
}

func (c *consumerGroupSession) findStrategy(name string, groupStrategies []BalanceStrategy) (BalanceStrategy, bool) {
	for _, strategy := range groupStrategies {
		if strategy.Name() == name {
			return strategy, true
		}
	}
	return nil, false
}

func (c *consumerGroupSession) coordinator(groupId string) (*Broker, error) {
	coordinator := c.cacheCoordinator(groupId)
	if coordinator == nil {
		c.refreshCoordinator(groupId)
		coordinator = c.cacheCoordinator(groupId)
	}

	if coordinator == nil {
		return nil, ErrConsumerCoordinatorNotAvailable
	}
	c.parent.broker.Close()
	coordinator.Open(c.parent.broker.conf)
	return coordinator, nil
}

func (c *consumerGroupSession) cacheCoordinator(groupId string) *Broker {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.coordinatorBroker
}

func (c *consumerGroupSession) refreshCoordinator(groupId string) error {
	if c.closed {
		return ErrClosedClient
	}

	res, err := c.parent.broker.FindCoordinator(groupId, CoordinatorGroup)
	if err != nil {
		return err
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	c.registerCoordinator(res.Coordinator)
	return nil
}

func (c *consumerGroupSession) registerCoordinator(broker *Broker) {
	c.coordinatorBroker = broker
}

func (c *consumerGroupSession) joinGroupRequest(groupId string, coordinator *Broker, topics []string) (*JoinGroupResponse, error) {
	req := &JoinGroupRequest{
		GroupId:        groupId,
		MemberId:       c.memberID,
		SessionTimeout: int32(c.coordinatorBroker.conf.Consumer.Group.Session.Timeout / time.Millisecond),
		ProtocolType:   "consumer",
	}
	if c.coordinatorBroker.conf.Version.IsAtLeast(V0_10_1_0) {
		req.Version = 1
		req.RebalanceTimeout = int32(c.coordinatorBroker.conf.Consumer.Group.Rebalance.Timeout / time.Millisecond)
	}
	if c.coordinatorBroker.conf.Version.IsAtLeast(V0_11_0_0) {
		req.Version = 2
	}
	if c.coordinatorBroker.conf.Version.IsAtLeast(V0_11_0_0) {
		req.Version = 2
	}
	if c.coordinatorBroker.conf.Version.IsAtLeast(V2_0_0_0) {
		req.Version = 3
	}
	// from JoinGroupRequest v4 onwards (due to KIP-394) the client will actually
	// send two JoinGroupRequests, once with the empty member id, and then again
	// with the assigned id from the first response. This is handled via the
	// ErrMemberIdRequired case.
	if c.coordinatorBroker.conf.Version.IsAtLeast(V2_2_0_0) {
		req.Version = 4
	}
	if c.coordinatorBroker.conf.Version.IsAtLeast(V2_3_0_0) {
		req.Version = 5
		req.GroupInstanceId = nil
	}

	meta := &ConsumerGroupMemberMetadata{
		Topics:   topics,
		UserData: c.userData,
	}
	var strategy BalanceStrategy
	if strategy = c.coordinatorBroker.conf.Consumer.Group.Rebalance.Strategy; strategy != nil {
		if err := req.AddGroupProtocolMetadata(strategy.Name(), meta); err != nil {
			return nil, err
		}
	} else {
		for _, strategy = range c.coordinatorBroker.conf.Consumer.Group.Rebalance.GroupStrategies {
			if err := req.AddGroupProtocolMetadata(strategy.Name(), meta); err != nil {
				return nil, err
			}
		}
	}

	return coordinator.JoinGroup(req)
}

func (c *consumerGroupSession) syncGroupRequest(
	coordinator *Broker,
	members map[string]ConsumerGroupMemberMetadata,
	plan BalanceStrategyPlan,
	generationID int32,
	strategy BalanceStrategy,
	groupId string,
) (*SyncGroupResponse, error) {
	req := &SyncGroupRequest{
		GroupId:      groupId,
		MemberId:     c.memberID,
		GenerationId: generationID,
	}

	// Versions 1 and 2 are the same as version 0.
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

	for memberID, topics := range plan {
		assignment := &ConsumerGroupMemberAssignment{Topics: topics}
		userDataBytes, err := strategy.AssignmentData(memberID, topics, generationID)
		if err != nil {
			return nil, err
		}
		assignment.UserData = userDataBytes
		if err := req.AddGroupAssignmentMember(memberID, assignment); err != nil {
			return nil, err
		}
		delete(members, memberID)
	}
	// add empty assignments for any remaining members
	for memberID := range members {
		if err := req.AddGroupAssignmentMember(memberID, &ConsumerGroupMemberAssignment{}); err != nil {
			return nil, err
		}
	}

	return coordinator.SyncGroup(req)
}
