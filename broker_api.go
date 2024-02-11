package kafkaClient

func (b *Broker) GetMetadata(request *MetadataRequest) (*MetadataResponse, error) {
	response := new(MetadataResponse)
	response.Version = request.Version // Required to ensure use of the correct response header version
	err := b.sendAndReceive(request, response)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (b *Broker) Produce(request *ProduceRequest) (*ProduceResponse, error) {
	var (
		response *ProduceResponse
		err      error
	)

	if request.RequiredAcks == NoResponse {
		err = b.sendAndReceive(request, nil)
	} else {
		response = new(ProduceResponse)
		err = b.sendAndReceive(request, response)
	}

	if err != nil {
		return nil, err
	}

	return response, nil
}

func (b *Broker) FindCoordinator(coordinatorKey string, coordinatorType CoordinatorType) (*FindCoordinatorResponse, error) {
	response := new(FindCoordinatorResponse)

	err := b.sendAndReceive(&FindCoordinatorRequest{
		Version:         2,
		CoordinatorKey:  coordinatorKey,
		CoordinatorType: coordinatorType,
	}, response)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (b *Broker) JoinGroup(request *JoinGroupRequest) (*JoinGroupResponse, error) {
	response := new(JoinGroupResponse)

	err := b.sendAndReceive(request, response)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (b *Broker) SyncGroup(request *SyncGroupRequest) (*SyncGroupResponse, error) {
	response := new(SyncGroupResponse)

	err := b.sendAndReceive(request, response)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (b *Broker) FetchOffset(request *OffsetFetchRequest) (*OffsetFetchResponse, error) {
	response := new(OffsetFetchResponse)
	response.Version = request.Version // needed to handle the two header versions

	err := b.sendAndReceive(request, response)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (b *Broker) Heartbeat(request *HeartbeatRequest) (*HeartbeatResponse, error) {
	response := new(HeartbeatResponse)

	err := b.sendAndReceive(request, response)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (b *Broker) GetAvailableOffsets(request *OffsetRequest) (*OffsetResponse, error) {
	response := new(OffsetResponse)

	err := b.sendAndReceive(request, response)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (b *Broker) Fetch(request *FetchRequest) (*FetchResponse, error) {
	response := new(FetchResponse)
	err := b.sendAndReceive(request, response)
	if err != nil {
		return nil, err
	}

	return response, nil
}

func (b *Broker) CommitOffset(request *OffsetCommitRequest) (*OffsetCommitResponse, error) {
	response := new(OffsetCommitResponse)

	err := b.sendAndReceive(request, response)
	if err != nil {
		return nil, err
	}

	return response, nil
}
