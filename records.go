package kafkaClient

import "fmt"

const (
	unknownRecords = iota
	legacyRecords
	defaultRecords

	magicOffset = 16
)

type Records struct {
	recordsType int
	MsgSet      *MessageSet
	RecordBatch *RecordBatch
}

func (r *Records) getControlRecord() (ControlRecord, error) {
	if r.RecordBatch == nil || len(r.RecordBatch.Records) == 0 {
		return ControlRecord{}, fmt.Errorf("cannot get control record, record batch is empty")
	}

	firstRecord := r.RecordBatch.Records[0]
	controlRecord := ControlRecord{}
	err := controlRecord.decode(&realDecoder{raw: firstRecord.Key}, &realDecoder{raw: firstRecord.Value})
	if err != nil {
		return ControlRecord{}, err
	}

	return controlRecord, nil
}

func newLegacyRecords(msgSet *MessageSet) Records {
	return Records{recordsType: legacyRecords, MsgSet: msgSet}
}

func newDefaultRecords(batch *RecordBatch) Records {
	return Records{recordsType: defaultRecords, RecordBatch: batch}
}

// setTypeFromFields sets type of Records depending on which of MsgSet or RecordBatch is not nil.
// The first return value indicates whether both fields are nil (and the type is not set).
// If both fields are not nil, it returns an error.
func (r *Records) setTypeFromFields() (bool, error) {
	// if r.MsgSet == nil && r.RecordBatch == nil {
	// 	return true, nil
	// }
	// if r.MsgSet != nil && r.RecordBatch != nil {
	// 	return false, fmt.Errorf("both MsgSet and RecordBatch are set, but record type is unknown")
	// }
	r.recordsType = defaultRecords
	// if r.MsgSet != nil {
	// 	r.recordsType = legacyRecords
	// }
	return false, nil
}

func (r *Records) encode(pe packetEncoder) error {
	if r.recordsType == unknownRecords {
		if empty, err := r.setTypeFromFields(); err != nil || empty {
			return err
		}
	}

	if r.RecordBatch == nil {
		return nil
	}
	return r.RecordBatch.encode(pe)

}

func (r *Records) setTypeFromMagic(pd packetDecoder) error {
	magic, err := magicValue(pd)
	if err != nil {
		return err
	}

	r.recordsType = defaultRecords
	if magic < 2 {
		r.recordsType = legacyRecords
	}

	return nil
}

func (r *Records) decode(pd packetDecoder) error {
	if r.recordsType == unknownRecords {
		if err := r.setTypeFromMagic(pd); err != nil {
			return err
		}
	}

	r.RecordBatch = &RecordBatch{}
	return r.RecordBatch.decode(pd)
}

func (r *Records) numRecords() (int, error) {
	if r.recordsType == unknownRecords {
		if empty, err := r.setTypeFromFields(); err != nil || empty {
			return 0, err
		}
	}

	if r.RecordBatch == nil {
		return 0, nil
	}
	return len(r.RecordBatch.Records), nil

}

func (r *Records) isPartial() (bool, error) {
	if r.recordsType == unknownRecords {
		if empty, err := r.setTypeFromFields(); err != nil || empty {
			return false, err
		}
	}

	if r.RecordBatch == nil {
		return false, nil
	}
	return r.RecordBatch.PartialTrailingRecord, nil

}

func (r *Records) isControl() (bool, error) {
	if r.recordsType == unknownRecords {
		if empty, err := r.setTypeFromFields(); err != nil || empty {
			return false, err
		}
	}

	switch r.recordsType {
	case legacyRecords:
		return false, nil
	case defaultRecords:
		if r.RecordBatch == nil {
			return false, nil
		}
		return r.RecordBatch.Control, nil
	}
	return false, fmt.Errorf("unknown records type: %v", r.recordsType)
}

func (r *Records) isOverflow() (bool, error) {
	if r.recordsType == unknownRecords {
		if empty, err := r.setTypeFromFields(); err != nil || empty {
			return false, err
		}
	}

	return false, nil

}

func (r *Records) recordsOffset() (*int64, error) {
	switch r.recordsType {
	case unknownRecords:
		return nil, nil
	case legacyRecords:
		return nil, nil
	case defaultRecords:
		if r.RecordBatch == nil {
			return nil, nil
		}
		return &r.RecordBatch.FirstOffset, nil
	}
	return nil, fmt.Errorf("unknown records type: %v", r.recordsType)
}

func magicValue(pd packetDecoder) (int8, error) {
	return pd.peekInt8(magicOffset)
}
