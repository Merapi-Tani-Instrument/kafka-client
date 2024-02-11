package kafkaClient

import (
	"fmt"
)

type encoder interface {
	encode(pe packetEncoder) error
}

type decoder interface {
	decode(pd packetDecoder) error
}

type versionedDecoder interface {
	decode(pd packetDecoder, version int16) error
}

func versionedDecode(buf []byte, in versionedDecoder, version int16) error {
	if buf == nil {
		return nil
	}

	helper := realDecoder{
		raw: buf,
	}
	err := in.decode(&helper, version)
	if err != nil {
		return err
	}

	if helper.off != len(buf) {
		return PacketDecodingError{
			Info: fmt.Sprintf("invalid length (off=%d, len=%d)", helper.off, len(buf)),
		}
	}

	return nil
}

func encode(e encoder) ([]byte, error) {
	if e == nil {
		return nil, nil
	}

	var prepEnc prepEncoder
	var realEnc realEncoder

	err := e.encode(&prepEnc)
	if err != nil {
		return nil, err
	}

	if prepEnc.length < 0 || prepEnc.length > int(MaxRequestSize) {
		return nil, PacketEncodingError{fmt.Sprintf("invalid request size (%d)", prepEnc.length)}
	}

	realEnc.raw = make([]byte, prepEnc.length)
	err = e.encode(&realEnc)
	if err != nil {
		return nil, err
	}

	return realEnc.raw, nil
}

func decode(buf []byte, in decoder) error {
	if buf == nil {
		return nil
	}

	helper := realDecoder{
		raw: buf,
	}
	err := in.decode(&helper)
	if err != nil {
		return err
	}

	if helper.off != len(buf) {
		return PacketDecodingError{"invalid length"}
	}

	return nil
}
