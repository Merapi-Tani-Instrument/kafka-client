package kafkaClient

type packetEncoder interface {
	// Primitives
	putInt8(in int8)
	putInt16(in int16)
	putInt32(in int32)
	putInt64(in int64)
	putVarint(in int64)
	putUVarint(in uint64)
	putFloat64(in float64)
	putCompactArrayLength(in int)
	putArrayLength(in int) error
	putBool(in bool)

	// Collections
	putBytes(in []byte) error
	putVarintBytes(in []byte) error
	putCompactBytes(in []byte) error
	putRawBytes(in []byte) error
	putCompactString(in string) error
	putNullableCompactString(in *string) error
	putString(in string) error
	putNullableString(in *string) error
	putStringArray(in []string) error
	putCompactInt32Array(in []int32) error
	putNullableCompactInt32Array(in []int32) error
	putInt32Array(in []int32) error
	putInt64Array(in []int64) error
	putEmptyTaggedFieldArray()

	// Provide the current offset to record the batch size metric
	offset() int

	// Stacks, see PushEncoder
	push(in pushEncoder)
	pop() error
}

// PushEncoder is the interface for encoding fields like CRCs and lengths where the value
// of the field depends on what is encoded after it in the packet. Start them with PacketEncoder.Push() where
// the actual value is located in the packet, then PacketEncoder.Pop() them when all the bytes they
// depend upon have been written.
type pushEncoder interface {
	// Saves the offset into the input buffer as the location to actually write the calculated value when able.
	saveOffset(in int)

	// Returns the length of data to reserve for the output of this encoder (eg 4 bytes for a CRC32).
	reserveLength() int

	// Indicates that all required data is now available to calculate and write the field.
	// SaveOffset is guaranteed to have been called first. The implementation should write ReserveLength() bytes
	// of data to the saved offset, based on the data between the saved offset and curOffset.
	run(curOffset int, buf []byte) error
}

type dynamicPushEncoder interface {
	pushEncoder

	// Called during pop() to adjust the length of the field.
	// It should return the difference in bytes between the last computed length and current length.
	adjustLength(currOffset int) int
}
