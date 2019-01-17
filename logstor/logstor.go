package logstor

// RecordBatch  is the on-disk format of a RecordBatch.
type RecordBatch struct {
	Header  RecordBatchHeader
	Records []Record
}

// RecordBatchHeader is fixed fields of a RecordBatch
// It is not an official term in kafka.
type RecordBatchHeader struct {
	BaseOffset           int64
	BatchLength          int32
	PartitionLeaderEpoch int32
	Magic                int8 // (current magic value is 2)

	// CRC covers the data from the attributes to the end of the batch
	// (i.e. all the bytes that follow the CRC).
	// The CRC-32C (Castagnoli) polynomial is used for the computation.
	Crc             int32
	Attributes      int16
	LastOffsetDelta int32
	FirstTimestamp  int64
	MaxTimestamp    int64
	ProducerId      int64
	ProducerEpoch   int16
	BaseSequence    int32
}

type Record struct {
	Length         varint
	Attributes     int8
	TimestampDelta varint
	OffsetDelta    varint
	KeyLength      varint
	Key            []byte
	ValueLen       varint
	Value          []byte
	Headers        []RecordHeader
}

type RecordHeader struct {
	HeaderKeyLength   varint
	HeaderKey         string
	HeaderValueLength varint
	Value             []byte
}

// Kafka use the same varint encoding as Protobuf.
// More information on the latter can be found here(https://developers.google.com/protocol-buffers/docs/encoding#varints)
// The count of headers in a record is also encoded as a varint.
type varint int64

type ControlRecord struct {
	Version int16 // (current version is 0)
	Type    int16 // (0 indicates an abort marker, 1 indicates a commit)
}
