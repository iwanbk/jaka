package server

import (
	"io"
	"time"

	"github.com/iwanbk/jaka/logstor"
)

func (s *Server) handleProduceRequestV2(dataW io.Writer, rd io.Reader) error {
	var pr produceRequestV2

	// read req
	err := pr.read(rd)
	if err != nil {
		return err
	}

	// handle
	return nil
}

func (prt *produceRequestV2Topic) toRecordBatch(producerID int64, producerEpoch int16) *logstor.RecordBatch {
	hdr := logstor.RecordBatchHeader{
		BaseOffset:           0, //?
		PartitionLeaderEpoch: int32(time.Now().Unix()),
		Magic:                logstor.MagicRecordBatch,
		Attributes:           0, //? what is timestamp tipe?
		LastOffsetDelta:      0, //? what is this
		FirstTimestamp:       prt.firstTimestamp(),
		MaxTimestamp:         prt.maxTimestamp(),
		ProducerID:           producerID,
		ProducerEpoch:        producerEpoch,
		// BatchLength will be set by logstor
		// Crc will be set by logstor
		//BaseSequence:         0, //will be set by logstor
	}

	var records []logstor.Record

	for _, prd := range prt.Data {
		for _, rs := range prd.RecordSet {
			rec := logstor.Record{
				//Length
				Attributes: 0, // TODO
				//TimestampDelta
				//OffsetDelta
				// KeyLength
				Key: rs.Record.Key,
				// ValueLen
				Value: rs.Record.Value,
			}
			records = append(records, rec)
		}
	}

	return &logstor.RecordBatch{
		Header:  hdr,
		Records: records,
	}
}

func (prt *produceRequestV2Topic) firstTimestamp() int64 {
	return prt.Data[0].RecordSet[0].Record.Timestamp
}

func (prt *produceRequestV2Topic) maxTimestamp() int64 {
	var (
		lastIDData      = len(prt.Data) - 1
		lastIDRecordSet = len(prt.Data[lastIDData].RecordSet) - 1
	)
	return prt.Data[lastIDData].RecordSet[lastIDRecordSet].Record.Timestamp
}
