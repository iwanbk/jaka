package server

import (
	"io"
	"log"
)

type produceRequestV2 struct {
	// The number of nodes that should replicate the produce before returning.
	// -1 indicates the full ISR.
	Acks int16

	// The time to await a response in ms.
	Timeout int32

	Topics []produceRequestV2Topic
}

type produceRequestV2Topic struct {
	Topic string
	Data  []produceRequestV2TopicData // read() guarantee us to have at least one data
}

type produceRequestV2TopicData struct {
	Partition int32
	RecordSet []recordSetItemV2 // read guaranted us to have at least one record set
}

type recordSetItemV2 struct {
	Offset     int64
	RecordSize int32
	Record     record
}

type record struct {
	CRC        int32
	MagicByte  int8
	Attributes int8
	Timestamp  int64
	Key        []byte
	Value      []byte
}

func (pr *produceRequestV2) read(rd io.Reader) error {
	// ack
	err := binRead(rd, &pr.Acks)
	if err != nil {
		return err
	}

	// timeout
	err = binRead(rd, &pr.Timeout)
	if err != nil {
		return err
	}

	// read topic_data
	topicDataLen, err := readArrayLen(rd)
	if err != nil {
		return err
	}
	pr.Topics = make([]produceRequestV2Topic, 0, int(topicDataLen))
	for i := 0; i < int(topicDataLen); i++ {
		var prt produceRequestV2Topic
		err = prt.read(rd)
		if err != nil {
			return err
		}
		pr.Topics = append(pr.Topics, prt)
	}
	return nil
}

func (pr *produceRequestV2Topic) read(rd io.Reader) error {
	var err error

	pr.Topic, err = readString(rd)
	if err != nil {
		return err
	}

	log.Printf("topic:%v", pr.Topic)

	dataLen, err := readArrayLen(rd)
	if err != nil {
		return err
	}

	pr.Data = make([]produceRequestV2TopicData, 0, int(dataLen))
	for i := 0; i < int(dataLen); i++ {
		var prtd produceRequestV2TopicData
		err = prtd.read(rd)
		if err != nil {
			return err
		}
		pr.Data = append(pr.Data, prtd)
	}
	return nil
}

func (pr *produceRequestV2TopicData) read(rd io.Reader) error {
	err := binRead(rd, &pr.Partition)
	if err != nil {
		return err
	}

	recordSetLen, err := readArrayLen(rd)
	if err != nil {
		return err
	}

	for i := 0; i < int(recordSetLen); i++ {
		var rsi recordSetItemV2
		err = rsi.read(rd)
		if err != nil {
			return err
		}
		pr.RecordSet = append(pr.RecordSet, rsi)
	}
	return nil
}

func (rsi *recordSetItemV2) read(rd io.Reader) error {
	err := binRead(rd, &rsi.Offset)
	if err != nil {
		return err
	}

	err = binRead(rd, &rsi.RecordSize)
	if err != nil {
		return err
	}

	return rsi.Record.read(rd)
}

func (r *record) read(rd io.Reader) error {
	var hdr struct {
		CRC        int32
		MagicByte  int8
		Attributes int8
		Timestamp  int64
	}
	err := binRead(rd, &hdr)
	if err != nil {
		return err
	}
	r.CRC = hdr.CRC
	r.MagicByte = hdr.MagicByte
	r.Attributes = hdr.Attributes
	r.Timestamp = hdr.Timestamp

	r.Key, err = readBytes(rd)
	if err != nil {
		return err
	}
	r.Value, err = readBytes(rd)

	log.Printf("key=%v, val=%v", string(r.Key), string(r.Value))
	return err
}
