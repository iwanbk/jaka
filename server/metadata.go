package server

import (
	"io"

	log "github.com/sirupsen/logrus"
)

// handle MetadataRequest
func (s *Server) handleMetadataReq(w io.Writer, rd io.Reader) error {
	var (
		numTopic int32
		topics   []string
	)

	// read number of topic
	err := binRead(rd, &numTopic)
	if err != nil {
		return err
	}

	// read each topic
	for i := 0; i < int(numTopic); i++ {
		topic, err := readString(rd)
		if err != nil {
			return err
		}
		topics = append(topics, topic)
	}

	return s.responMetadata(w, topics)
}

// send MetadataResponse
func (s *Server) responMetadata(w io.Writer, topics []string) error {
	// construct brokers message
	err := s.sendBrokers(w)
	if err != nil {
		return err
	}

	// controller ID
	log.Println("controllerID")
	controllerID := int32(1)
	err = binWrite(w, controllerID)
	if err != nil {
		return err
	}

	// construct topicMetada
	return s.sendTopicMetadata(w, topics)
}

// TODO : send real brokers, not only our self
func (s *Server) sendBrokers(w io.Writer) error {
	host, port, err := parseHostPort(s.ListenAddr())
	if err != nil {
		return err
	}

	log.Println("send numBroker")
	// numBroker
	numBroker := int32(1)
	err = binWrite(w, numBroker)
	if err != nil {
		return err
	}

	// node id
	err = binWrite(w, s.NodeID)
	if err != nil {
		return err
	}

	// host
	err = writeString(w, host)
	if err != nil {
		return err
	}

	// port
	err = binWrite(w, port)
	if err != nil {
		return err
	}
	// rack : always null now
	return binWrite(w, int16(-1))
}

func (s *Server) sendTopicMetadata(w io.Writer, topics []string) error {
	// num of topic metadata
	numTopic := int32(len(topics))
	err := binWrite(w, numTopic)
	if err != nil {
		return err
	}

	// send the topics
	for i, topic := range topics {
		pms := []partitionMetadata{
			{
				fixed: partitionMetadataFixedFields{
					PartitionErrorCode: 0,
					PartitionID:        int32(i),
					Leader:             s.NodeID,
				},
				Replicas: nil,
				Isr:      nil,
			},
		}
		tm := topicMetadata{
			TopicErrorCode: 0,
			TopicName:      topic,
			IsInternal:     false,
			Metadata:       pms,
		}
		err = tm.Write(w)
		if err != nil {
			return err
		}
	}
	return nil
}
