package server

import (
	"io"
)

// topicMetadata version 1
type topicMetadata struct {
	TopicErrorCode int16
	TopicName      string
	IsInternal     bool
	Metadata       []partitionMetadata
}

type partitionMetadata struct {
	fixed    partitionMetadataFixedFields
	Replicas []int32
	Isr      []int32
}

type partitionMetadataFixedFields struct {
	PartitionErrorCode int16
	PartitionID        int32
	Leader             int32
}

func (tm *topicMetadata) Write(w io.Writer) error {

	// topic error code
	err := binWrite(w, tm.TopicErrorCode)
	if err != nil {
		return err
	}

	// topic name
	err = writeString(w, tm.TopicName)
	if err != nil {
		return err
	}

	err = binWrite(w, tm.IsInternal)
	if err != nil {
		return err
	}

	// Partition metadata
	partitionNum := int32(len(tm.Metadata))
	err = binWrite(w, partitionNum)
	if err != nil {
		return err
	}
	for _, pm := range tm.Metadata {
		err = pm.Write(w)
		if err != nil {
			return err
		}
	}
	return nil
}

func (pm *partitionMetadata) Write(w io.Writer) error {
	// fixed fields
	err := binWrite(w, pm.fixed)
	if err != nil {
		return err
	}

	//replicas
	lenReplicas := int32(len(pm.Replicas))
	err = binWrite(w, lenReplicas)
	if err != nil {
		return err
	}
	for _, rep := range pm.Replicas {
		err = binWrite(w, rep)
		if err != nil {
			return err
		}
	}

	// isr
	lenIsr := int32(len(pm.Isr))
	err = binWrite(w, lenIsr)
	if err != nil {
		return err
	}
	for _, v := range pm.Isr {
		err = binWrite(w, v)
		if err != nil {
			return err
		}
	}
	return nil
}
