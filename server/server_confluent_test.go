package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/require"
)

func TestConfluentWrite(t *testing.T) {
	cfg := Config{
		ListenAddr: "127.0.0.1:9092",
		NodeID:     1,
	}

	// create server
	s, err := New(cfg)
	require.NoError(t, err)

	// test context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// start server
	go func() {
		defer cancel()
		s.Start(ctx)
		t.Logf("server finished")
	}()

	// create producer
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost"},
	)
	require.NoError(t, err)

	defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// Produce messages to topic (asynchronously)
	topic := "myTopic"
	for _, word := range []string{"Welcome"} {
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Value: []byte(word),
		}, nil)
	}

	// Wait for message deliveries before shutting down
	p.Flush(15 * 1000)

}
