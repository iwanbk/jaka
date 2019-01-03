package server

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

func TestConnect(t *testing.T) {
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

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{s.ListenAddr()},
		Topic:    "topic-A",
		Balancer: &kafka.LeastBytes{},
		Logger:   log.New(os.Stderr, "writer", log.Lshortfile),
	})
	defer w.Close()

	err = w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("Key-A"),
			Value: []byte("Hello World!"),
		},
	)
	require.NoError(t, err)
}
