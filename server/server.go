package server

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"

	log "github.com/sirupsen/logrus"
)

type Config struct {
	ListenAddr string
	NodeID     int32
}

// Server represents a Jaka server
type Server struct {
	list   net.Listener
	NodeID int32
}

// New creates new Server from the given config
func New(conf Config) (*Server, error) {
	var (
		err  error
		list net.Listener
	)
	if conf.ListenAddr != "" {
		// listen for tcp requests on given address
		list, err = net.Listen("tcp", conf.ListenAddr)
	} else {
		// listen for tcp requests on localhost using any available port
		list, err = net.Listen("tcp", "127.0.0.1:0")
	}
	if err != nil {
		return nil, err
	}
	log.Infof("Started listening on address %s", list.Addr().String())

	return &Server{
		list:   list,
		NodeID: conf.NodeID,
	}, nil
}

// Start starts the server with the given context
func (s *Server) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			conn, err := s.list.Accept()
			if err != nil {
				log.Errorf("failed to accept conn: %v", err)
				// TODO : add a smarter handler than simply return here
				return
			}
			go s.handle(ctx, conn)
		}
	}
}

func (s *Server) ListenAddr() string {
	return s.list.Addr().String()
}

func (s *Server) handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	bw := bufio.NewWriter(conn)

	for {
		log.Printf("-- waiting req")
		// read request
		hdr, rd, err := s.readRequest(ctx, conn)
		if err != nil {
			log.Errorf("readRequest err: %v", err)
			return
		}

		// write correlation ID
		buf := new(bytes.Buffer)
		err = binWrite(buf, hdr.CorrelationID)
		if err != nil {
			return
		}

		// handle request
		switch hdr.APIKey {
		case metadataRequest:
			err = s.handleMetadataReq(buf, rd)
		case produceRequest:
			err = s.handleProduceRequestV2(buf, rd)
		default:
			err = fmt.Errorf("unhandled api key : %v", hdr.APIKey)
		}

		// return if request = nil
		if err != nil {
			log.Errorf("failed to handle req(%v): %v", hdr.APIKey, err)
			return
		}

		// send the response len
		respLen := int32(buf.Len())
		err = binWrite(bw, respLen)
		if err != nil {
			log.Errorf("send response size failed: %v", err)
			return
		}

		// send the message
		n, err := bw.Write(buf.Bytes())
		if err != nil {
			log.Errorf("send response data failed: %v", err)
			return
		}

		if n != buf.Len() {
			log.Errorf("failed to send all response data. expect:%v, sent:%v", buf.Len(), n)
			return // TODO : add some retry
		}

		// flush the response
		err = bw.Flush()
		if err != nil {
			log.Errorf("failed to flush repsponse: %v", err)
			return
		}
	}
}

type header struct {
	APIKey        int16
	APIVersion    int16
	CorrelationID int32
	ClientIDLen   int16
}

func (s *Server) readRequest(ctx context.Context, rd io.Reader) (header, *bytes.Buffer, error) {

	var (
		size     int32
		hdr      header
		clientID string
	)

	// read length
	err := binRead(rd, &size)
	if err != nil {
		return hdr, nil, err
	}

	var (
		buf = make([]byte, size)
	)

	const (
		headerSize = 2 /* apiKey */ + 2 /*apiVersion */ + 4 /*correlationID*/ + 2 /*clientID length */
	)

	_, err = io.ReadFull(rd, buf)
	if err != nil {
		return hdr, nil, err
	}

	bBuf := bytes.NewBuffer(buf)

	// hdr
	err = binRead(bBuf, &hdr)
	if err != nil {
		return hdr, nil, err
	}

	// clientID
	cidBuf := make([]byte, int(hdr.ClientIDLen))
	err = binRead(bBuf, &cidBuf)
	if err != nil {
		return hdr, nil, err
	}
	clientID = string(cidBuf)

	log.Printf("size: %d, apiKey:%v, apiVersion:%v, correlationID:%d,clientID:%s",
		size, hdr.APIKey, hdr.APIVersion, hdr.CorrelationID, clientID)

	return hdr, bBuf, nil
}

func parseHostPort(addr string) (string, int32, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(portStr)
	return host, int32(port), err
}
