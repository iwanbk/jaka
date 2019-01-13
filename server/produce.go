package server

import (
	"io"
)

func (s *Server) handleProduceRequestV2(dataW io.Writer, rd io.Reader) error {
	var pr produceRequestV2

	// read req
	err := pr.read(rd)
	if err != nil {
		return err
	}

	// handle req

	return nil
}
