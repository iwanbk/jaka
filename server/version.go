package server

import (
	"io"
)

func (s *Server) handleAPIVersionV0(w io.Writer) error {
	resp := apiVersionResponseV0{
		APIVersions: []apiVersionV0{
			{
				APIKey:     apiVersions,
				MinVersion: 0,
				MaxVersion: 0,
			},
			{
				APIKey:     metadataRequest,
				MinVersion: 1,
				MaxVersion: 1,
			},
			{
				APIKey:     produceRequest,
				MinVersion: 2,
				MaxVersion: 2,
			},
		},
	}
	return resp.Write(w)
}

type apiVersionResponseV0 struct {
	ErrorCode   int16
	APIVersions []apiVersionV0
}

type apiVersionV0 struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
}

func (av *apiVersionResponseV0) Write(w io.Writer) error {
	err := binWrite(w, av.ErrorCode)
	if err != nil {
		return err
	}
	err = writeArrayLen(w, len(av.APIVersions))
	if err != nil {
		return err
	}

	for _, v := range av.APIVersions {
		err = binWrite(w, v)
		if err != nil {
			return err
		}
	}
	return nil
}
