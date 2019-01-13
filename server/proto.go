package server

import (
	"bytes"
	"encoding/binary"
	"io"
)

// api key const
const (
	produceRequest  = 0
	fetchReq        = 1
	offsetReq       = 2
	metadataRequest = 3
	offsetCommitReq = 8
	offsetFetchReq
	groupCoordinatorReq
	joinGroupReq
)

func binRead(r io.Reader, data interface{}) error {
	return binary.Read(r, binary.BigEndian, data)
}

func binWrite(w io.Writer, data interface{}) error {
	return binary.Write(w, binary.BigEndian, data)
}

/*
Variable Length Primitives

bytes, string - These types consist of a signed integer giving a length N followed by N bytes of content. A length of -1 indicates null. string uses an int16 for its size, and bytes uses an int32.
*/

func readBytes(rd io.Reader) ([]byte, error) {
	var len int32

	// read bytes len
	err := binRead(rd, &len)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, int(len))
	return buf, binRead(rd, &buf)
}

// read kafka string from the given reader
func readString(rd io.Reader) (string, error) {
	var len int16

	// read string len
	err := binRead(rd, &len)
	if err != nil {
		return "", err
	}

	// read the string
	buf := make([]byte, int(len))
	err = binRead(rd, &buf)

	return string(buf), err
}

// write kafka string to the given writer
func writeString(w io.Writer, s string) error {
	length := int16(len(s))
	err := binWrite(w, length)
	if err != nil {
		return err
	}

	// TODO : real write all
	_, err = io.Copy(w, bytes.NewBufferString(s))
	return err
}

/*
Arrays

This is a notation for handling repeated structures. These will always be encoded as an int32 size containing the length N followed by N repetitions of the structure which can itself be made up of other primitive types. In the BNF grammars below we will show an array of a structure foo as [foo].
*/

func readArrayLen(rd io.Reader) (int32, error) {
	var len int32
	err := binRead(rd, &len)
	return len, err
}
