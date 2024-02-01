package febe

import (
	"errors"
)

var (
	CopyDone = errors.New("copy done")
)

// CopyResponse represnets the response to the COPY IN, COPY OUT commands.
type CopyResponse struct {
	MessageHeader
	Format  byte
	NumCols int16

	// NOTE: format codes, today, will always be all 0 or all 1
	// respective to the  the Format field, so we can safely ignore them
	// and use the Format field instead.
}

// ReadCopyOut reads the response to the COPY OUT command.
//
// It discards the format codes for each column since they are always all 0
// or all 1 depending on the Format field (currently for PostgreSQL 16).
func (c *Conn) ReadCopyOut() (CopyResponse, error) {
	var zero CopyResponse
	if err := c.checkMessageHeader('H'); err != nil {
		return zero, err
	}
	format, err := c.c.ReadByte()
	if err != nil {
		return zero, err
	}
	numCols, err := c.c.ReadInt16()
	if err != nil {
		return zero, err
	}

	// discard the format codes for each column
	if err := c.c.Discard(int(numCols * 2)); err != nil {
		return zero, err
	}

	return CopyResponse{
		MessageHeader: c.h,
		Format:        format,
		NumCols:       numCols,
	}, nil
}

type CopyDataResponse struct {
	MessageHeader
	Data []byte
}

func (c *Conn) ReadCopyData() (CopyDataResponse, error) {
	var zero CopyDataResponse
	if err := c.checkMessageHeader('d'); err != nil {
		return zero, err
	}
	n := c.h.BodyLen()
	data, err := c.c.ReadByteN(int(n))
	if err != nil {
		return zero, err
	}
	return CopyDataResponse{
		MessageHeader: c.h,
		Data:          data,
	}, nil
}

type CopyDoneResponse struct {
	MessageHeader
}

func (c *Conn) ReadCopyDone() (CopyDoneResponse, error) {
	var zero CopyDoneResponse
	if err := c.checkMessageHeader('c'); err != nil {
		return zero, err
	}
	return CopyDoneResponse{MessageHeader: c.h}, nil
}
