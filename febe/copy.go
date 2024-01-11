package febe

import (
	"context"
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
func (c *Conn) ReadCopyOut(ctx context.Context) (CopyResponse, error) {
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

type CopyDataHeaderResponse struct {
	MessageHeader
}

func (c *Conn) ReadCopyDataBinaryHeader(ctx context.Context) (CopyDataHeaderResponse, error) {
	var zero CopyDataHeaderResponse
	if err := c.checkMessageHeader('d'); err != nil {
		return zero, err
	}

	// discard 15 byte PGCOPY preamble
	if err := c.c.Discard(15); err != nil {
		return zero, err
	}

	return CopyDataHeaderResponse{
		MessageHeader: c.h,
	}, nil
}

type CopyDataBinaryColumnsResponse struct {
	MessageHeader
	NumCols int16
}

func (c *Conn) ReadCopyDataBinaryColumns(ctx context.Context) (CopyDataBinaryColumnsResponse, error) {
	var zero CopyDataBinaryColumnsResponse
	if err := c.checkMessageHeader('d'); err != nil {
		return zero, err
	}
	numCols, err := c.c.ReadInt16()
	if err != nil {
		return zero, err
	}
	return CopyDataBinaryColumnsResponse{
		MessageHeader: c.h,
		NumCols:       numCols,
	}, nil
}

type CopyDataBinaryValueResponse struct {
	MessageHeader
	Data []byte // NULL if nil
}

func (c *Conn) ReadCopyDataBinaryValue(ctx context.Context) (CopyDataBinaryValueResponse, error) {
	var zero CopyDataBinaryValueResponse
	if err := c.checkMessageHeader('d'); err != nil {
		return zero, err
	}
	n, err := c.c.ReadInt32()
	if err != nil {
		return zero, err
	}
	if n == -1 {
		return CopyDataBinaryValueResponse{
			MessageHeader: c.h,
			Data:          nil,
		}, nil
	}
	data, err := c.c.ReadByteN(int(n))
	if err != nil {
		return zero, err
	}
	return CopyDataBinaryValueResponse{
		Data: data,
	}, nil
}
