package febe

import (
	"errors"
)

var (
	CopyDone = errors.New("copy done")
)

type CopyFormat byte

var (
	CopyFormatText   CopyFormat = 0
	CopyFormatBinary CopyFormat = 1
)

func (c *CopyFormat) String() string {
	switch *c {
	case CopyFormatText:
		return "text"
	case CopyFormatBinary:
		return "binary"
	}
	return "unknown"
}

// CopyResponse represnets the response to the COPY IN, COPY OUT commands.
type CopyResponse struct {
	MessageHeader
	Format  CopyFormat
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
		Format:        CopyFormat(format),
		NumCols:       numCols,
	}, nil
}

func (c *Conn) DiscardBinarySignatureFlagsAndExt() error {
	if err := c.c.Discard(11 + 4); err != nil {
		// signature + flags field
		return err
	}
	n, err := c.c.ReadInt32() // length of the extension area
	if err != nil {
		return err
	}
	return c.c.Discard(int(n))
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
