package febe

import (
	"errors"
	"io"
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
	return c.readCopy()
}

func (c *Conn) ReadCopyIn() (CopyResponse, error) {
	var zero CopyResponse
	if err := c.checkMessageHeader('G'); err != nil {
		return zero, err
	}
	return c.readCopy()
}

func (c *Conn) readCopy() (CopyResponse, error) {
	var zero CopyResponse
	format, err := c.c.ReadByte()
	if err != nil {
		return zero, err
	}
	numCols, err := c.c.ReadInt16()
	if err != nil {
		return zero, err
	}
	if err := c.c.Discard(int(numCols * 2)); err != nil {
		return zero, err
	}
	return CopyResponse{
		MessageHeader: c.h,
		Format:        CopyFormat(format),
		NumCols:       numCols,
	}, nil
}

const (
	BinarySignature = "PGCOPY\n\377\r\n\000"
	BinaryFlags     = "\x00\x00\x00\x00"
	BinaryExtLength = "\x00\x00\x00\x00"
)

func (c *Conn) WriteCopyDataRow(values [][]byte, includeHeader bool) error {
	var s sizer
	s.addInt32() // length
	if includeHeader {
		s.addBytes(len(BinarySignature))
		s.addBytes(len(BinaryFlags))
		s.addBytes(len(BinaryExtLength))
	}
	s.addInt16() // column count
	for _, v := range values {
		s.addInt32() // length
		s.addBytes(len(v))
	}

	c.c.WriteByte('d')
	c.c.WriteInt32(s.size())

	if includeHeader {
		io.WriteString(c.c, BinarySignature)
		io.WriteString(c.c, BinaryFlags)
		io.WriteString(c.c, BinaryExtLength)
	}

	if len(values) == 0 {
		// final row
		c.c.WriteInt16(-1)
		return c.Err()
	} else {
		c.c.WriteInt16(int16(len(values)))
		for _, v := range values {
			c.c.WriteInt32(int32(len(v)))
			c.c.Write(v)
		}
		return c.Err()
	}
}

func (c *Conn) DiscardBinarySignatureFlagsAndExt() error {
	if err := c.c.Discard(len(BinarySignature) + len(BinaryFlags)); err != nil {
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

func (c *Conn) WriteCopyDone() error {
	c.c.WriteByte('c')
	c.c.WriteInt32(4)
	return c.Err()
}

func (c *Conn) WriteCopyFail(msg string) error {
	var s sizer
	s.addInt32() // length
	s.addString(msg)
	c.c.WriteByte('f')
	c.c.WriteInt32(s.size())
	c.c.WriteString(msg)
	return c.Err()
}
