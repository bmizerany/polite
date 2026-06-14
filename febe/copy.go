package febe

import (
	"io"
)

// CopyFormat reports whether COPY data is encoded as text or binary.
type CopyFormat byte

const (
	// CopyFormatText indicates text COPY encoding.
	CopyFormatText CopyFormat = 0

	// CopyFormatBinary indicates binary COPY encoding.
	CopyFormatBinary CopyFormat = 1
)

// String returns c as "text", "binary", or "unknown".
func (c CopyFormat) String() string {
	switch c {
	case CopyFormatText:
		return "text"
	case CopyFormatBinary:
		return "binary"
	}
	return "unknown"
}

// CopyResponse represents a CopyInResponse or CopyOutResponse message.
type CopyResponse struct {
	// MessageHeader is the decoded message header.
	MessageHeader

	// Format reports the row format used by the COPY stream.
	Format CopyFormat

	// NumCols reports the number of columns in each COPY row.
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

// ReadCopyIn reads the response to the COPY IN command.
//
// It discards the format codes for each column since they are always all 0
// or all 1 depending on the Format field (currently for PostgreSQL 16).
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
	if numCols < 0 {
		return zero, errShortMessage
	}
	if err := c.c.Discard(int(numCols) * 2); err != nil {
		return zero, err
	}
	return CopyResponse{
		MessageHeader: c.h,
		Format:        CopyFormat(format),
		NumCols:       numCols,
	}, nil
}

const (
	binarySignature = "PGCOPY\n\377\r\n\000"
	binaryFlags     = "\x00\x00\x00\x00"
	binaryExtLength = "\x00\x00\x00\x00"
	binaryHeader    = binarySignature + binaryFlags + binaryExtLength
)

// WriteCopyDataHeader writes the header that begins a binary COPY FROM
// data stream. It must precede any rows.
func (c *Conn) WriteCopyDataHeader() error {
	c.c.WriteByte('d')
	c.c.WriteInt32(int32(4 + len(binaryHeader)))
	io.WriteString(c.c, binaryHeader)
	return c.Err()
}

// WriteCopyDataRow writes one row of values to a binary COPY FROM data
// stream. Each value is sent length-prefixed: a nil slice is SQL NULL,
// a non-nil empty slice is a zero-length value of the column's type,
// and an empty values slice writes a row with no columns.
func (c *Conn) WriteCopyDataRow(values [][]byte) error {
	size := 4 + 2 // length + column count
	for _, v := range values {
		size += 4 // length prefix
		size += len(v)
	}
	c.c.WriteByte('d')
	c.c.WriteInt32(int32(size))
	c.c.WriteInt16(int16(len(values)))
	for _, v := range values {
		if v == nil {
			c.c.WriteInt32(-1)
			continue
		}
		c.c.WriteInt32(int32(len(v)))
		c.c.Write(v)
	}
	return c.Err()
}

// WriteCopyDataTrailer writes the end-of-stream marker that terminates
// a binary COPY FROM data stream. It must follow the last row.
func (c *Conn) WriteCopyDataTrailer() error {
	c.c.WriteByte('d')
	c.c.WriteInt32(6) // 4 (length) + 2 (column count)
	c.c.WriteInt16(-1)
	return c.Err()
}

func (c *Conn) discardBinarySignatureFlagsAndExt() error {
	if err := c.c.Discard(len(binarySignature) + len(binaryFlags)); err != nil {
		return err
	}
	n, err := c.c.ReadInt32() // length of the extension area
	if err != nil {
		return err
	}
	if n < 0 {
		return errShortMessage
	}
	return c.c.Discard(int(n))
}

// CopyDoneResponse is the backend CopyDone message.
type CopyDoneResponse struct {
	// MessageHeader is the decoded message header.
	MessageHeader
}

// ReadCopyDone reads a CopyDone response.
func (c *Conn) ReadCopyDone() (CopyDoneResponse, error) {
	var zero CopyDoneResponse
	if err := c.checkMessageHeader('c'); err != nil {
		return zero, err
	}
	return CopyDoneResponse{MessageHeader: c.h}, nil
}

// WriteCopyDone writes a frontend CopyDone message.
func (c *Conn) WriteCopyDone() error {
	c.c.WriteByte('c')
	c.c.WriteInt32(4)
	return c.Err()
}

// WriteCopyFail writes a frontend CopyFail message with msg.
func (c *Conn) WriteCopyFail(msg string) error {
	var s sizer
	s.addInt32() // length
	s.addString(msg)
	c.c.WriteByte('f')
	c.c.WriteInt32(s.size())
	c.c.WriteString(msg)
	return c.Err()
}
