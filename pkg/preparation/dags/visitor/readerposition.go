package visitor

import (
	"io"
)

type ReaderPosition interface {
	io.Reader
	Offset() uint64
	// Stamp returns the current offset and updates the internal stamp to the
	// current offset.
	Stamp() uint64
}

// ReaderPositionFromReader creates a ReaderPosition from an io.Reader.
func ReaderPositionFromReader(r io.Reader) ReaderPosition {
	return &readerPosition{
		reader: r,
		offset: 0,
		stamp:  0,
	}
}

type readerPosition struct {
	reader io.Reader
	offset uint64
	stamp  uint64
}

func (frp *readerPosition) Read(p []byte) (n int, err error) {
	n, err = frp.reader.Read(p)
	if n > 0 {
		frp.offset += uint64(n)
	}
	return n, err
}

func (frp *readerPosition) Offset() uint64 {
	return frp.offset
}

func (frp *readerPosition) Stamp() uint64 {
	stamp := frp.stamp
	frp.stamp = frp.offset
	return stamp
}
