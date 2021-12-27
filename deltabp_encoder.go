package goparquet

import (
	"bytes"
	"encoding/binary"
	"io"
	"math/bits"

	"github.com/pkg/errors"
)

type deltaBitPackEncoder[T intType] struct {
	deltas   []T
	bitWidth []uint8
	packed   [][]byte
	w        io.Writer

	// this value should be there before the init
	blockSize      int // Must be multiple of 128
	miniBlockCount int // blockSize % miniBlockCount should be 0

	miniBlockValueCount int

	valuesCount int
	buffer      *bytes.Buffer

	firstValue    T // the first value to write
	minDelta      T
	previousValue T
}

func (d *deltaBitPackEncoder[T]) init(w io.Writer) error {
	d.w = w

	if d.blockSize%128 != 0 || d.blockSize <= 0 {
		return errors.Errorf("invalid block size, it should be multiply of 128, it is %d", d.blockSize)
	}

	if d.miniBlockCount <= 0 || d.blockSize%d.miniBlockCount != 0 {
		return errors.Errorf("invalid mini block count, it is %d", d.miniBlockCount)
	}

	d.miniBlockValueCount = d.blockSize / d.miniBlockCount
	if d.miniBlockValueCount%8 != 0 {
		return errors.Errorf("invalid mini block count, the mini block value count should be multiply of 8, it is %d", d.miniBlockCount)
	}

	d.firstValue = 0
	d.valuesCount = 0
	d.minDelta = maxValue[T]()
	d.deltas = make([]T, 0, d.blockSize)
	d.previousValue = 0
	d.buffer = &bytes.Buffer{}
	d.bitWidth = make([]uint8, 0, d.miniBlockCount)
	return nil
}

func (d *deltaBitPackEncoder[T]) flush() error {
	// Technically, based on the spec after this step all values are positive, but NO, it's not. the problem is when
	// the min delta is small enough (lets say MinInt) and one of deltas are MaxInt, the the result of MaxInt-MinInt is
	// -1, get the idea, there is a lot of numbers here because of overflow can produce negative value
	for i := range d.deltas {
		d.deltas[i] -= d.minDelta
	}

	if err := writeVariant(d.buffer, int64(d.minDelta)); err != nil {
		return err
	}

	var x T

	d.bitWidth = d.bitWidth[:0] //reset the bitWidth buffer
	d.packed = d.packed[:0]
	for i := 0; i < len(d.deltas); i += d.miniBlockValueCount {
		end := i + d.miniBlockValueCount
		if end >= len(d.deltas) {
			end = len(d.deltas)
		}
		var bw int
		buf := make([][8]T, d.miniBlockValueCount/8)
		switch (interface{}(x)).(type) {
		case int32:
			// The cast to uint32 here, is the key. or the max not works at all
			max := uint32(d.deltas[i])
			for j := i; j < end; j++ {
				if max < uint32(d.deltas[j]) {
					max = uint32(d.deltas[j])
				}
				t := j - i
				buf[t/8][t%8] = d.deltas[j]
			}
			bw = bits.Len32(uint32(max))
		case int64:
			// The cast to uint64 here, is the key. or the max not works at all
			max := uint64(d.deltas[i])
			for j := i; j < end; j++ {
				if max < uint64(d.deltas[j]) {
					max = uint64(d.deltas[j])
				}
				t := j - i
				buf[t/8][t%8] = d.deltas[j]
			}
			bw = bits.Len64(uint64(max))
		}

		d.bitWidth = append(d.bitWidth, uint8(bw))
		data := make([]byte, 0, bw*len(buf))
		packerFunc := packer[T](bw)
		for j := range buf {
			data = append(data, packerFunc(buf[j])...)
		}
		d.packed = append(d.packed, data)
	}

	for len(d.bitWidth) < d.miniBlockCount {
		d.bitWidth = append(d.bitWidth, 0)
	}

	if err := binary.Write(d.buffer, binary.LittleEndian, d.bitWidth); err != nil {
		return err
	}

	for i := range d.packed {
		if err := writeFull(d.buffer, d.packed[i]); err != nil {
			return err
		}
	}
	d.minDelta = maxValue[T]()
	d.deltas = d.deltas[:0]

	return nil
}

func (d *deltaBitPackEncoder[T]) addValue(i T) error {
	d.valuesCount++
	if d.valuesCount == 1 {
		d.firstValue = i
		d.previousValue = i
		return nil
	}

	delta := i - d.previousValue
	d.previousValue = i
	d.deltas = append(d.deltas, delta)
	if delta < d.minDelta {
		d.minDelta = delta
	}

	if len(d.deltas) == d.blockSize {
		// flush
		return d.flush()
	}

	return nil
}

func (d *deltaBitPackEncoder[T]) write() error {
	if len(d.deltas) > 0 {
		if err := d.flush(); err != nil {
			return err
		}
	}

	if err := writeUVariant(d.w, uint64(d.blockSize)); err != nil {
		return err
	}

	if err := writeUVariant(d.w, uint64(d.miniBlockCount)); err != nil {
		return err
	}

	if err := writeUVariant(d.w, uint64(d.valuesCount)); err != nil {
		return err
	}

	if err := writeVariant(d.w, int64(d.firstValue)); err != nil {
		return err
	}

	return writeFull(d.w, d.buffer.Bytes())
}

func (d *deltaBitPackEncoder[T]) Close() error {
	return d.write()
}
