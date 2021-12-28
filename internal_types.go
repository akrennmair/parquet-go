package goparquet

import (
	"encoding/binary"
	"io"
	"math"

	"github.com/fraugster/parquet-go/parquet"
)

type internalFloat32 struct{}

func (f internalFloat32) MinValue() float32 {
	return -math.MaxFloat32
}

func (f internalFloat32) MaxValue() float32 {
	return math.MaxFloat32
}

func (f internalFloat32) ParquetType() parquet.Type {
	return parquet.Type_FLOAT
}

func (f internalFloat32) ToBytes(v float32) []byte {
	ret := make([]byte, 4)
	binary.LittleEndian.PutUint32(ret, math.Float32bits(float32(v)))
	return ret
}

func (f internalFloat32) EncodeBinaryValues(w io.Writer, values []interface{}) error {
	data := make([]uint32, len(values))
	for i := range values {
		data[i] = math.Float32bits(values[i].(float32))
	}

	return binary.Write(w, binary.LittleEndian, data)
}

func (f internalFloat32) DecodeBinaryValues(r io.Reader, dst []interface{}) (int, error) {
	var data uint32
	for i := range dst {
		if err := binary.Read(r, binary.LittleEndian, &data); err != nil {
			return i, err
		}
		dst[i] = math.Float32frombits(data)
	}
	return len(dst), nil
}

type internalFloat64 struct{}

func (f internalFloat64) MinValue() float64 {
	return -math.MaxFloat64
}

func (f internalFloat64) MaxValue() float64 {
	return math.MaxFloat64
}

func (f internalFloat64) ParquetType() parquet.Type {
	return parquet.Type_DOUBLE
}

func (f internalFloat64) ToBytes(v float64) []byte {
	ret := make([]byte, 8)
	binary.LittleEndian.PutUint64(ret, math.Float64bits(float64(v)))
	return ret
}

func (f internalFloat64) EncodeBinaryValues(w io.Writer, values []interface{}) error {
	data := make([]uint64, len(values))
	for i := range values {
		data[i] = math.Float64bits(values[i].(float64))
	}

	return binary.Write(w, binary.LittleEndian, data)
}

func (f internalFloat64) DecodeBinaryValues(r io.Reader, dst []interface{}) (int, error) {
	var data uint64
	for i := range dst {
		if err := binary.Read(r, binary.LittleEndian, &data); err != nil {
			return i, err
		}
		dst[i] = math.Float64frombits(data)
	}
	return len(dst), nil
}
