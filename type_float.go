package goparquet

import (
	"io"
	"unsafe"

	"github.com/pkg/errors"

	"github.com/fraugster/parquet-go/parquet"
)

type internalType[T floatType] interface {
	MaxValue() T
	MinValue() T
	ParquetType() parquet.Type
	ToBytes(v T) []byte
	EncodeBinaryValues(w io.Writer, values []interface{}) error
	DecodeBinaryValues(r io.Reader, dst []interface{}) (int, error)
	// TODO
}

type floatType interface {
	float32 | float64
}

type internalFloatType[T floatType] interface {
	internalType[T]
}

type floatPlainDecoder[F floatType, T internalFloatType[F]] struct {
	r io.Reader
}

func (f *floatPlainDecoder[F, T]) init(r io.Reader) error {
	f.r = r

	return nil
}

func (f *floatPlainDecoder[F, T]) decodeValues(dst []interface{}) (int, error) {
	var x T
	return x.DecodeBinaryValues(f.r, dst)
}

type floatPlainEncoder[F floatType, T internalFloatType[F]] struct {
	w io.Writer
}

func (d *floatPlainEncoder[F, T]) Close() error {
	return nil
}

func (d *floatPlainEncoder[F, T]) init(w io.Writer) error {
	d.w = w

	return nil
}

func (d *floatPlainEncoder[F, T]) encodeValues(values []interface{}) error {
	var x T
	return x.EncodeBinaryValues(d.w, values)
}

type floatStore[F floatType, T internalFloatType[F]] struct {
	repTyp   parquet.FieldRepetitionType
	min, max F

	*ColumnParameters
}

func (f *floatStore[F, T]) params() *ColumnParameters {
	if f.ColumnParameters == nil {
		panic("ColumnParameters is nil")
	}
	return f.ColumnParameters
}

func (*floatStore[F, T]) sizeOf(v interface{}) int {
	var x T
	return int(unsafe.Sizeof(x))
}

func (f *floatStore[F, T]) parquetType() parquet.Type {
	var x T
	return x.ParquetType()
}

func (f *floatStore[F, T]) repetitionType() parquet.FieldRepetitionType {
	return f.repTyp
}

func (f *floatStore[F, T]) reset(rep parquet.FieldRepetitionType) {
	var x T
	f.repTyp = rep
	f.min = x.MaxValue()
	f.max = x.MinValue()
}

func (f *floatStore[F, T]) maxValue() []byte {
	var x T
	if f.max == x.MinValue() {
		return nil
	}
	return x.ToBytes(f.max)
}

func (f *floatStore[F, T]) minValue() []byte {
	var x T
	if f.min == x.MaxValue() {
		return nil
	}
	return x.ToBytes(f.min)
}

func (f *floatStore[F, T]) setMinMax(j F) {
	if j < f.min {
		f.min = j
	}
	if j > f.max {
		f.max = j
	}
}

func (f *floatStore[F, T]) getValues(v interface{}) ([]interface{}, error) {
	var vals []interface{}
	switch typed := v.(type) {
	case F:
		f.setMinMax(typed)
		vals = []interface{}{typed}
	case []F:
		if f.repTyp != parquet.FieldRepetitionType_REPEATED {
			return nil, errors.Errorf("the value is not repeated but it is an array")
		}
		vals = make([]interface{}, len(typed))
		for j := range typed {
			f.setMinMax(typed[j])
			vals[j] = typed[j]
		}
	default:
		return nil, errors.Errorf("unsupported type for storing in float32 column: %T => %+v", v, v)
	}

	return vals, nil
}

func (*floatStore[F, T]) append(arrayIn interface{}, value interface{}) interface{} {
	if arrayIn == nil {
		arrayIn = make([]F, 0, 1)
	}
	return append(arrayIn.([]F), value.(F))
}
