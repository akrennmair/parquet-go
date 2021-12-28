package goparquet

import (
	"io"
	"unsafe"

	"github.com/fraugster/parquet-go/parquet"
	"github.com/pkg/errors"
)

type floatPlainDecoder[T floatType, I internalFloatType[T]] struct {
	r io.Reader
}

func (f *floatPlainDecoder[T, I]) init(r io.Reader) error {
	f.r = r

	return nil
}

func (f *floatPlainDecoder[T, I]) decodeValues(dst []interface{}) (int, error) {
	var x I
	return x.DecodeBinaryValues(f.r, dst)
}

type floatPlainEncoder[F floatType, T internalFloatType[F]] struct {
	w io.Writer
}

func (d *floatPlainEncoder[T, I]) Close() error {
	return nil
}

func (d *floatPlainEncoder[T, I]) init(w io.Writer) error {
	d.w = w

	return nil
}

func (d *floatPlainEncoder[T, I]) encodeValues(values []interface{}) error {
	var x I
	return x.EncodeBinaryValues(d.w, values)
}

type floatStore[F floatType, T internalFloatType[F]] struct {
	repTyp   parquet.FieldRepetitionType
	min, max F

	*ColumnParameters
}

func (f *floatStore[T, I]) params() *ColumnParameters {
	if f.ColumnParameters == nil {
		panic("ColumnParameters is nil")
	}
	return f.ColumnParameters
}

func (*floatStore[T, I]) sizeOf(v interface{}) int {
	var x T
	return int(unsafe.Sizeof(x))
}

func (f *floatStore[T, I]) parquetType() parquet.Type {
	var x I
	return x.ParquetType()
}

func (f *floatStore[T, I]) repetitionType() parquet.FieldRepetitionType {
	return f.repTyp
}

func (f *floatStore[T, I]) reset(rep parquet.FieldRepetitionType) {
	var x I
	f.repTyp = rep
	f.min = x.MaxValue()
	f.max = x.MinValue()
}

func (f *floatStore[T, I]) maxValue() []byte {
	var x I
	if f.max == x.MinValue() {
		return nil
	}
	return x.ToBytes(f.max)
}

func (f *floatStore[T, I]) minValue() []byte {
	var x I
	if f.min == x.MaxValue() {
		return nil
	}
	return x.ToBytes(f.min)
}

func (f *floatStore[T, I]) setMinMax(j T) {
	if j < f.min {
		f.min = j
	}
	if j > f.max {
		f.max = j
	}
}

func (f *floatStore[T, I]) getValues(v interface{}) ([]interface{}, error) {
	var vals []interface{}
	switch typed := v.(type) {
	case T:
		f.setMinMax(typed)
		vals = []interface{}{typed}
	case []T:
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

func (*floatStore[T, I]) append(arrayIn interface{}, value interface{}) interface{} {
	if arrayIn == nil {
		arrayIn = make([]T, 0, 1)
	}
	return append(arrayIn.([]T), value.(T))
}
