package goparquet

import (
	"io"

	"github.com/fraugster/parquet-go/parquet"
	"github.com/pkg/errors"
)

type numberPlainDecoder[T numberType, I internalNumberType[T]] struct {
	r io.Reader
}

func (f *numberPlainDecoder[T, I]) init(r io.Reader) error {
	f.r = r

	return nil
}

func (f *numberPlainDecoder[T, I]) decodeValues(dst []interface{}) (int, error) {
	var x I
	return x.DecodeBinaryValues(f.r, dst)
}

type numberPlainEncoder[T numberType, I internalNumberType[T]] struct {
	w io.Writer
}

func (d *numberPlainEncoder[T, I]) Close() error {
	return nil
}

func (d *numberPlainEncoder[T, I]) init(w io.Writer) error {
	d.w = w

	return nil
}

func (d *numberPlainEncoder[T, I]) encodeValues(values []interface{}) error {
	var x I
	return x.EncodeBinaryValues(d.w, values)
}

type numberStore[T numberType, I internalNumberType[T]] struct {
	repTyp   parquet.FieldRepetitionType
	min, max T

	*ColumnParameters
}

func (f *numberStore[T, I]) params() *ColumnParameters {
	if f.ColumnParameters == nil {
		panic("ColumnParameters is nil")
	}
	return f.ColumnParameters
}

func (*numberStore[T, I]) sizeOf(v interface{}) int {
	var x I
	return x.Sizeof()
}

func (f *numberStore[T, I]) parquetType() parquet.Type {
	var x I
	return x.ParquetType()
}

func (f *numberStore[T, I]) repetitionType() parquet.FieldRepetitionType {
	return f.repTyp
}

func (f *numberStore[T, I]) reset(rep parquet.FieldRepetitionType) {
	var x I
	f.repTyp = rep
	f.min = x.MaxValue()
	f.max = x.MinValue()
}

func (f *numberStore[T, I]) maxValue() []byte {
	var x I
	if f.max == x.MinValue() {
		return nil
	}
	return x.ToBytes(f.max)
}

func (f *numberStore[T, I]) minValue() []byte {
	var x I
	if f.min == x.MaxValue() {
		return nil
	}
	return x.ToBytes(f.min)
}

func (f *numberStore[T, I]) setMinMax(j T) {
	if j < f.min {
		f.min = j
	}
	if j > f.max {
		f.max = j
	}
}

func (f *numberStore[T, I]) getValues(v interface{}) ([]interface{}, error) {
	var t T

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
		return nil, errors.Errorf("unsupported type for storing in %T column: %T => %+v", t, v, v)
	}

	return vals, nil
}

func (*numberStore[T, I]) append(arrayIn interface{}, value interface{}) interface{} {
	if arrayIn == nil {
		arrayIn = make([]T, 0, 1)
	}
	return append(arrayIn.([]T), value.(T))
}
