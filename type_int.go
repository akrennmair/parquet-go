package goparquet

import (
	"io"
	"reflect"

	"github.com/fraugster/parquet-go/parquet"
	"github.com/pkg/errors"
)

type plainDecoder[T intType, I internalIntType[T]] struct {
	r io.Reader
}

func (i *plainDecoder[T, I]) init(r io.Reader) error {
	i.r = r

	return nil
}

func (i *plainDecoder[T, I]) decodeValues(dst []interface{}) (int, error) {
	var d I
	return d.DecodeBinaryValues(i.r, dst)
}

type plainEncoder[T intType, I internalIntType[T]] struct {
	w io.Writer
}

func (i *plainEncoder[T, I]) Close() error {
	return nil
}

func (i *plainEncoder[T, I]) init(w io.Writer) error {
	i.w = w

	return nil
}

func (i *plainEncoder[T, I]) encodeValues(values []interface{}) error {
	var d I
	return d.EncodeBinaryValues(i.w, values)
}

type deltaBPDecoder[T intType, I internalIntType[T]] struct {
	deltaBitPackDecoder[T, I]
}

func (d *deltaBPDecoder[T, I]) decodeValues(dst []interface{}) (int, error) {
	for i := range dst {
		u, err := d.next()
		if err != nil {
			return i, err
		}
		dst[i] = u
	}

	return len(dst), nil
}

type deltaBPEncoder[T intType, I internalIntType[T]] struct {
	deltaBitPackEncoder[T, I]
}

func (d *deltaBPEncoder[T, I]) encodeValues(values []interface{}) error {
	for i := range values {
		if err := d.addValue(values[i].(T)); err != nil {
			return err
		}
	}

	return nil
}

type intStore[T intType, I internalIntType[T]] struct {
	repTyp   parquet.FieldRepetitionType
	min, max T

	*ColumnParameters
}

func (is *intStore[T, I]) params() *ColumnParameters {
	if is.ColumnParameters == nil {
		panic("ColumnParameters is nil")
	}
	return is.ColumnParameters
}

func (*intStore[T, I]) sizeOf(v interface{}) int {
	return int(reflect.TypeOf(v).Size())
}

func (is *intStore[T, I]) parquetType() parquet.Type {
	var x I
	return x.ParquetType()
}

func (is *intStore[T, I]) repetitionType() parquet.FieldRepetitionType {
	return is.repTyp
}

func (is *intStore[T, I]) reset(rep parquet.FieldRepetitionType) {
	var x I
	is.repTyp = rep
	is.min = x.MaxValue()
	is.max = x.MinValue()
}

func (is *intStore[T, I]) maxValue() []byte {
	var x I
	if is.max == x.MinValue() {
		return nil
	}
	return x.ToBytes(is.max)
}

func (is *intStore[T, I]) minValue() []byte {
	var x I
	if is.min == x.MaxValue() {
		return nil
	}
	return x.ToBytes(is.min)
}

func (is *intStore[T, I]) setMinMax(j T) {
	if j < is.min {
		is.min = j
	}
	if j > is.max {
		is.max = j
	}
}

func (is *intStore[T, I]) getValues(v interface{}) ([]interface{}, error) {
	var vals []interface{}
	switch typed := v.(type) {
	case T:
		is.setMinMax(typed)
		vals = []interface{}{typed}
	case []T:
		if is.repTyp != parquet.FieldRepetitionType_REPEATED {
			return nil, errors.Errorf("the value is not repeated but it is an array")
		}
		vals = make([]interface{}, len(typed))
		for j := range typed {
			is.setMinMax(typed[j])
			vals[j] = typed[j]
		}
	default:
		return nil, errors.Errorf("unsupported type for storing in int32 column: %T => %+v", v, v)
	}

	return vals, nil
}

func (*intStore[T, I]) append(arrayIn interface{}, value interface{}) interface{} {
	if arrayIn == nil {
		arrayIn = make([]T, 0, 1)
	}
	return append(arrayIn.([]T), value.(T))
}
