package goparquet

import (
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"unsafe"

	"github.com/fraugster/parquet-go/parquet"
	"github.com/pkg/errors"
)

type intType interface {
	~int32 | ~int64
}

type uintType interface {
	~uint32 | ~uint64
}

type plainDecoder[T intType] struct {
	r io.Reader
}

func (i *plainDecoder[T]) init(r io.Reader) error {
	i.r = r

	return nil
}

func (i *plainDecoder[T]) decodeValues(dst []interface{}) (int, error) {
	var d T
	for idx := range dst {
		if err := binary.Read(i.r, binary.LittleEndian, &d); err != nil {
			return idx, err
		}
		dst[idx] = d
	}

	return len(dst), nil
}

type plainEncoder[T intType] struct {
	w io.Writer
}

func (i *plainEncoder[T]) Close() error {
	return nil
}

func (i *plainEncoder[T]) init(w io.Writer) error {
	i.w = w

	return nil
}

func (i *plainEncoder[T]) encodeValues(values []interface{}) error {
	d := make([]T, len(values))
	for j := range values {
		d[j] = values[j].(T)
	}
	return binary.Write(i.w, binary.LittleEndian, d)
}

type deltaBPDecoder[T intType] struct {
	deltaBitPackDecoder[T]
}

func (d *deltaBPDecoder[T]) decodeValues(dst []interface{}) (int, error) {
	for i := range dst {
		u, err := d.next()
		if err != nil {
			return i, err
		}
		dst[i] = u
	}

	return len(dst), nil
}

type deltaBPEncoder[T intType] struct {
	deltaBitPackEncoder[T]
}

func (d *deltaBPEncoder[T]) encodeValues(values []interface{}) error {
	for i := range values {
		if err := d.addValue(values[i].(T)); err != nil {
			return err
		}
	}

	return nil
}

type intStore[T intType] struct {
	repTyp   parquet.FieldRepetitionType
	min, max T

	*ColumnParameters
}

func (is *intStore[T]) params() *ColumnParameters {
	if is.ColumnParameters == nil {
		panic("ColumnParameters is nil")
	}
	return is.ColumnParameters
}

func (*intStore[T]) sizeOf(v interface{}) int {
	return int(reflect.TypeOf(v).Size())
}

func (is *intStore[T]) parquetType() parquet.Type {
	var x T
	switch (interface{}(x)).(type) {
	case int32:
		return parquet.Type_INT32
	case int64:
		return parquet.Type_INT64
	default:
		panic(fmt.Sprintf("unexpected type %T", x))
	}
}

func (is *intStore[T]) repetitionType() parquet.FieldRepetitionType {
	return is.repTyp
}

func maxValue[T intType]() (v T) {
	return 1<<(unsafe.Sizeof(v)*8-1) - 1
}

func minValue[T intType]() (v T) {
	return -1 << (unsafe.Sizeof(v)*8 - 1)
}

func (is *intStore[T]) reset(rep parquet.FieldRepetitionType) {
	is.repTyp = rep
	is.min = maxValue[T]()
	is.max = minValue[T]()
}

func (is *intStore[T]) maxValue() []byte {
	if is.max == minValue[T]() {
		return nil
	}
	var x T
	ret := make([]byte, reflect.TypeOf(x).Size())
	switch (interface{}(x)).(type) {
	case int32:
		binary.LittleEndian.PutUint32(ret, uint32(is.max))
	case int64:
		binary.LittleEndian.PutUint64(ret, uint64(is.max))
	default:
		panic(fmt.Sprintf("unexpected type %T", x))
	}
	return ret
}

func (is *intStore[T]) minValue() []byte {
	if is.min == maxValue[T]() {
		return nil
	}
	var x T
	ret := make([]byte, reflect.TypeOf(x).Size())
	switch (interface{}(x)).(type) {
	case int32:
		binary.LittleEndian.PutUint32(ret, uint32(is.min))
	case int64:
		binary.LittleEndian.PutUint64(ret, uint64(is.min))
	default:
		panic(fmt.Sprintf("unexpected type %T", x))
	}
	return ret
}

func (is *intStore[T]) setMinMax(j T) {
	if j < is.min {
		is.min = j
	}
	if j > is.max {
		is.max = j
	}
}

func (is *intStore[T]) getValues(v interface{}) ([]interface{}, error) {
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

func (*intStore[T]) append(arrayIn interface{}, value interface{}) interface{} {
	if arrayIn == nil {
		arrayIn = make([]T, 0, 1)
	}
	return append(arrayIn.([]T), value.(T))
}
