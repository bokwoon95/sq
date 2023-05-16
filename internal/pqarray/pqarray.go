// Copyright (c) 2011-2013, 'pq' Contributors Portions Copyright (C) 2011 Blake Mizerany
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

package pqarray

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

var typeByteSlice = reflect.TypeOf([]byte{})
var typeDriverValuer = reflect.TypeOf((*driver.Valuer)(nil)).Elem()
var typeSQLScanner = reflect.TypeOf((*sql.Scanner)(nil)).Elem()

// Array returns the optimal driver.Valuer and sql.Scanner for an array or
// slice of any dimension.
//
// For example:
//
//	db.Query(`SELECT * FROM t WHERE id = ANY($1)`, pq.Array([]int{235, 401}))
//
//	var x []sql.NullInt64
//	db.QueryRow(`SELECT ARRAY[235, 401]`).Scan(pq.Array(&x))
//
// Scanning multi-dimensional arrays is not supported.  Arrays where the lower
// bound is not one (such as `[0:0]={1}') are not supported.
func Array(a interface{}) interface {
	driver.Valuer
	sql.Scanner
} {
	switch a := a.(type) {
	case []bool:
		return (*BoolArray)(&a)
	case []float64:
		return (*Float64Array)(&a)
	case []float32:
		return (*Float32Array)(&a)
	case []int64:
		return (*Int64Array)(&a)
	case []int32:
		return (*Int32Array)(&a)
	case []string:
		return (*StringArray)(&a)
	case [][]byte:
		return (*ByteaArray)(&a)

	case *[]bool:
		return (*BoolArray)(a)
	case *[]float64:
		return (*Float64Array)(a)
	case *[]float32:
		return (*Float32Array)(a)
	case *[]int64:
		return (*Int64Array)(a)
	case *[]int32:
		return (*Int32Array)(a)
	case *[]string:
		return (*StringArray)(a)
	case *[][]byte:
		return (*ByteaArray)(a)
	}

	return GenericArray{a}
}

// ArrayDelimiter may be optionally implemented by driver.Valuer or sql.Scanner
// to override the array delimiter used by GenericArray.
type ArrayDelimiter interface {
	// ArrayDelimiter returns the delimiter character(s) for this element's type.
	ArrayDelimiter() string
}

// BoolArray represents a one-dimensional array of the PostgreSQL boolean type.
type BoolArray []bool

// Scan implements the sql.Scanner interface.
func (a *BoolArray) Scan(src interface{}) error {
	switch src := src.(type) {
	case []byte:
		return a.scanBytes(src)
	case string:
		return a.scanBytes([]byte(src))
	case nil:
		*a = nil
		return nil
	}

	return fmt.Errorf("pq: cannot convert %T to BoolArray", src)
}

func (a *BoolArray) scanBytes(src []byte) error {
	elems, err := scanLinearArray(src, []byte{','}, "BoolArray")
	if err != nil {
		return err
	}
	if *a != nil && len(elems) == 0 {
		*a = (*a)[:0]
	} else {
		b := make(BoolArray, len(elems))
		for i, v := range elems {
			if len(v) != 1 {
				return fmt.Errorf("pq: could not parse boolean array index %d: invalid boolean %q", i, v)
			}
			switch v[0] {
			case 't':
				b[i] = true
			case 'f':
				b[i] = false
			default:
				return fmt.Errorf("pq: could not parse boolean array index %d: invalid boolean %q", i, v)
			}
		}
		*a = b
	}
	return nil
}

// Value implements the driver.Valuer interface.
func (a BoolArray) Value() (driver.Value, error) {
	if a == nil {
		return nil, nil
	}

	if n := len(a); n > 0 {
		// There will be exactly two curly brackets, N bytes of values,
		// and N-1 bytes of delimiters.
		b := make([]byte, 1+2*n)

		for i := 0; i < n; i++ {
			b[2*i] = ','
			if a[i] {
				b[1+2*i] = 't'
			} else {
				b[1+2*i] = 'f'
			}
		}

		b[0] = '{'
		b[2*n] = '}'

		return string(b), nil
	}

	return "{}", nil
}

// ByteaArray represents a one-dimensional array of the PostgreSQL bytea type.
type ByteaArray [][]byte

// Scan implements the sql.Scanner interface.
func (a *ByteaArray) Scan(src interface{}) error {
	switch src := src.(type) {
	case []byte:
		return a.scanBytes(src)
	case string:
		return a.scanBytes([]byte(src))
	case nil:
		*a = nil
		return nil
	}

	return fmt.Errorf("pq: cannot convert %T to ByteaArray", src)
}

func (a *ByteaArray) scanBytes(src []byte) error {
	elems, err := scanLinearArray(src, []byte{','}, "ByteaArray")
	if err != nil {
		return err
	}
	if *a != nil && len(elems) == 0 {
		*a = (*a)[:0]
	} else {
		b := make(ByteaArray, len(elems))
		for i, v := range elems {
			b[i], err = parseBytea(v)
			if err != nil {
				return fmt.Errorf("could not parse bytea array index %d: %s", i, err.Error())
			}
		}
		*a = b
	}
	return nil
}

// Value implements the driver.Valuer interface. It uses the "hex" format which
// is only supported on PostgreSQL 9.0 or newer.
func (a ByteaArray) Value() (driver.Value, error) {
	if a == nil {
		return nil, nil
	}

	if n := len(a); n > 0 {
		// There will be at least two curly brackets, 2*N bytes of quotes,
		// 3*N bytes of hex formatting, and N-1 bytes of delimiters.
		size := 1 + 6*n
		for _, x := range a {
			size += hex.EncodedLen(len(x))
		}

		b := make([]byte, size)

		for i, s := 0, b; i < n; i++ {
			o := copy(s, `,"\\x`)
			o += hex.Encode(s[o:], a[i])
			s[o] = '"'
			s = s[o+1:]
		}

		b[0] = '{'
		b[size-1] = '}'

		return string(b), nil
	}

	return "{}", nil
}

// Float64Array represents a one-dimensional array of the PostgreSQL double
// precision type.
type Float64Array []float64

// Scan implements the sql.Scanner interface.
func (a *Float64Array) Scan(src interface{}) error {
	switch src := src.(type) {
	case []byte:
		return a.scanBytes(src)
	case string:
		return a.scanBytes([]byte(src))
	case nil:
		*a = nil
		return nil
	}

	return fmt.Errorf("pq: cannot convert %T to Float64Array", src)
}

func (a *Float64Array) scanBytes(src []byte) error {
	elems, err := scanLinearArray(src, []byte{','}, "Float64Array")
	if err != nil {
		return err
	}
	if *a != nil && len(elems) == 0 {
		*a = (*a)[:0]
	} else {
		b := make(Float64Array, len(elems))
		for i, v := range elems {
			if b[i], err = strconv.ParseFloat(string(v), 64); err != nil {
				return fmt.Errorf("pq: parsing array element index %d: %v", i, err)
			}
		}
		*a = b
	}
	return nil
}

// Value implements the driver.Valuer interface.
func (a Float64Array) Value() (driver.Value, error) {
	if a == nil {
		return nil, nil
	}

	if n := len(a); n > 0 {
		// There will be at least two curly brackets, N bytes of values,
		// and N-1 bytes of delimiters.
		b := make([]byte, 1, 1+2*n)
		b[0] = '{'

		b = strconv.AppendFloat(b, a[0], 'f', -1, 64)
		for i := 1; i < n; i++ {
			b = append(b, ',')
			b = strconv.AppendFloat(b, a[i], 'f', -1, 64)
		}

		return string(append(b, '}')), nil
	}

	return "{}", nil
}

// Float32Array represents a one-dimensional array of the PostgreSQL double
// precision type.
type Float32Array []float32

// Scan implements the sql.Scanner interface.
func (a *Float32Array) Scan(src interface{}) error {
	switch src := src.(type) {
	case []byte:
		return a.scanBytes(src)
	case string:
		return a.scanBytes([]byte(src))
	case nil:
		*a = nil
		return nil
	}

	return fmt.Errorf("pq: cannot convert %T to Float32Array", src)
}

func (a *Float32Array) scanBytes(src []byte) error {
	elems, err := scanLinearArray(src, []byte{','}, "Float32Array")
	if err != nil {
		return err
	}
	if *a != nil && len(elems) == 0 {
		*a = (*a)[:0]
	} else {
		b := make(Float32Array, len(elems))
		for i, v := range elems {
			var x float64
			if x, err = strconv.ParseFloat(string(v), 32); err != nil {
				return fmt.Errorf("pq: parsing array element index %d: %v", i, err)
			}
			b[i] = float32(x)
		}
		*a = b
	}
	return nil
}

// Value implements the driver.Valuer interface.
func (a Float32Array) Value() (driver.Value, error) {
	if a == nil {
		return nil, nil
	}

	if n := len(a); n > 0 {
		// There will be at least two curly brackets, N bytes of values,
		// and N-1 bytes of delimiters.
		b := make([]byte, 1, 1+2*n)
		b[0] = '{'

		b = strconv.AppendFloat(b, float64(a[0]), 'f', -1, 32)
		for i := 1; i < n; i++ {
			b = append(b, ',')
			b = strconv.AppendFloat(b, float64(a[i]), 'f', -1, 32)
		}

		return string(append(b, '}')), nil
	}

	return "{}", nil
}

// GenericArray implements the driver.Valuer and sql.Scanner interfaces for
// an array or slice of any dimension.
type GenericArray struct{ A interface{} }

func (GenericArray) evaluateDestination(rt reflect.Type) (reflect.Type, func([]byte, reflect.Value) error, string) {
	var assign func([]byte, reflect.Value) error
	var del = ","

	// TODO calculate the assign function for other types
	// TODO repeat this section on the element type of arrays or slices (multidimensional)
	{
		if reflect.PtrTo(rt).Implements(typeSQLScanner) {
			// dest is always addressable because it is an element of a slice.
			assign = func(src []byte, dest reflect.Value) (err error) {
				ss := dest.Addr().Interface().(sql.Scanner)
				if src == nil {
					err = ss.Scan(nil)
				} else {
					err = ss.Scan(src)
				}
				return
			}
			goto FoundType
		}

		assign = func([]byte, reflect.Value) error {
			return fmt.Errorf("pq: scanning to %s is not implemented; only sql.Scanner", rt)
		}
	}

FoundType:

	if ad, ok := reflect.Zero(rt).Interface().(ArrayDelimiter); ok {
		del = ad.ArrayDelimiter()
	}

	return rt, assign, del
}

// Scan implements the sql.Scanner interface.
func (a GenericArray) Scan(src interface{}) error {
	dpv := reflect.ValueOf(a.A)
	switch {
	case dpv.Kind() != reflect.Ptr:
		return fmt.Errorf("pq: destination %T is not a pointer to array or slice", a.A)
	case dpv.IsNil():
		return fmt.Errorf("pq: destination %T is nil", a.A)
	}

	dv := dpv.Elem()
	switch dv.Kind() {
	case reflect.Slice:
	case reflect.Array:
	default:
		return fmt.Errorf("pq: destination %T is not a pointer to array or slice", a.A)
	}

	switch src := src.(type) {
	case []byte:
		return a.scanBytes(src, dv)
	case string:
		return a.scanBytes([]byte(src), dv)
	case nil:
		if dv.Kind() == reflect.Slice {
			dv.Set(reflect.Zero(dv.Type()))
			return nil
		}
	}

	return fmt.Errorf("pq: cannot convert %T to %s", src, dv.Type())
}

func (a GenericArray) scanBytes(src []byte, dv reflect.Value) error {
	dtype, assign, del := a.evaluateDestination(dv.Type().Elem())
	dims, elems, err := parseArray(src, []byte(del))
	if err != nil {
		return err
	}

	// TODO allow multidimensional

	if len(dims) > 1 {
		return fmt.Errorf("pq: scanning from multidimensional ARRAY%s is not implemented",
			strings.Replace(fmt.Sprint(dims), " ", "][", -1))
	}

	// Treat a zero-dimensional array like an array with a single dimension of zero.
	if len(dims) == 0 {
		dims = append(dims, 0)
	}

	for i, rt := 0, dv.Type(); i < len(dims); i, rt = i+1, rt.Elem() {
		switch rt.Kind() {
		case reflect.Slice:
		case reflect.Array:
			if rt.Len() != dims[i] {
				return fmt.Errorf("pq: cannot convert ARRAY%s to %s",
					strings.Replace(fmt.Sprint(dims), " ", "][", -1), dv.Type())
			}
		default:
			// TODO handle multidimensional
		}
	}

	values := reflect.MakeSlice(reflect.SliceOf(dtype), len(elems), len(elems))
	for i, e := range elems {
		if err := assign(e, values.Index(i)); err != nil {
			return fmt.Errorf("pq: parsing array element index %d: %v", i, err)
		}
	}

	// TODO handle multidimensional

	switch dv.Kind() {
	case reflect.Slice:
		dv.Set(values.Slice(0, dims[0]))
	case reflect.Array:
		for i := 0; i < dims[0]; i++ {
			dv.Index(i).Set(values.Index(i))
		}
	}

	return nil
}

// Value implements the driver.Valuer interface.
func (a GenericArray) Value() (driver.Value, error) {
	if a.A == nil {
		return nil, nil
	}

	rv := reflect.ValueOf(a.A)

	switch rv.Kind() {
	case reflect.Slice:
		if rv.IsNil() {
			return nil, nil
		}
	case reflect.Array:
	default:
		return nil, fmt.Errorf("pq: Unable to convert %T to array", a.A)
	}

	if n := rv.Len(); n > 0 {
		// There will be at least two curly brackets, N bytes of values,
		// and N-1 bytes of delimiters.
		b := make([]byte, 0, 1+2*n)

		b, _, err := appendArray(b, rv, n)
		return string(b), err
	}

	return "{}", nil
}

// Int64Array represents a one-dimensional array of the PostgreSQL integer types.
type Int64Array []int64

// Scan implements the sql.Scanner interface.
func (a *Int64Array) Scan(src interface{}) error {
	switch src := src.(type) {
	case []byte:
		return a.scanBytes(src)
	case string:
		return a.scanBytes([]byte(src))
	case nil:
		*a = nil
		return nil
	}

	return fmt.Errorf("pq: cannot convert %T to Int64Array", src)
}

func (a *Int64Array) scanBytes(src []byte) error {
	elems, err := scanLinearArray(src, []byte{','}, "Int64Array")
	if err != nil {
		return err
	}
	if *a != nil && len(elems) == 0 {
		*a = (*a)[:0]
	} else {
		b := make(Int64Array, len(elems))
		for i, v := range elems {
			if b[i], err = strconv.ParseInt(string(v), 10, 64); err != nil {
				return fmt.Errorf("pq: parsing array element index %d: %v", i, err)
			}
		}
		*a = b
	}
	return nil
}

// Value implements the driver.Valuer interface.
func (a Int64Array) Value() (driver.Value, error) {
	if a == nil {
		return nil, nil
	}

	if n := len(a); n > 0 {
		// There will be at least two curly brackets, N bytes of values,
		// and N-1 bytes of delimiters.
		b := make([]byte, 1, 1+2*n)
		b[0] = '{'

		b = strconv.AppendInt(b, a[0], 10)
		for i := 1; i < n; i++ {
			b = append(b, ',')
			b = strconv.AppendInt(b, a[i], 10)
		}

		return string(append(b, '}')), nil
	}

	return "{}", nil
}

// Int32Array represents a one-dimensional array of the PostgreSQL integer types.
type Int32Array []int32

// Scan implements the sql.Scanner interface.
func (a *Int32Array) Scan(src interface{}) error {
	switch src := src.(type) {
	case []byte:
		return a.scanBytes(src)
	case string:
		return a.scanBytes([]byte(src))
	case nil:
		*a = nil
		return nil
	}

	return fmt.Errorf("pq: cannot convert %T to Int32Array", src)
}

func (a *Int32Array) scanBytes(src []byte) error {
	elems, err := scanLinearArray(src, []byte{','}, "Int32Array")
	if err != nil {
		return err
	}
	if *a != nil && len(elems) == 0 {
		*a = (*a)[:0]
	} else {
		b := make(Int32Array, len(elems))
		for i, v := range elems {
			x, err := strconv.ParseInt(string(v), 10, 32)
			if err != nil {
				return fmt.Errorf("pq: parsing array element index %d: %v", i, err)
			}
			b[i] = int32(x)
		}
		*a = b
	}
	return nil
}

// Value implements the driver.Valuer interface.
func (a Int32Array) Value() (driver.Value, error) {
	if a == nil {
		return nil, nil
	}

	if n := len(a); n > 0 {
		// There will be at least two curly brackets, N bytes of values,
		// and N-1 bytes of delimiters.
		b := make([]byte, 1, 1+2*n)
		b[0] = '{'

		b = strconv.AppendInt(b, int64(a[0]), 10)
		for i := 1; i < n; i++ {
			b = append(b, ',')
			b = strconv.AppendInt(b, int64(a[i]), 10)
		}

		return string(append(b, '}')), nil
	}

	return "{}", nil
}

// StringArray represents a one-dimensional array of the PostgreSQL character types.
type StringArray []string

// Scan implements the sql.Scanner interface.
func (a *StringArray) Scan(src interface{}) error {
	switch src := src.(type) {
	case []byte:
		return a.scanBytes(src)
	case string:
		return a.scanBytes([]byte(src))
	case nil:
		*a = nil
		return nil
	}

	return fmt.Errorf("pq: cannot convert %T to StringArray", src)
}

func (a *StringArray) scanBytes(src []byte) error {
	elems, err := scanLinearArray(src, []byte{','}, "StringArray")
	if err != nil {
		return err
	}
	if *a != nil && len(elems) == 0 {
		*a = (*a)[:0]
	} else {
		b := make(StringArray, len(elems))
		for i, v := range elems {
			if b[i] = string(v); v == nil {
				return fmt.Errorf("pq: parsing array element index %d: cannot convert nil to string", i)
			}
		}
		*a = b
	}
	return nil
}

// Value implements the driver.Valuer interface.
func (a StringArray) Value() (driver.Value, error) {
	if a == nil {
		return nil, nil
	}

	if n := len(a); n > 0 {
		// There will be at least two curly brackets, 2*N bytes of quotes,
		// and N-1 bytes of delimiters.
		b := make([]byte, 1, 1+3*n)
		b[0] = '{'

		b = appendArrayQuotedBytes(b, []byte(a[0]))
		for i := 1; i < n; i++ {
			b = append(b, ',')
			b = appendArrayQuotedBytes(b, []byte(a[i]))
		}

		return string(append(b, '}')), nil
	}

	return "{}", nil
}

// appendArray appends rv to the buffer, returning the extended buffer and
// the delimiter used between elements.
//
// It panics when n <= 0 or rv's Kind is not reflect.Array nor reflect.Slice.
func appendArray(b []byte, rv reflect.Value, n int) ([]byte, string, error) {
	var del string
	var err error

	b = append(b, '{')

	if b, del, err = appendArrayElement(b, rv.Index(0)); err != nil {
		return b, del, err
	}

	for i := 1; i < n; i++ {
		b = append(b, del...)
		if b, del, err = appendArrayElement(b, rv.Index(i)); err != nil {
			return b, del, err
		}
	}

	return append(b, '}'), del, nil
}

// appendArrayElement appends rv to the buffer, returning the extended buffer
// and the delimiter to use before the next element.
//
// When rv's Kind is neither reflect.Array nor reflect.Slice, it is converted
// using driver.DefaultParameterConverter and the resulting []byte or string
// is double-quoted.
//
// See http://www.postgresql.org/docs/current/static/arrays.html#ARRAYS-IO
func appendArrayElement(b []byte, rv reflect.Value) ([]byte, string, error) {
	if k := rv.Kind(); k == reflect.Array || k == reflect.Slice {
		if t := rv.Type(); t != typeByteSlice && !t.Implements(typeDriverValuer) {
			if n := rv.Len(); n > 0 {
				return appendArray(b, rv, n)
			}

			return b, "", nil
		}
	}

	var del = ","
	var err error
	var iv interface{} = rv.Interface()

	if ad, ok := iv.(ArrayDelimiter); ok {
		del = ad.ArrayDelimiter()
	}

	if iv, err = driver.DefaultParameterConverter.ConvertValue(iv); err != nil {
		return b, del, err
	}

	switch v := iv.(type) {
	case nil:
		return append(b, "NULL"...), del, nil
	case []byte:
		return appendArrayQuotedBytes(b, v), del, nil
	case string:
		return appendArrayQuotedBytes(b, []byte(v)), del, nil
	}

	b, err = appendValue(b, iv)
	return b, del, err
}

func appendArrayQuotedBytes(b, v []byte) []byte {
	b = append(b, '"')
	for {
		i := bytes.IndexAny(v, `"\`)
		if i < 0 {
			b = append(b, v...)
			break
		}
		if i > 0 {
			b = append(b, v[:i]...)
		}
		b = append(b, '\\', v[i])
		v = v[i+1:]
	}
	return append(b, '"')
}

func appendValue(b []byte, v driver.Value) ([]byte, error) {
	return append(b, encode(nil, v, 0)...), nil
}

// parseArray extracts the dimensions and elements of an array represented in
// text format. Only representations emitted by the backend are supported.
// Notably, whitespace around brackets and delimiters is significant, and NULL
// is case-sensitive.
//
// See http://www.postgresql.org/docs/current/static/arrays.html#ARRAYS-IO
func parseArray(src, del []byte) (dims []int, elems [][]byte, err error) {
	var depth, i int

	if len(src) < 1 || src[0] != '{' {
		return nil, nil, fmt.Errorf("pq: unable to parse array; expected %q at offset %d", '{', 0)
	}

Open:
	for i < len(src) {
		switch src[i] {
		case '{':
			depth++
			i++
		case '}':
			elems = make([][]byte, 0)
			goto Close
		default:
			break Open
		}
	}
	dims = make([]int, i)

Element:
	for i < len(src) {
		switch src[i] {
		case '{':
			if depth == len(dims) {
				break Element
			}
			depth++
			dims[depth-1] = 0
			i++
		case '"':
			var elem = []byte{}
			var escape bool
			for i++; i < len(src); i++ {
				if escape {
					elem = append(elem, src[i])
					escape = false
				} else {
					switch src[i] {
					default:
						elem = append(elem, src[i])
					case '\\':
						escape = true
					case '"':
						elems = append(elems, elem)
						i++
						break Element
					}
				}
			}
		default:
			for start := i; i < len(src); i++ {
				if bytes.HasPrefix(src[i:], del) || src[i] == '}' {
					elem := src[start:i]
					if len(elem) == 0 {
						return nil, nil, fmt.Errorf("pq: unable to parse array; unexpected %q at offset %d", src[i], i)
					}
					if bytes.Equal(elem, []byte("NULL")) {
						elem = nil
					}
					elems = append(elems, elem)
					break Element
				}
			}
		}
	}

	for i < len(src) {
		if bytes.HasPrefix(src[i:], del) && depth > 0 {
			dims[depth-1]++
			i += len(del)
			goto Element
		} else if src[i] == '}' && depth > 0 {
			dims[depth-1]++
			depth--
			i++
		} else {
			return nil, nil, fmt.Errorf("pq: unable to parse array; unexpected %q at offset %d", src[i], i)
		}
	}

Close:
	for i < len(src) {
		if src[i] == '}' && depth > 0 {
			depth--
			i++
		} else {
			return nil, nil, fmt.Errorf("pq: unable to parse array; unexpected %q at offset %d", src[i], i)
		}
	}
	if depth > 0 {
		err = fmt.Errorf("pq: unable to parse array; expected %q at offset %d", '}', i)
	}
	if err == nil {
		for _, d := range dims {
			if (len(elems) % d) != 0 {
				err = fmt.Errorf("pq: multidimensional arrays must have elements with matching dimensions")
			}
		}
	}
	return
}

func scanLinearArray(src, del []byte, typ string) (elems [][]byte, err error) {
	dims, elems, err := parseArray(src, del)
	if err != nil {
		return nil, err
	}
	if len(dims) > 1 {
		return nil, fmt.Errorf("pq: cannot convert ARRAY%s to %s", strings.Replace(fmt.Sprint(dims), " ", "][", -1), typ)
	}
	return elems, err
}

func parseBytea(s []byte) (result []byte, err error) {
	if len(s) >= 2 && bytes.Equal(s[:2], []byte("\\x")) {
		// bytea_output = hex
		s = s[2:] // trim off leading "\\x"
		result = make([]byte, hex.DecodedLen(len(s)))
		_, err := hex.Decode(result, s)
		if err != nil {
			return nil, err
		}
	} else {
		// bytea_output = escape
		for len(s) > 0 {
			if s[0] == '\\' {
				// escaped '\\'
				if len(s) >= 2 && s[1] == '\\' {
					result = append(result, '\\')
					s = s[2:]
					continue
				}

				// '\\' followed by an octal number
				if len(s) < 4 {
					return nil, fmt.Errorf("invalid bytea sequence %v", s)
				}
				r, err := strconv.ParseUint(string(s[1:4]), 8, 8)
				if err != nil {
					return nil, fmt.Errorf("could not parse bytea value: %s", err.Error())
				}
				result = append(result, byte(r))
				s = s[4:]
			} else {
				// We hit an unescaped, raw byte.  Try to read in as many as
				// possible in one go.
				i := bytes.IndexByte(s, '\\')
				if i == -1 {
					result = append(result, s...)
					break
				}
				result = append(result, s[:i]...)
				s = s[i:]
			}
		}
	}
	return result, nil
}

func encode(parameterStatus *parameterStatus, x interface{}, pgtypOid Oid) []byte {
	switch v := x.(type) {
	case int64:
		return strconv.AppendInt(nil, v, 10)
	case float64:
		return strconv.AppendFloat(nil, v, 'f', -1, 64)
	case []byte:
		if pgtypOid == T_bytea {
			return encodeBytea(parameterStatus.serverVersion, v)
		}

		return v
	case string:
		if pgtypOid == T_bytea {
			return encodeBytea(parameterStatus.serverVersion, []byte(v))
		}

		return []byte(v)
	case bool:
		return strconv.AppendBool(nil, v)
	case time.Time:
		return formatTs(v)

	default:
		panic(fmt.Errorf("pq: %s", fmt.Sprintf("encode: unknown type for %T", v)))
	}
}

type parameterStatus struct {
	// server version in the same format as server_version_num, or 0 if
	// unavailable
	serverVersion int

	// the current location based on the TimeZone value of the session, if
	// available
	currentLocation *time.Location
}

func encodeBytea(serverVersion int, v []byte) (result []byte) {
	if serverVersion >= 90000 {
		// Use the hex format if we know that the server supports it
		result = make([]byte, 2+hex.EncodedLen(len(v)))
		result[0] = '\\'
		result[1] = 'x'
		hex.Encode(result[2:], v)
	} else {
		// .. or resort to "escape"
		for _, b := range v {
			if b == '\\' {
				result = append(result, '\\', '\\')
			} else if b < 0x20 || b > 0x7e {
				result = append(result, []byte(fmt.Sprintf("\\%03o", b))...)
			} else {
				result = append(result, b)
			}
		}
	}

	return result
}

var infinityTsEnabled = false
var infinityTsNegative time.Time
var infinityTsPositive time.Time

// formatTs formats t into a format postgres understands.
func formatTs(t time.Time) []byte {
	if infinityTsEnabled {
		// t <= -infinity : ! (t > -infinity)
		if !t.After(infinityTsNegative) {
			return []byte("-infinity")
		}
		// t >= infinity : ! (!t < infinity)
		if !t.Before(infinityTsPositive) {
			return []byte("infinity")
		}
	}
	return FormatTimestamp(t)
}

// FormatTimestamp formats t into Postgres' text format for timestamps.
func FormatTimestamp(t time.Time) []byte {
	// Need to send dates before 0001 A.D. with " BC" suffix, instead of the
	// minus sign preferred by Go.
	// Beware, "0000" in ISO is "1 BC", "-0001" is "2 BC" and so on
	bc := false
	if t.Year() <= 0 {
		// flip year sign, and add 1, e.g: "0" will be "1", and "-10" will be "11"
		t = t.AddDate((-t.Year())*2+1, 0, 0)
		bc = true
	}
	b := []byte(t.Format("2006-01-02 15:04:05.999999999Z07:00"))

	_, offset := t.Zone()
	offset %= 60
	if offset != 0 {
		// RFC3339Nano already printed the minus sign
		if offset < 0 {
			offset = -offset
		}

		b = append(b, ':')
		if offset < 10 {
			b = append(b, '0')
		}
		b = strconv.AppendInt(b, int64(offset), 10)
	}

	if bc {
		b = append(b, " BC"...)
	}
	return b
}

// Oid is a Postgres Object ID.
type Oid uint32

const (
	T_bool             Oid = 16
	T_bytea            Oid = 17
	T_char             Oid = 18
	T_name             Oid = 19
	T_int8             Oid = 20
	T_int2             Oid = 21
	T_int2vector       Oid = 22
	T_int4             Oid = 23
	T_regproc          Oid = 24
	T_text             Oid = 25
	T_oid              Oid = 26
	T_tid              Oid = 27
	T_xid              Oid = 28
	T_cid              Oid = 29
	T_oidvector        Oid = 30
	T_pg_ddl_command   Oid = 32
	T_pg_type          Oid = 71
	T_pg_attribute     Oid = 75
	T_pg_proc          Oid = 81
	T_pg_class         Oid = 83
	T_json             Oid = 114
	T_xml              Oid = 142
	T__xml             Oid = 143
	T_pg_node_tree     Oid = 194
	T__json            Oid = 199
	T_smgr             Oid = 210
	T_index_am_handler Oid = 325
	T_point            Oid = 600
	T_lseg             Oid = 601
	T_path             Oid = 602
	T_box              Oid = 603
	T_polygon          Oid = 604
	T_line             Oid = 628
	T__line            Oid = 629
	T_cidr             Oid = 650
	T__cidr            Oid = 651
	T_float4           Oid = 700
	T_float8           Oid = 701
	T_abstime          Oid = 702
	T_reltime          Oid = 703
	T_tinterval        Oid = 704
	T_unknown          Oid = 705
	T_circle           Oid = 718
	T__circle          Oid = 719
	T_money            Oid = 790
	T__money           Oid = 791
	T_macaddr          Oid = 829
	T_inet             Oid = 869
	T__bool            Oid = 1000
	T__bytea           Oid = 1001
	T__char            Oid = 1002
	T__name            Oid = 1003
	T__int2            Oid = 1005
	T__int2vector      Oid = 1006
	T__int4            Oid = 1007
	T__regproc         Oid = 1008
	T__text            Oid = 1009
	T__tid             Oid = 1010
	T__xid             Oid = 1011
	T__cid             Oid = 1012
	T__oidvector       Oid = 1013
	T__bpchar          Oid = 1014
	T__varchar         Oid = 1015
	T__int8            Oid = 1016
	T__point           Oid = 1017
	T__lseg            Oid = 1018
	T__path            Oid = 1019
	T__box             Oid = 1020
	T__float4          Oid = 1021
	T__float8          Oid = 1022
	T__abstime         Oid = 1023
	T__reltime         Oid = 1024
	T__tinterval       Oid = 1025
	T__polygon         Oid = 1027
	T__oid             Oid = 1028
	T_aclitem          Oid = 1033
	T__aclitem         Oid = 1034
	T__macaddr         Oid = 1040
	T__inet            Oid = 1041
	T_bpchar           Oid = 1042
	T_varchar          Oid = 1043
	T_date             Oid = 1082
	T_time             Oid = 1083
	T_timestamp        Oid = 1114
	T__timestamp       Oid = 1115
	T__date            Oid = 1182
	T__time            Oid = 1183
	T_timestamptz      Oid = 1184
	T__timestamptz     Oid = 1185
	T_interval         Oid = 1186
	T__interval        Oid = 1187
	T__numeric         Oid = 1231
	T_pg_database      Oid = 1248
	T__cstring         Oid = 1263
	T_timetz           Oid = 1266
	T__timetz          Oid = 1270
	T_bit              Oid = 1560
	T__bit             Oid = 1561
	T_varbit           Oid = 1562
	T__varbit          Oid = 1563
	T_numeric          Oid = 1700
	T_refcursor        Oid = 1790
	T__refcursor       Oid = 2201
	T_regprocedure     Oid = 2202
	T_regoper          Oid = 2203
	T_regoperator      Oid = 2204
	T_regclass         Oid = 2205
	T_regtype          Oid = 2206
	T__regprocedure    Oid = 2207
	T__regoper         Oid = 2208
	T__regoperator     Oid = 2209
	T__regclass        Oid = 2210
	T__regtype         Oid = 2211
	T_record           Oid = 2249
	T_cstring          Oid = 2275
	T_any              Oid = 2276
	T_anyarray         Oid = 2277
	T_void             Oid = 2278
	T_trigger          Oid = 2279
	T_language_handler Oid = 2280
	T_internal         Oid = 2281
	T_opaque           Oid = 2282
	T_anyelement       Oid = 2283
	T__record          Oid = 2287
	T_anynonarray      Oid = 2776
	T_pg_authid        Oid = 2842
	T_pg_auth_members  Oid = 2843
	T__txid_snapshot   Oid = 2949
	T_uuid             Oid = 2950
	T__uuid            Oid = 2951
	T_txid_snapshot    Oid = 2970
	T_fdw_handler      Oid = 3115
	T_pg_lsn           Oid = 3220
	T__pg_lsn          Oid = 3221
	T_tsm_handler      Oid = 3310
	T_anyenum          Oid = 3500
	T_tsvector         Oid = 3614
	T_tsquery          Oid = 3615
	T_gtsvector        Oid = 3642
	T__tsvector        Oid = 3643
	T__gtsvector       Oid = 3644
	T__tsquery         Oid = 3645
	T_regconfig        Oid = 3734
	T__regconfig       Oid = 3735
	T_regdictionary    Oid = 3769
	T__regdictionary   Oid = 3770
	T_jsonb            Oid = 3802
	T__jsonb           Oid = 3807
	T_anyrange         Oid = 3831
	T_event_trigger    Oid = 3838
	T_int4range        Oid = 3904
	T__int4range       Oid = 3905
	T_numrange         Oid = 3906
	T__numrange        Oid = 3907
	T_tsrange          Oid = 3908
	T__tsrange         Oid = 3909
	T_tstzrange        Oid = 3910
	T__tstzrange       Oid = 3911
	T_daterange        Oid = 3912
	T__daterange       Oid = 3913
	T_int8range        Oid = 3926
	T__int8range       Oid = 3927
	T_pg_shseclabel    Oid = 4066
	T_regnamespace     Oid = 4089
	T__regnamespace    Oid = 4090
	T_regrole          Oid = 4096
	T__regrole         Oid = 4097
)

var TypeName = map[Oid]string{
	T_bool:             "BOOL",
	T_bytea:            "BYTEA",
	T_char:             "CHAR",
	T_name:             "NAME",
	T_int8:             "INT8",
	T_int2:             "INT2",
	T_int2vector:       "INT2VECTOR",
	T_int4:             "INT4",
	T_regproc:          "REGPROC",
	T_text:             "TEXT",
	T_oid:              "OID",
	T_tid:              "TID",
	T_xid:              "XID",
	T_cid:              "CID",
	T_oidvector:        "OIDVECTOR",
	T_pg_ddl_command:   "PG_DDL_COMMAND",
	T_pg_type:          "PG_TYPE",
	T_pg_attribute:     "PG_ATTRIBUTE",
	T_pg_proc:          "PG_PROC",
	T_pg_class:         "PG_CLASS",
	T_json:             "JSON",
	T_xml:              "XML",
	T__xml:             "_XML",
	T_pg_node_tree:     "PG_NODE_TREE",
	T__json:            "_JSON",
	T_smgr:             "SMGR",
	T_index_am_handler: "INDEX_AM_HANDLER",
	T_point:            "POINT",
	T_lseg:             "LSEG",
	T_path:             "PATH",
	T_box:              "BOX",
	T_polygon:          "POLYGON",
	T_line:             "LINE",
	T__line:            "_LINE",
	T_cidr:             "CIDR",
	T__cidr:            "_CIDR",
	T_float4:           "FLOAT4",
	T_float8:           "FLOAT8",
	T_abstime:          "ABSTIME",
	T_reltime:          "RELTIME",
	T_tinterval:        "TINTERVAL",
	T_unknown:          "UNKNOWN",
	T_circle:           "CIRCLE",
	T__circle:          "_CIRCLE",
	T_money:            "MONEY",
	T__money:           "_MONEY",
	T_macaddr:          "MACADDR",
	T_inet:             "INET",
	T__bool:            "_BOOL",
	T__bytea:           "_BYTEA",
	T__char:            "_CHAR",
	T__name:            "_NAME",
	T__int2:            "_INT2",
	T__int2vector:      "_INT2VECTOR",
	T__int4:            "_INT4",
	T__regproc:         "_REGPROC",
	T__text:            "_TEXT",
	T__tid:             "_TID",
	T__xid:             "_XID",
	T__cid:             "_CID",
	T__oidvector:       "_OIDVECTOR",
	T__bpchar:          "_BPCHAR",
	T__varchar:         "_VARCHAR",
	T__int8:            "_INT8",
	T__point:           "_POINT",
	T__lseg:            "_LSEG",
	T__path:            "_PATH",
	T__box:             "_BOX",
	T__float4:          "_FLOAT4",
	T__float8:          "_FLOAT8",
	T__abstime:         "_ABSTIME",
	T__reltime:         "_RELTIME",
	T__tinterval:       "_TINTERVAL",
	T__polygon:         "_POLYGON",
	T__oid:             "_OID",
	T_aclitem:          "ACLITEM",
	T__aclitem:         "_ACLITEM",
	T__macaddr:         "_MACADDR",
	T__inet:            "_INET",
	T_bpchar:           "BPCHAR",
	T_varchar:          "VARCHAR",
	T_date:             "DATE",
	T_time:             "TIME",
	T_timestamp:        "TIMESTAMP",
	T__timestamp:       "_TIMESTAMP",
	T__date:            "_DATE",
	T__time:            "_TIME",
	T_timestamptz:      "TIMESTAMPTZ",
	T__timestamptz:     "_TIMESTAMPTZ",
	T_interval:         "INTERVAL",
	T__interval:        "_INTERVAL",
	T__numeric:         "_NUMERIC",
	T_pg_database:      "PG_DATABASE",
	T__cstring:         "_CSTRING",
	T_timetz:           "TIMETZ",
	T__timetz:          "_TIMETZ",
	T_bit:              "BIT",
	T__bit:             "_BIT",
	T_varbit:           "VARBIT",
	T__varbit:          "_VARBIT",
	T_numeric:          "NUMERIC",
	T_refcursor:        "REFCURSOR",
	T__refcursor:       "_REFCURSOR",
	T_regprocedure:     "REGPROCEDURE",
	T_regoper:          "REGOPER",
	T_regoperator:      "REGOPERATOR",
	T_regclass:         "REGCLASS",
	T_regtype:          "REGTYPE",
	T__regprocedure:    "_REGPROCEDURE",
	T__regoper:         "_REGOPER",
	T__regoperator:     "_REGOPERATOR",
	T__regclass:        "_REGCLASS",
	T__regtype:         "_REGTYPE",
	T_record:           "RECORD",
	T_cstring:          "CSTRING",
	T_any:              "ANY",
	T_anyarray:         "ANYARRAY",
	T_void:             "VOID",
	T_trigger:          "TRIGGER",
	T_language_handler: "LANGUAGE_HANDLER",
	T_internal:         "INTERNAL",
	T_opaque:           "OPAQUE",
	T_anyelement:       "ANYELEMENT",
	T__record:          "_RECORD",
	T_anynonarray:      "ANYNONARRAY",
	T_pg_authid:        "PG_AUTHID",
	T_pg_auth_members:  "PG_AUTH_MEMBERS",
	T__txid_snapshot:   "_TXID_SNAPSHOT",
	T_uuid:             "UUID",
	T__uuid:            "_UUID",
	T_txid_snapshot:    "TXID_SNAPSHOT",
	T_fdw_handler:      "FDW_HANDLER",
	T_pg_lsn:           "PG_LSN",
	T__pg_lsn:          "_PG_LSN",
	T_tsm_handler:      "TSM_HANDLER",
	T_anyenum:          "ANYENUM",
	T_tsvector:         "TSVECTOR",
	T_tsquery:          "TSQUERY",
	T_gtsvector:        "GTSVECTOR",
	T__tsvector:        "_TSVECTOR",
	T__gtsvector:       "_GTSVECTOR",
	T__tsquery:         "_TSQUERY",
	T_regconfig:        "REGCONFIG",
	T__regconfig:       "_REGCONFIG",
	T_regdictionary:    "REGDICTIONARY",
	T__regdictionary:   "_REGDICTIONARY",
	T_jsonb:            "JSONB",
	T__jsonb:           "_JSONB",
	T_anyrange:         "ANYRANGE",
	T_event_trigger:    "EVENT_TRIGGER",
	T_int4range:        "INT4RANGE",
	T__int4range:       "_INT4RANGE",
	T_numrange:         "NUMRANGE",
	T__numrange:        "_NUMRANGE",
	T_tsrange:          "TSRANGE",
	T__tsrange:         "_TSRANGE",
	T_tstzrange:        "TSTZRANGE",
	T__tstzrange:       "_TSTZRANGE",
	T_daterange:        "DATERANGE",
	T__daterange:       "_DATERANGE",
	T_int8range:        "INT8RANGE",
	T__int8range:       "_INT8RANGE",
	T_pg_shseclabel:    "PG_SHSECLABEL",
	T_regnamespace:     "REGNAMESPACE",
	T__regnamespace:    "_REGNAMESPACE",
	T_regrole:          "REGROLE",
	T__regrole:         "_REGROLE",
}
