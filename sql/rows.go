package sql

import (
	"database/sql/driver"
	"errors"
	"io"
)

var IsNotTupleError = errors.New("is not a tuple")

type rows struct {
	data    []interface{}
	columns []string
	iter    int
	closed  bool
}

func (r *rows) readColumns(meta map[string]string) {
	var columns []string

	for k := range meta {
		columns = append(columns, k)
	}

	r.columns = columns
}

func (r *rows) Close() error {
	r.closed = true

	return nil
}

func (r rows) Columns() []string {
	return r.columns
}

func (r *rows) Next(dest []driver.Value) error {
	if r.closed || r.iter >= len(r.data) {
		return io.EOF
	}

	row, ok := r.data[r.iter].([]interface{})
	r.iter++

	if !ok {
		return IsNotTupleError
	}

	for i, val := range row {
		dest[i] = driver.Value(val)
	}

	return nil
}
