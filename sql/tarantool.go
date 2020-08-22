package sql

import (
	//_sql "database/sql"
	_driver "database/sql/driver"
	"fmt"
	"io"

	//"net"

	"github.com/tarantool/go-tarantool"
)

type Tarantool struct {
	Options tarantool.Opts
}

type Connection struct {
	conn *tarantool.Connection
}

func (c *Connection) Prepare(query string) (_driver.Stmt, error) {
	return nil, fmt.Errorf("not implemented")
}

func (c *Connection) Close() error {
	return c.conn.Close()
}

func (c *Connection) Begin() (_driver.Tx, error) {
	return nil, nil
}

func (c *Connection) Commit() error {
	return nil
}

func (c *Connection) Rollback() error {
	return nil
}

func (c *Connection) Query(query string, args []_driver.Value) (_driver.Rows, error) {
	resp, err := c.conn.Execute(query, args)
	if err != nil {
		return nil, err
	}

	r := &rows{data: resp.Data}
	r.readColumns(resp.Meta)

	return r, nil
}

type rows struct {
	data   []interface{}
	columns []string
	iter   int
	closed bool
}

func (r *rows) Close() error {
	r.closed = true

	return nil
}

func (r rows) Columns() []string {
	return r.columns
}

func (r *rows) readColumns(meta map[string]string) {
	var columns []string

	for k := range meta {
		columns = append(columns, k)
	}

	r.columns = columns
}

func (r *rows) Next(dest []_driver.Value) error {
	if r.closed || r.iter >= len(r.data) {
		return io.EOF
	}

	row := r.data[r.iter].([]interface{})
	r.iter++

	for i, val := range row {
		dest[i] = _driver.Value(val)
	}

	return nil
}

func (t Tarantool) Open(name string) (_driver.Conn, error) {
	conn, err := tarantool.Connect(name, t.Options)
	if err != nil {
		return nil, err
	}

	return &Connection{conn: conn}, nil
}
