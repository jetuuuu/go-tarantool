package sql

import (
	"context"
	_driver "database/sql/driver"
	"errors"
	"fmt"

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
	return nil, errors.New("not implemented")
}

func (c *Connection) BeginTx(ctx context.Context, opt _driver.TxOptions) (_driver.Tx, error) {
	return c.Begin()
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

func (c *Connection) QueryContext(ctx context.Context, query string, namedArgs []_driver.NamedValue) (_driver.Rows, error) {
	var args []_driver.Value

	for _, a := range namedArgs {
		args = append(args, a.Value)
	}

	return c.Query(query, args)
}

func (c *Connection) Exec(query string, args []_driver.Value) (_driver.Result, error) {
	resp, err := c.conn.Execute(query, args)
	if err != nil {
		return nil, err
	}

	return execResult{affectedRowCount: int64(resp.SQLChangedRowCount)}, nil
}

func (c *Connection) ExecContext(ctx context.Context, query string, namedArgs []_driver.NamedValue) (_driver.Result, error) {
	var args []_driver.Value

	for _, a := range namedArgs {
		args = append(args, a.Value)
	}

	return c.Exec(query, args)
}

func (t Tarantool) Open(name string) (_driver.Conn, error) {
	conn, err := tarantool.Connect(name, t.Options)
	if err != nil {
		return nil, err
	}

	return &Connection{conn: conn}, nil
}
