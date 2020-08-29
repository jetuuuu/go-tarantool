package sql

import (
	"context"
	_driver "database/sql/driver"
	"errors"
	"fmt"
	"strings"

	"github.com/tarantool/go-tarantool"
)

var EmptyConnectionError = errors.New("connection string is empty")

type Tarantool struct {
	Options tarantool.Opts
}

type Connection struct {
	master      *tarantool.Connection
	replicas    []*tarantool.Connection
	nextReplica int
}

func (c *Connection) Prepare(query string) (_driver.Stmt, error) {
	return nil, fmt.Errorf("not implemented")
}

func (c *Connection) Close() error {
	var errs []string

	if c.master != nil {
		err := c.master.Close()
		if err != nil {
			errs = append(errs, err.Error())
		}
	}

	for _, r := range c.replicas {
		if r != nil {
			err := r.Close()
			if err != nil {
				errs = append(errs, err.Error())
			}
		}
	}

	if len(errs) != 0 {
		return errors.New(strings.Join(errs, "\n"))
	}

	return nil
}

func (c *Connection) Begin() (_driver.Tx, error) {
	return nil, errors.New("not implemented")
}

func (c *Connection) BeginTx(ctx context.Context, opt _driver.TxOptions) (_driver.Tx, error) {
	return c.Begin()
}

func (c *Connection) Query(query string, args []_driver.Value) (_driver.Rows, error) {
	defer func() {
		if c.nextReplica >= (len(c.replicas) - 1) {
			c.nextReplica = 0
		} else {
			c.nextReplica++
		}
	}()

	instance := c.replicas[c.nextReplica]

	if instance == nil {
		return nil, _driver.ErrBadConn
	}

	if args == nil {
		args = []_driver.Value{}
	}

	resp, err := instance.Execute(query, args)
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
	if c.master == nil {
		return nil, _driver.ErrBadConn
	}

	if args == nil {
		args = []_driver.Value{}
	}

	resp, err := c.master.Execute(query, args)
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
	names := strings.Split(name, ",")
	if len(names) == 0 {
		return nil, EmptyConnectionError
	}

	master, masterErr := tarantool.Connect(names[0], t.Options)
	if masterErr != nil && len(names) == 1 {
		return nil, masterErr
	}

	c := &Connection{master: master}

	if len(names) == 1 {
		c.replicas = []*tarantool.Connection{master}

		return c, nil
	}

	for _, addr := range names[1:] {
		replica, err := tarantool.Connect(addr, t.Options)
		if err == nil {
			c.replicas = append(c.replicas, replica)
		}
	}

	if masterErr != nil && len(c.replicas) == 0 {
		return nil, _driver.ErrBadConn
	}

	return c, nil
}
