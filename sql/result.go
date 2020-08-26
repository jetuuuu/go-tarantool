package sql

import (
	"errors"
)

type execResult struct {
	affectedRowCount int64
}

func (r execResult) LastInsertId() (int64, error) {
	return 0, errors.New("not implemented")
}

func (r execResult) RowsAffected() (int64, error) {
	return r.affectedRowCount, nil
}
