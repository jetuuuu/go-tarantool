package sql

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/tarantool/go-tarantool"
)

func TestConnect(t *testing.T) {
	opts := tarantool.Opts{User: "guest"}
	conn, err := tarantool.Connect("127.0.0.1:3301", opts)

	if err != nil {
		fmt.Println("Connection refused:", err)
	}

	//resp, err = conn.Select(spaceNo, indexNo, 0, 1, tarantool.IterEq, []interface{}{uint(15)})
	resp, err := conn.Execute("SELECT * FROM table1", nil)
	fmt.Println(err, resp.Data)
}

func TestSQL(t *testing.T) {
	dt := Tarantool{
		Options: tarantool.Opts{User: "guest"},
	}

	sql.Register("tarantool", &dt)

	db, err := sql.Open("tarantool", "127.0.0.1:3301")
	if err != nil {
		t.Fatal(err)
	}

	rows, err := db.Query("select * from table1")
	if err != nil {
		t.Fatal(err)
	}

	for rows.Next() {
		var (
			index int
			value string
		)

		err := rows.Scan(&index, &value)
		if err != nil {
			t.Fatal(err)
		}

		fmt.Println(index, value)
	}
}
