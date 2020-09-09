package sql

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/valenok-husky/go-tarantool"
)

func TestConnect(t *testing.T) {
	opts := tarantool.Opts{User: "guest"}
	conn, err := tarantool.Connect("localhost:3301", opts)

	if err != nil {
		fmt.Println("Connection refused:", err)
	}

	resp, err := conn.Execute("SELECT * FROM table1 where column1 = ?", []interface{}{1})
	fmt.Println(err, resp.Data)
}

func TestSQL(t *testing.T) {
	dt := Tarantool{
		Options: tarantool.Opts{User: "guest"},
	}

	sql.Register("tarantool", &dt)

	db, err := sql.Open("tarantool", "localhost:3301")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec("delete from table1 where 1=1")
	if err != nil {
		t.Fatal(err.Error())
	}

	result, err := db.Exec("insert into table1 values (?,?)", 1, "nikita")
	if err != nil {
		t.Fatal(err.Error())
	}

	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatal(err.Error())
	}

	if affected != 1 {
		t.Fatalf("should be affected 1 row but %d", affected)
	}

	rows, err := db.Query("select * from table1 where column1 = $1", 1)
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

		if index != 1 && value != "nikita" {
			t.Fatalf(`got [%d; %s] expected [1, "nikita"]`, index, value)
		}
	}
}

func TestSQL_Replica(t *testing.T) {
	dt := Tarantool{
		Options: tarantool.Opts{User: "guest"},
	}

	sql.Register("tarantool", &dt)

	db, err := sql.Open("tarantool", "localhost:3301,localhost:3302")
	if err != nil {
		t.Fatal(err)
	}

	db.SetConnMaxLifetime(10 * time.Second)

	go func() {
		for i := 3; i < 1000; i++ {
			_, err = db.Exec("insert into table1 values (?,?)", i, fmt.Sprintf("%d", i))
			if err != nil {
				// t.Fatal(err.Error())
				fmt.Println("insert: ", err)
			}

			time.Sleep(3 * time.Second)
		}
	}()

	go func() {
		for {
			row := db.QueryRow("select count(*) from table1")
			if err := row.Err(); err != nil {
				fmt.Println("select: ", err)
			} else {
				var cnt int

				err := row.Scan(&cnt)
				if err != nil {
					fmt.Println("scan: ", err)
				} else {
					fmt.Println("cnt: ", cnt)
				}
			}

			time.Sleep(2 * time.Second)
		}
	}()

	time.Sleep(2 * time.Minute)
}
