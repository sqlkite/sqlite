package sqlite_test

import (
	"errors"
	"io/fs"
	"math/rand"
	"os"
	"testing"
	"time"

	"src.sqlkite.com/sqlite"
	"src.sqlkite.com/tests/assert"
)

type TestRow struct {
	Id    int
	Int   int
	Intn  *int
	Real  float64
	Realn *float64
	Text  string
	Textn *string
	Blob  []byte
	Blobn *[]byte
	Time  time.Time
	Timen *time.Time
}

func Test_Conn_NotExistWhenNotCreate(t *testing.T) {
	_, err := sqlite.Open("fail", false)
	assert.Equal(t, errors.Is(err, fs.ErrNotExist), true)
}

func Test_Conn_ExecAndScan(t *testing.T) {
	db := testDB()
	defer db.Close()

	now := time.Now()
	mustExec(db, `
		insert into test (cint, creal, ctext, cblob, ctime)
		values (?1, ?2, ?3, ?4, ?5)
	`, 1, 2.2, "three", []byte("four"), now)
	assert.Equal(t, db.Changes(), 1)

	lastId := db.LastInsertRowID()
	row := queryLast(db)
	assert.Equal(t, row.Id, lastId)
	assert.Equal(t, row.Int, 1)
	assert.Equal(t, row.Real, 2.2)
	assert.Equal(t, row.Text, "three")
	assert.Equal(t, string(row.Blob), "four")
	assert.Equal(t, row.Time, now.Truncate(time.Second))
	assert.True(t, row.Intn == nil)
	assert.True(t, row.Realn == nil)
	assert.True(t, row.Textn == nil)
	assert.True(t, row.Blobn == nil)
	assert.True(t, row.Timen == nil)

	mustExec(db, "delete from test where id = ?", lastId)
	assert.Equal(t, db.Changes(), 1)
	assert.Nil(t, queryLast(db))

	mustExec(db, "delete from test where id = ?", lastId)
	assert.Equal(t, db.Changes(), 0)
	assert.Nil(t, queryLast(db))
}

func Test_Conn_BindNil(t *testing.T) {
	db := testDB()
	defer db.Close()

	mustExec(db, `
		insert into test (cintn, crealn, ctextn, cblobn, ctimen)
		values (?1, ?2, ?3, ?4, ?5)
	`, nil, nil, nil, nil, nil)
	assert.Equal(t, db.Changes(), 1)

	row := queryLast(db)
	assert.True(t, row.Intn == nil)
	assert.True(t, row.Realn == nil)
	assert.True(t, row.Textn == nil)
	assert.True(t, row.Blobn == nil)
	assert.True(t, row.Timen == nil)

	var data TestRow
	mustExec(db, `
		insert into test (cintn, crealn, ctextn, cblobn, ctimen)
		values (?1, ?2, ?3, ?4, ?5)
	`, data.Intn, data.Realn, data.Textn, data.Blobn, data.Timen)
	assert.Equal(t, db.Changes(), 1)

	row = queryLast(db)
	assert.True(t, row.Intn == nil)
	assert.True(t, row.Realn == nil)
	assert.True(t, row.Textn == nil)
	assert.True(t, row.Blobn == nil)
	assert.True(t, row.Timen == nil)
}

func Test_Conn_Scan_RawBytes(t *testing.T) {
	db := testDB()
	defer db.Close()

	// can't use db.Row since that closes the stmnt after scanning
	// (which is possibly a design issue)
	rows := db.Rows("select 'a9c', null")
	assert.Equal(t, rows.Next(), true)
	var b1, b2 sqlite.RawBytes
	rows.Scan(&b1, b2)
	assert.Equal(t, len(b1), 3)
	assert.Equal(t, b1[0], 'a')
	assert.Equal(t, b1[1], '9')
	assert.Equal(t, b1[2], 'c')
	assert.True(t, b2 == nil)
	rows.Close()
}

func Test_Bool_True(t *testing.T) {
	db := testDB()
	defer db.Close()
	mustExec(db, `
		insert into test (cint, cintn)
		values (?, ?)
	`, true, true)

	var b1, b2 bool
	row := db.RowB([]byte("select cint, cintn from test where id = ?"), db.LastInsertRowID())

	names := row.Stmt.ColumnNames()
	assert.Equal(t, len(names), 2)
	assert.Equal(t, names[0], "cint")
	assert.Equal(t, names[1], "cintn")

	row.Scan(&b1, &b2)
	assert.Equal(t, b1, true)
	assert.Equal(t, b2, true)
}

func Test_Bool_False(t *testing.T) {
	db := testDB()
	defer db.Close()
	mustExec(db, `
		insert into test (cint, cintn)
		values (?, ?)
	`, false, false)

	var b1, b2 bool
	row := db.RowB([]byte("select cint, cintn from test where id = ?"), db.LastInsertRowID())
	row.Scan(&b1, &b2)
	assert.Equal(t, b1, false)
	assert.Equal(t, b2, false)
}

func Test_Int(t *testing.T) {
	db := testDB()
	defer db.Close()
	mustExec(db, `
		insert into test (cint, cintn)
		values (?, ?)
	`, -9223372036854775808, 9223372036854775807)

	var b1, b2 int
	row := db.RowB([]byte("select cint, cintn from test where id = ?"), db.LastInsertRowID())
	row.Scan(&b1, &b2)
	assert.Equal(t, b1, -9223372036854775808)
	assert.Equal(t, b2, 9223372036854775807)
}

func Test_Int64(t *testing.T) {
	db := testDB()
	defer db.Close()
	mustExec(db, `
		insert into test (cint, cintn)
		values (?, ?)
	`, -9223372036854775808, 9223372036854775807)

	var b1, b2 int64
	row := db.Row("select cint, cintn from test where id = ?", db.LastInsertRowID())
	row.Scan(&b1, &b2)
	assert.Equal(t, b1, -9223372036854775808)
	assert.Equal(t, b2, 9223372036854775807)
}

func Test_Uint(t *testing.T) {
	db := testDB()
	defer db.Close()
	mustExec(db, `
		insert into test (cint, cintn)
		values (?, ?)
	`, 9001, 9002)

	var u16_1, u16_2 uint16
	row := db.Row("select cint, cintn from test where id = ?", db.LastInsertRowID())
	row.Scan(&u16_1, &u16_2)
	assert.Equal(t, u16_1, 9001)
	assert.Equal(t, u16_2, 9002)

	var u32_1, u32_2 uint32
	row = db.Row("select cint, cintn from test where id = ?", db.LastInsertRowID())
	row.Scan(&u32_1, &u32_2)
	assert.Equal(t, u32_1, 9001)
	assert.Equal(t, u32_2, 9002)

	var u64_1, u64_2 uint64
	row = db.Row("select cint, cintn from test where id = ?", db.LastInsertRowID())
	row.Scan(&u64_1, &u64_2)
	assert.Equal(t, u64_1, 9001)
	assert.Equal(t, u64_2, 9002)
}

func Test_String_Empty(t *testing.T) {
	db := testDB()
	defer db.Close()
	mustExec(db, `
		insert into test (ctext)
		values (?1)
	`, "")

	var t1 string
	row := db.Row("select ctextfrom test where id = ?", db.LastInsertRowID())
	row.Scan(&t1)
	assert.Equal(t, t1, "")
}

func Test_Transaction_Commit(t *testing.T) {
	db := testDB()
	defer db.Close()

	var id1 int
	var id2 int

	db.Transaction(func() error {
		mustExec(db, `
			insert into test (ctext)
			values (?)
		`, "hello")

		id1 = db.LastInsertRowID()

		mustExec(db, `
			insert into test (ctextn)
			values (?)
		`, "world")
		id2 = db.LastInsertRowID()

		return nil
	})

	assert.Equal(t, queryId(db, id1).Text, "hello")
	assert.Equal(t, *queryId(db, id2).Textn, "world")
}

func Test_Transaction_Rollback(t *testing.T) {
	db := testDB()
	defer db.Close()

	var id1 int
	var id2 int

	db.Transaction(func() error {
		mustExec(db, `
			insert into test (ctext)
			values (?)
		`, "hello")

		id1 = db.LastInsertRowID()

		mustExec(db, `
			insert into test (ctextn)
			values (?)
		`, "world")
		id2 = db.LastInsertRowID()
		return errors.New("fail")
	})

	assert.Nil(t, queryId(db, id1))
	assert.Nil(t, queryId(db, id2))
}

func Test_Rows(t *testing.T) {
	db := testDB()
	defer db.Close()

	mustExec(db, "delete from test")
	mustExec(db, `
		insert into test (cint, ctext)
		values (?1, ?2), (?3, ?4)
	`, 1, "two", 3, "four")
	assert.Equal(t, db.Changes(), 2)

	rows := db.RowsB([]byte("select cint, ctext from test"))
	defer rows.Close()

	results := make([][]any, 0, 2)
	for rows.Next() {
		var n int
		var t string
		if rows.Scan(&n, &t) != nil {
			break
		}
		results = append(results, []any{n, t})
	}
	assert.Nil(t, rows.Error())
	assert.Equal(t, len(results), 2)
	assert.Equal(t, results[0][0].(int), 1)
	assert.Equal(t, results[0][1].(string), "two")
	assert.Equal(t, results[1][0].(int), 3)
	assert.Equal(t, results[1][1].(string), "four")
}

func Test_Rows_QueryError(t *testing.T) {
	db := testDB()
	defer db.Close()

	rows := db.RowsB([]byte("select invalid from test"))
	defer rows.Close()

	called := 0
	for rows.Next() {
		called += 1
	}
	assert.Equal(t, rows.Error().Error(), "sqlite: no such column: invalid (code: 1) - select invalid from test")
	assert.Equal(t, called, 0)
}

func Test_Rows_ScanError(t *testing.T) {
	db := testDB()
	defer db.Close()

	mustExec(db, "delete from test")
	mustExec(db, `
		insert into test (cint)
		values (?1)
	`, 33)

	rows := db.Rows("select cint from test")
	defer rows.Close()

	called := 0
	for rows.Next() {
		called += 1
		var c os.File
		if rows.Scan(&c) == nil {
			break
		}
	}
	assert.Equal(t, called, 1)
	assert.Equal(t, rows.Error().Error(), "sqlite: cannot scan into *os.File (index: 0) (code: 21)")
}

func Test_Row_Map(t *testing.T) {
	db := testDB()
	defer db.Close()

	now := time.Now()

	mustExec(db, `
		insert into test (id, cint, creal, ctext, cblob, ctime)
		values (?1, ?2, ?3, ?4, ?5, ?6)
	`, 99, 2, 3.3, "four", []byte("five"), now)

	m, err := db.Row("select * from test").Map()
	assert.Nil(t, err)
	assert.Equal(t, len(m), 11)

	assert.Equal(t, m["id"].(int), 99)
	assert.Equal(t, m["cint"].(int), 2)
	assert.Nil(t, m["cintn"])
	assert.Equal(t, m["creal"].(float64), 3.3)
	assert.Nil(t, m["crealn"])
	assert.Equal(t, m["ctext"].(string), "four")
	assert.Nil(t, m["ctextn"])
	assert.Equal(t, string(m["cblob"].([]byte)), "five")
	assert.Nil(t, m["cblobn"])
	assert.Equal(t, m["ctime"].(int), int(now.Unix()))
	assert.Nil(t, m["ctimen"])
}

func testDB() sqlite.Conn {
	db, err := sqlite.Open(":memory:", true)
	if err != nil {
		panic(err)
	}
	mustExec(db, `
		create table test (
			id integer primary key not null,
			cint integer not null default(0),
			cintn integer null,
			creal real not null default(0.0),
			crealn real null,
			ctext text not null default(''),
			ctextn text null,
			cblob blob not null default(''),
			cblobn blob null,
			ctime int not null default(0),
			ctimen int null
		)
	`)
	return db
}

func mustExec(db sqlite.Conn, sql string, args ...interface{}) {
	var err error
	if rand.Intn(2) == 0 {
		err = db.ExecB([]byte(sql), args...)
	} else {
		err = db.Exec(sql, args...)
	}
	if err != nil {
		panic(err)
	}
}

func queryLast(db sqlite.Conn) *TestRow {
	id := db.LastInsertRowID()
	return queryId(db, id)
}

func queryId(db sqlite.Conn, id int) *TestRow {
	var tr TestRow
	row := db.RowB([]byte("select * from test where id = ?"), id)
	err := row.Scan(&tr.Id, &tr.Int, &tr.Intn, &tr.Real, &tr.Realn, &tr.Text, &tr.Textn, &tr.Blob, &tr.Blobn, &tr.Time, &tr.Timen)

	if err == sqlite.ErrNoRows {
		return nil
	}

	if err != nil {
		panic(err)
	}

	return &tr
}
