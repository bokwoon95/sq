package sq

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"flag"
	"strings"
	"testing"
	"time"

	"github.com/bokwoon95/sq/internal/testutil"
)

var (
	postgresDSN  = flag.String("postgres", "", "")
	mysqlDSN     = flag.String("mysql", "", "")
	sqlserverDSN = flag.String("sqlserver", "", "")
)

func TestWritef(t *testing.T) {
	type TT struct {
		ctx        context.Context
		dialect    string
		format     string
		values     []any
		wantQuery  string
		wantArgs   []any
		wantParams map[string][]int
	}

	assert := func(t *testing.T, tt TT) {
		if tt.ctx == nil {
			tt.ctx = context.Background()
		}
		buf := new(bytes.Buffer)
		args := new([]any)
		params := make(map[string][]int)
		err := Writef(tt.ctx, tt.dialect, buf, args, params, tt.format, tt.values)
		if err != nil {
			t.Fatal(testutil.Callers(), err)
		}
		if diff := testutil.Diff(buf.String(), tt.wantQuery); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
		if len(*args) > 0 || len(tt.wantArgs) > 0 {
			if diff := testutil.Diff(*args, tt.wantArgs); diff != "" {
				t.Error(testutil.Callers(), diff)
			}
		}
		if len(params) > 0 || len(tt.wantParams) > 0 {
			if diff := testutil.Diff(params, tt.wantParams); diff != "" {
				t.Error(testutil.Callers(), diff)
			}
		}
	}

	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.format = ""
		tt.values = []any{}
		tt.wantQuery = ""
		tt.wantArgs = []any{}
		assert(t, tt)
	})

	t.Run("escape curly bracket {{", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.format = "SELECT {} = '{{}'"
		tt.values = []any{"{}"}
		tt.wantQuery = `SELECT ? = '{}'`
		tt.wantArgs = []any{"{}"}
		assert(t, tt)
	})

	t.Run("expr", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.format = "(MAX(AVG({one}), AVG({two}), SUM({three})) + {incr}) IN ({slice})"
		tt.values = []any{
			sql.Named("one", tmpfield("user_id")),
			sql.Named("two", tmpfield("age")),
			sql.Named("three", tmpfield("age")),
			sql.Named("incr", 1),
			sql.Named("slice", []int{1, 2, 3}),
		}
		tt.wantQuery = "(MAX(AVG(user_id), AVG(age), SUM(age)) + ?) IN (?, ?, ?)"
		tt.wantArgs = []any{1, 1, 2, 3}
		tt.wantParams = map[string][]int{"incr": {0}}
		assert(t, tt)
	})

	t.Run("Field slice expansion", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.format = "SELECT {} FROM {}"
		tt.values = []any{
			[]Field{
				tmpfield("111.aaa"),
				tmpfield("222.bbb"),
				tmpfield("333.ccc"),
			},
			tmptable("public.222"),
		}
		tt.wantQuery = `SELECT "111".aaa, "222".bbb, "333".ccc FROM public."222"`
		assert(t, tt)
	})

	t.Run("params", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.format = "{param}, {param}" +
			", {array}, {array}" +
			", {bytes}, {bytes}" +
			", {bool}, {bool}" +
			", {enum}, {enum}" +
			", {json}, {json}" +
			", {int}, {int}" +
			", {int64}, {float64}" +
			", {string}, {string}" +
			", {time}, {time}" +
			", {uuid}, {uuid}"
		tt.values = []any{
			Param("param", nil),
			ArrayParam("array", []int{1, 2, 3}),
			BytesParam("bytes", []byte{0xFF, 0xFF, 0xFF}),
			BoolParam("bool", true),
			EnumParam("enum", Monday),
			JSONParam("json", map[string]string{"lorem": "ipsum"}),
			IntParam("int", 5),
			Int64Param("int64", 7),
			Float64Param("float64", 11.0),
			StringParam("string", "lorem ipsum"),
			TimeParam("time", time.Unix(0, 0)),
			UUIDParam("uuid", [16]byte{0xa4, 0xf9, 0x52, 0xf1, 0x4c, 0x45, 0x4e, 0x63, 0xbd, 0x4e, 0x15, 0x9c, 0xa3, 0x3c, 0x8e, 0x20}),
		}
		tt.wantQuery = "?, ?" +
			", ?, ?" +
			", ?, ?" +
			", ?, ?" +
			", ?, ?" +
			", ?, ?" +
			", ?, ?" +
			", ?, ?" +
			", ?, ?" +
			", ?, ?" +
			", ?, ?"
		tt.wantArgs = []any{
			nil, nil,
			"[1,2,3]", "[1,2,3]",
			[]byte{0xFF, 0xFF, 0xFF}, []byte{0xFF, 0xFF, 0xFF},
			true, true,
			"Monday", "Monday",
			`{"lorem":"ipsum"}`, `{"lorem":"ipsum"}`,
			5, 5,
			int64(7), float64(11.0),
			"lorem ipsum", "lorem ipsum",
			time.Unix(0, 0), time.Unix(0, 0),
			[]byte{0xa4, 0xf9, 0x52, 0xf1, 0x4c, 0x45, 0x4e, 0x63, 0xbd, 0x4e, 0x15, 0x9c, 0xa3, 0x3c, 0x8e, 0x20},
			[]byte{0xa4, 0xf9, 0x52, 0xf1, 0x4c, 0x45, 0x4e, 0x63, 0xbd, 0x4e, 0x15, 0x9c, 0xa3, 0x3c, 0x8e, 0x20},
		}
		tt.wantParams = map[string][]int{
			"param":   {0, 1},
			"array":   {2, 3},
			"bytes":   {4, 5},
			"bool":    {6, 7},
			"enum":    {8, 9},
			"json":    {10, 11},
			"int":     {12, 13},
			"int64":   {14},
			"float64": {15},
			"string":  {16, 17},
			"time":    {18, 19},
			"uuid":    {20, 21},
		}
		assert(t, tt)
	})

	t.Run("duplicate params should error", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.format = "{param}, {param}"
		tt.values = []any{
			Param("param", 1),
			Param("param", 1),
		}
		var buf bytes.Buffer
		var args []any
		params := make(map[string][]int)
		format := "{param}, {param}"
		values := []any{
			Param("param", 1),
			Param("param", 1),
		}
		err := Writef(context.Background(), "", &buf, &args, params, format, values)
		if err == nil {
			t.Errorf(testutil.Callers() + " expected error but got nil")
		}
	})

	t.Run("sqlite,postgres QuoteIdentifier", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = DialectSQLite
		tt.format = "SELECT {}"
		tt.values = []any{
			tmpfield(`"; ""; DROP TABLE users --`),
		}
		tt.wantQuery = `SELECT """; ""; DROP TABLE users --"`
		assert(t, tt)

		tt.dialect = DialectPostgres
		assert(t, tt)
	})

	t.Run("sqlite,postgres anonymous params", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = DialectSQLite
		tt.format = "SELECT {}" +
			" FROM {}" +
			" WHERE {} = {}" +
			" AND {} <> {}" +
			" AND {} IN ({})"
		tt.values = []any{
			tmpfield("name"),
			tmptable("users"),
			tmpfield("age"), 5,
			tmpfield("email"), "bob@email.com",
			tmpfield("name"), []string{"tom", "dick", "harry"},
		}
		tt.wantQuery = "SELECT name" +
			" FROM users" +
			" WHERE age = $1" +
			" AND email <> $2" +
			" AND name IN ($3, $4, $5)"
		tt.wantArgs = []any{5, "bob@email.com", "tom", "dick", "harry"}
		assert(t, tt)

		tt.dialect = DialectPostgres
		assert(t, tt)
	})

	t.Run("sqlite,postgres ordinal params", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = DialectSQLite
		tt.format = "SELECT {}" +
			" FROM {}" +
			" WHERE {} = {5}" +
			" AND {} <> {5}" +
			" AND {1} IN ({6})" +
			" AND {4} IN ({6})"
		tt.values = []any{
			tmpfield("name"),
			tmptable("users"),
			tmpfield("age"),
			tmpfield("email"),
			"bob@email.com",
			[]string{"tom", "dick", "harry"},
		}
		tt.wantQuery = "SELECT name" +
			" FROM users" +
			" WHERE age = $1" +
			" AND email <> $1" +
			" AND name IN ($2, $3, $4)" +
			" AND email IN ($5, $6, $7)"
		tt.wantArgs = []any{
			"bob@email.com",
			"tom", "dick", "harry",
			"tom", "dick", "harry",
		}
		assert(t, tt)

		tt.dialect = DialectPostgres
		assert(t, tt)
	})

	t.Run("sqlite named params", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = DialectSQLite
		tt.format = "SELECT {}" +
			" FROM {}" +
			" WHERE {3} = {age}" +
			" AND {3} > {6}" +
			" AND {4} <> {email}" +
			" AND {1} IN ({names})" +
			" AND {4} IN ({names})"
		tt.values = []any{
			tmpfield("name"),
			tmptable("users"),
			tmpfield("age"),
			tmpfield("email"),
			sql.Named("email", "bob@email.com"),
			sql.Named("age", 5),
			sql.Named("names", []string{"tom", "dick", "harry"}),
		}
		tt.wantQuery = "SELECT name" +
			" FROM users" +
			" WHERE age = $age" +
			" AND age > $age" +
			" AND email <> $email" +
			" AND name IN ($3, $4, $5)" +
			" AND email IN ($6, $7, $8)"
		tt.wantArgs = []any{
			sql.Named("age", 5),
			sql.Named("email", "bob@email.com"),
			"tom", "dick", "harry",
			"tom", "dick", "harry",
		}
		tt.wantParams = map[string][]int{"age": {0}, "email": {1}}
		assert(t, tt)
	})

	t.Run("postgres named params", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = DialectPostgres
		tt.format = "SELECT {}" +
			" FROM {}" +
			" WHERE {3} = {age}" +
			" AND {3} > {6}" +
			" AND {4} <> {email}" +
			" AND {1} IN ({names})" +
			" AND {4} IN ({names})"
		tt.values = []any{
			tmpfield("name"),
			tmptable("users"),
			tmpfield("age"),
			tmpfield("email"),
			sql.Named("email", "bob@email.com"),
			sql.Named("age", 5),
			sql.Named("names", []string{"tom", "dick", "harry"}),
		}
		tt.wantQuery = "SELECT name" +
			" FROM users" +
			" WHERE age = $1" +
			" AND age > $1" +
			" AND email <> $2" +
			" AND name IN ($3, $4, $5)" +
			" AND email IN ($6, $7, $8)"
		tt.wantArgs = []any{
			5,
			"bob@email.com",
			"tom", "dick", "harry",
			"tom", "dick", "harry",
		}
		tt.wantParams = map[string][]int{"age": {0}, "email": {1}}
		assert(t, tt)
	})

	t.Run("sqlite,postgres SQLWriter in named param", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = DialectSQLite
		tt.format = "SELECT {field} FROM {tbl} WHERE {field} IN ({nums})"
		tt.values = []any{
			sql.Named("nums", []int{1, 2, 3}),
			sql.Named("tbl", tmptable("public.tbl")),
			sql.Named("field", tmpfield("tbl.field")),
		}
		tt.wantQuery = `SELECT tbl.field FROM public.tbl WHERE tbl.field IN ($1, $2, $3)`
		tt.wantArgs = []any{1, 2, 3}
		assert(t, tt)

		tt.dialect = DialectPostgres
		assert(t, tt)
	})

	t.Run("mysql QuoteIdentifier", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = DialectMySQL
		tt.format = "SELECT {}"
		tt.values = []any{
			tmpfield("`; ``; DROP TABLE users --"),
		}
		tt.wantQuery = "SELECT ```; ``; DROP TABLE users --`"
		assert(t, tt)
	})

	t.Run("mysql anonymous params", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = DialectMySQL
		tt.format = "SELECT {}" +
			" FROM {}" +
			" WHERE {} = {}" +
			" AND {} <> {}" +
			" AND {} IN ({})"
		tt.values = []any{
			tmpfield("name"),
			tmptable("users"),
			tmpfield("age"), 5,
			tmpfield("email"), "bob@email.com",
			tmpfield("name"), []string{"tom", "dick", "harry"},
		}
		tt.wantQuery = "SELECT name" +
			" FROM users" +
			" WHERE age = ?" +
			" AND email <> ?" +
			" AND name IN (?, ?, ?)"
		tt.wantArgs = []any{5, "bob@email.com", "tom", "dick", "harry"}
		assert(t, tt)
	})

	t.Run("mysql ordinal params", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = DialectMySQL
		tt.format = "SELECT {}" +
			" FROM {}" +
			" WHERE {} = {5}" +
			" AND {} <> {5}" +
			" AND {1} IN ({6})" +
			" AND {4} IN ({6})"
		tt.values = []any{
			tmpfield("name"),
			tmptable("users"),
			tmpfield("age"),
			tmpfield("email"),
			"bob@email.com",
			[]string{"tom", "dick", "harry"},
		}
		tt.wantQuery = "SELECT name" +
			" FROM users" +
			" WHERE age = ?" +
			" AND email <> ?" +
			" AND name IN (?, ?, ?)" +
			" AND email IN (?, ?, ?)"
		tt.wantArgs = []any{
			"bob@email.com", "bob@email.com",
			"tom", "dick", "harry",
			"tom", "dick", "harry",
		}
		assert(t, tt)
	})

	t.Run("mysql named params", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = DialectMySQL
		tt.format = "SELECT {}" +
			" FROM {}" +
			" WHERE {3} = {age}" +
			" AND {3} > {6}" +
			" AND {4} <> {email}" +
			" AND {1} IN ({names})" +
			" AND {4} IN ({names})"
		tt.values = []any{
			tmpfield("name"),
			tmptable("users"),
			tmpfield("age"),
			tmpfield("email"),
			sql.Named("email", "bob@email.com"),
			sql.Named("age", 5),
			sql.Named("names", []string{"tom", "dick", "harry"}),
		}
		tt.wantQuery = "SELECT name" +
			" FROM users" +
			" WHERE age = ?" +
			" AND age > ?" +
			" AND email <> ?" +
			" AND name IN (?, ?, ?)" +
			" AND email IN (?, ?, ?)"
		tt.wantArgs = []any{
			5, 5,
			"bob@email.com",
			"tom", "dick", "harry",
			"tom", "dick", "harry",
		}
		tt.wantParams = map[string][]int{"age": {0, 1}, "email": {2}}
		assert(t, tt)
	})

	t.Run("mysql SQLWriter in named param", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = DialectMySQL
		tt.format = "SELECT {field} FROM {tbl} WHERE {field} IN ({nums})"
		tt.values = []any{
			sql.Named("nums", []int{1, 2, 3}),
			sql.Named("tbl", tmptable("public.tbl")),
			sql.Named("field", tmpfield("tbl.field")),
		}
		tt.wantQuery = `SELECT tbl.field FROM public.tbl WHERE tbl.field IN (?, ?, ?)`
		tt.wantArgs = []any{1, 2, 3}
		assert(t, tt)
	})

	t.Run("sqlserver QuoteIdentifier", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = DialectSQLServer
		tt.format = "SELECT {}"
		tt.values = []any{
			tmpfield("]; ]]; DROP TABLE users --"),
		}
		tt.wantQuery = "SELECT []]; ]]; DROP TABLE users --]"
		assert(t, tt)
	})

	t.Run("sqlserver anonymous params", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = DialectSQLServer
		tt.format = "SELECT {}" +
			" FROM {}" +
			" WHERE {} = {}" +
			" AND {} <> {}" +
			" AND {} IN ({})"
		tt.values = []any{
			tmpfield("name"),
			tmptable("users"),
			tmpfield("age"), 5,
			tmpfield("email"), "bob@email.com",
			tmpfield("name"), []string{"tom", "dick", "harry"},
		}
		tt.wantQuery = "SELECT name" +
			" FROM users" +
			" WHERE age = @p1" +
			" AND email <> @p2" +
			" AND name IN (@p3, @p4, @p5)"
		tt.wantArgs = []any{5, "bob@email.com", "tom", "dick", "harry"}
		assert(t, tt)
	})

	t.Run("sqlserver ordinal params", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = DialectSQLServer
		tt.format = "SELECT {}" +
			" FROM {}" +
			" WHERE {} = {5}" +
			" AND {} <> {5}" +
			" AND {1} IN ({6})" +
			" AND {4} IN ({6})"
		tt.values = []any{
			tmpfield("name"),
			tmptable("users"),
			tmpfield("age"),
			tmpfield("email"),
			"bob@email.com",
			[]string{"tom", "dick", "harry"},
		}
		tt.wantQuery = "SELECT name" +
			" FROM users" +
			" WHERE age = @p1" +
			" AND email <> @p1" +
			" AND name IN (@p2, @p3, @p4)" +
			" AND email IN (@p5, @p6, @p7)"
		tt.wantArgs = []any{
			"bob@email.com",
			"tom", "dick", "harry",
			"tom", "dick", "harry",
		}
		assert(t, tt)
	})

	t.Run("sqlserver named params", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = DialectSQLServer
		tt.format = "SELECT {}" +
			" FROM {}" +
			" WHERE {3} = {age}" +
			" AND {3} > {6}" +
			" AND {4} <> {email}" +
			" AND {1} IN ({names})" +
			" AND {4} IN ({names})"
		tt.values = []any{
			tmpfield("name"),
			tmptable("users"),
			tmpfield("age"),
			tmpfield("email"),
			sql.Named("email", "bob@email.com"),
			sql.Named("age", 5),
			sql.Named("names", []string{"tom", "dick", "harry"}),
		}
		tt.wantQuery = "SELECT name" +
			" FROM users" +
			" WHERE age = @age" +
			" AND age > @age" +
			" AND email <> @email" +
			" AND name IN (@p3, @p4, @p5)" +
			" AND email IN (@p6, @p7, @p8)"
		tt.wantArgs = []any{
			sql.Named("age", 5),
			sql.Named("email", "bob@email.com"),
			"tom", "dick", "harry",
			"tom", "dick", "harry",
		}
		tt.wantParams = map[string][]int{"age": {0}, "email": {1}}
		assert(t, tt)
	})

	t.Run("sqlserver SQLWriter in named param", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = DialectSQLServer
		tt.format = "SELECT {field} FROM {tbl} WHERE {field} IN ({nums})"
		tt.values = []any{
			sql.Named("nums", []int{1, 2, 3}),
			sql.Named("tbl", tmptable("dbo.tbl")),
			sql.Named("field", tmpfield("tbl.field")),
		}
		tt.wantQuery = `SELECT tbl.field FROM dbo.tbl WHERE tbl.field IN (@p1, @p2, @p3)`
		tt.wantArgs = []any{1, 2, 3}
		assert(t, tt)
	})

	t.Run("preprocessValue kicks in for anonymous, ordinal params, named params and slices", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = DialectSQLite
		tt.format = "SELECT {}, {2}, {foo}, {3}, {bar}"
		tt.values = []any{
			Monday,
			sql.Named("foo", Tuesday),
			Wednesday,
			sql.Named("bar", []Weekday{Thursday, Friday, Saturday}),
		}
		tt.wantQuery = "SELECT $1, $foo, $foo, $3, $4, $5, $6"
		tt.wantArgs = []any{
			"Monday",
			sql.NamedArg{Name: "foo", Value: "Tuesday"},
			"Wednesday",
			"Thursday",
			"Friday",
			"Saturday",
		}
		tt.wantParams = map[string][]int{"foo": {1}}
		assert(t, tt)
	})

	t.Run("no closing curly brace }", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.format = "SELECT {field"
		buf := new(bytes.Buffer)
		args := new([]any)
		params := make(map[string][]int)
		err := Writef(tt.ctx, tt.dialect, buf, args, params, tt.format, tt.values)
		if err == nil {
			t.Error(testutil.Callers(), "expected error but got nil")
		}
	})

	t.Run("too few values passed in", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.format = "SELECT {}, {}, {}, {}"
		tt.values = []any{1, 2}
		buf := new(bytes.Buffer)
		args := new([]any)
		params := make(map[string][]int)
		err := Writef(tt.ctx, tt.dialect, buf, args, params, tt.format, tt.values)
		if err == nil {
			t.Error(testutil.Callers(), "expected error but got nil")
		}
	})

	t.Run("anonymous param faulty SQL", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.format = "SELECT {}"
		tt.values = []any{FaultySQL{}}
		buf := new(bytes.Buffer)
		args := new([]any)
		params := make(map[string][]int)
		err := Writef(tt.ctx, tt.dialect, buf, args, params, tt.format, tt.values)
		if !errors.Is(err, ErrFaultySQL) {
			t.Error(testutil.Callers(), "expected ErrFaultySQL but got %v", err)
		}
	})

	t.Run("ordinal param faulty SQL", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.format = "SELECT {1}"
		tt.values = []any{FaultySQL{}}
		buf := new(bytes.Buffer)
		args := new([]any)
		params := make(map[string][]int)
		err := Writef(tt.ctx, tt.dialect, buf, args, params, tt.format, tt.values)
		if !errors.Is(err, ErrFaultySQL) {
			t.Error(testutil.Callers(), "expected ErrFaultySQL but got %v", err)
		}
	})

	t.Run("named param faulty SQL", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.format = "SELECT {field}"
		tt.values = []any{sql.Named("field", FaultySQL{})}
		buf := new(bytes.Buffer)
		args := new([]any)
		params := make(map[string][]int)
		err := Writef(tt.ctx, tt.dialect, buf, args, params, tt.format, tt.values)
		if !errors.Is(err, ErrFaultySQL) {
			t.Error(testutil.Callers(), "expected ErrFaultySQL but got %v", err)
		}
	})

	t.Run("ordinal param out of bounds", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.format = "SELECT {1}, {2}, {99}"
		tt.values = []any{1, 2, 3}
		buf := new(bytes.Buffer)
		args := new([]any)
		params := make(map[string][]int)
		err := Writef(tt.ctx, tt.dialect, buf, args, params, tt.format, tt.values)
		if err == nil {
			t.Error(testutil.Callers(), "expected error but got nil")
		}
	})

	t.Run("nonexistent named param", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.format = "SELECT {A}, {B}, {C}"
		tt.values = []any{
			sql.Named("A", 1),
			sql.Named("B", 2),
			sql.Named("E", 5),
		}
		buf := new(bytes.Buffer)
		args := new([]any)
		params := make(map[string][]int)
		err := Writef(tt.ctx, tt.dialect, buf, args, params, tt.format, tt.values)
		if err == nil {
			t.Error(testutil.Callers(), "expected error but got nil")
		}
	})

	t.Run("expandSlice faulty SQL", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.format = "SELECT {}"
		tt.values = []any{
			[]Field{tmpfield("name"), tmpfield("age"), FaultySQL{}},
		}
		buf := new(bytes.Buffer)
		args := new([]any)
		params := make(map[string][]int)
		err := Writef(tt.ctx, tt.dialect, buf, args, params, tt.format, tt.values)
		if !errors.Is(err, ErrFaultySQL) {
			t.Error(testutil.Callers(), "expected ErrFaultySQL but got %v", err)
		}
	})
}

func TestSprintf(t *testing.T) {
	type TT struct {
		dialect    string
		query      string
		args       []any
		wantString string
	}

	assert := func(t *testing.T, tt TT) {
		gotString, err := Sprintf(tt.dialect, tt.query, tt.args)
		if err != nil {
			t.Fatal(testutil.Callers(), err)
		}
		if diff := testutil.Diff(gotString, tt.wantString); diff != "" {
			t.Error(testutil.Callers(), diff)
		}
	}

	assertNotOK := func(t *testing.T, tt TT) {
		_, err := Sprintf(tt.dialect, tt.query, tt.args)
		if err == nil {
			t.Fatal(testutil.Callers(), "expected error but got nil")
		}
	}

	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = ""
		tt.query = ""
		tt.args = []any{}
		tt.wantString = ""
		assert(t, tt)
	})

	t.Run("insideString, insideIdentifier and escaping single quotes", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = ""
		tt.query = `SELECT ?` +
			`, 'do not "rebind" ? ? ?'` + // string
			`, "do not 'rebind' ? ? ?"` + // identifier
			`, ?` +
			`, ?`
		tt.args = []any{
			"normal string",
			"string with 'quotes' must be escaped",
			"string with already escaped ''quotes'' except for 'this'",
		}
		tt.wantString = `SELECT 'normal string'` +
			`, 'do not "rebind" ? ? ?'` +
			`, "do not 'rebind' ? ? ?"` +
			`, 'string with ''quotes'' must be escaped'` +
			`, 'string with already escaped ''''quotes'''' except for ''this'''`
		assert(t, tt)
	})

	t.Run("insideString, insideIdentifier and escaping single quotes (dialect == mysql)", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = DialectMySQL
		tt.query = `SELECT ?` +
			`, 'do not "rebind" ? ? ?'` + // string
			", `do not \" 'rebind' ? ? ?`" + // identifier
			", \"do not ``` 'rebind' ? ? ?\"" + // identifier
			`, ?` +
			`, ?`
		tt.args = []any{
			"normal string",
			"string with 'quotes' must be escaped",
			"string with already escaped ''quotes'' except for 'this'",
		}
		tt.wantString = `SELECT 'normal string'` +
			`, 'do not "rebind" ? ? ?'` +
			", `do not \" 'rebind' ? ? ?`" +
			", \"do not ``` 'rebind' ? ? ?\"" +
			`, 'string with ''quotes'' must be escaped'` +
			`, 'string with already escaped ''''quotes'''' except for ''this'''`
		assert(t, tt)
	})

	t.Run("insideString, insideIdentifier and escaping single quotes (dialect == sqlserver)", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = DialectSQLServer
		tt.query = `SELECT ?` +
			`, 'do not [[rebind] @p1 @p2 @name'` + // string
			", [do not \" 'rebind' [[[[[@pp]] @p3 @p1]" + // identifier
			", \"do not [[[ 'rebind' [[[[[@pp]] @p3 @p1\"" + // identifier
			`, ?` +
			`, @p3`
		tt.args = []any{
			"normal string",
			"string with 'quotes' must be escaped",
			"string with already escaped ''quotes'' except for 'this'",
		}
		tt.wantString = `SELECT 'normal string'` +
			`, 'do not [[rebind] @p1 @p2 @name'` +
			", [do not \" 'rebind' [[[[[@pp]] @p3 @p1]" +
			", \"do not [[[ 'rebind' [[[[[@pp]] @p3 @p1\"" +
			`, 'string with ''quotes'' must be escaped'` +
			`, 'string with already escaped ''''quotes'''' except for ''this'''`
		assert(t, tt)
	})

	t.Run("mysql", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = DialectMySQL
		tt.query = "SELECT name FROM users WHERE age = ? AND email <> ? AND name IN (?, ?, ?)"
		tt.args = []any{5, "bob@email.com", "tom", "dick", "harry"}
		tt.wantString = "SELECT name FROM users WHERE age = 5 AND email <> 'bob@email.com' AND name IN ('tom', 'dick', 'harry')"
		assert(t, tt)
	})

	t.Run("mysql insideString", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = DialectMySQL
		tt.query = "SELECT name FROM users WHERE age = ? AND email <> '? ? ? ? ''bruh ?' AND name IN (?, ?) ?"
		tt.args = []any{5, "tom", "dick", "harry"}
		tt.wantString = "SELECT name FROM users WHERE age = 5 AND email <> '? ? ? ? ''bruh ?' AND name IN ('tom', 'dick') 'harry'"
		assert(t, tt)
	})

	t.Run("omitted dialect insideString", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = ""
		tt.query = "SELECT name FROM users WHERE age = ? AND email <> '? ? ? ? ''bruh ?' AND name IN (?, ?) ?"
		tt.args = []any{5, "tom", "dick", "harry"}
		tt.wantString = "SELECT name FROM users WHERE age = 5 AND email <> '? ? ? ? ''bruh ?' AND name IN ('tom', 'dick') 'harry'"
		assert(t, tt)
	})

	t.Run("postgres", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = DialectPostgres
		tt.query = "SELECT name FROM users WHERE age = $1 AND email <> $2 AND name IN ($2, $3, $4, $1)"
		tt.args = []any{5, "tom", "dick", "harry"}
		tt.wantString = "SELECT name FROM users WHERE age = 5 AND email <> 'tom' AND name IN ('tom', 'dick', 'harry', 5)"
		assert(t, tt)
	})

	t.Run("postgres insideString", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = DialectPostgres
		tt.query = "SELECT name FROM users WHERE age = $1 AND email <> '$2 $2 $3 $4 ''bruh $1' AND name IN ($2, $3) $4"
		tt.args = []any{5, "tom", "dick", "harry"}
		tt.wantString = "SELECT name FROM users WHERE age = 5 AND email <> '$2 $2 $3 $4 ''bruh $1' AND name IN ('tom', 'dick') 'harry'"
		assert(t, tt)
	})

	t.Run("sqlite", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = DialectSQLite
		tt.query = "SELECT name FROM users WHERE age = $1 AND email <> $2 AND name IN ($2, $3, $4, $1)"
		tt.args = []any{5, "tom", "dick", "harry"}
		tt.wantString = "SELECT name FROM users WHERE age = 5 AND email <> 'tom' AND name IN ('tom', 'dick', 'harry', 5)"
		assert(t, tt)
	})

	t.Run("sqlite insideString", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = DialectSQLite
		tt.query = "SELECT name FROM users WHERE age = $1 AND email <> '$2 $2 $3 $4 ''bruh $1' AND name IN ($2, $3) $4"
		tt.args = []any{5, "tom", "dick", "harry"}
		tt.wantString = "SELECT name FROM users WHERE age = 5 AND email <> '$2 $2 $3 $4 ''bruh $1' AND name IN ('tom', 'dick') 'harry'"
		assert(t, tt)
	})

	t.Run("sqlite mixing ordinal param and named param", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = DialectSQLite
		tt.query = "SELECT name FROM users WHERE age = $age AND age > $1 AND email <> $email"
		tt.args = []any{sql.Named("age", 5), sql.Named("email", "bob@email.com")}
		tt.wantString = "SELECT name FROM users WHERE age = 5 AND age > 5 AND email <> 'bob@email.com'"
		assert(t, tt)
	})

	t.Run("sqlite supports everything", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = DialectSQLite
		tt.query = "SELECT name FROM users WHERE age = ?age AND email <> :email AND name IN (@3, ?4, $5, :5) ? ?"
		tt.args = []any{sql.Named("age", 5), sql.Named("email", "bob@email.com"), "tom", "dick", "harry"}
		tt.wantString = "SELECT name FROM users WHERE age = 5 AND email <> 'bob@email.com' AND name IN ('tom', 'dick', 'harry', 'harry') 5 'bob@email.com'"
		assert(t, tt)
	})

	t.Run("sqlserver", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = DialectSQLServer
		tt.query = "SELECT name FROM users WHERE age = @p1 AND email <> @P2 AND name IN (@p2, @p3, @p4, @P1)"
		tt.args = []any{5, "tom", "dick", "harry"}
		tt.wantString = "SELECT name FROM users WHERE age = 5 AND email <> 'tom' AND name IN ('tom', 'dick', 'harry', 5)"
		assert(t, tt)
	})

	t.Run("sqlserver insideString", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = DialectSQLServer
		tt.query = "SELECT name FROM users WHERE age = @p1 AND email <> '@p2 @p2 @p3 @p4 ''bruh @p1' AND name IN (@p2, @p3) @p4"
		tt.args = []any{5, "tom", "dick", "harry"}
		tt.wantString = "SELECT name FROM users WHERE age = 5 AND email <> '@p2 @p2 @p3 @p4 ''bruh @p1' AND name IN ('tom', 'dick') 'harry'"
		assert(t, tt)
	})

	t.Run("sqlserver mixing ordinal param and named param", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = DialectSQLServer
		tt.query = "SELECT name FROM users WHERE age = @age AND age > @p1 AND email <> @email"
		tt.args = []any{sql.Named("age", 5), sql.Named("email", "bob@email.com")}
		tt.wantString = "SELECT name FROM users WHERE age = 5 AND age > 5 AND email <> 'bob@email.com'"
		assert(t, tt)
	})

	t.Run("unclosed string and identifier", func(t *testing.T) {
		t.Parallel()
		var tt TT
		// unclosed string
		tt.query = `SELECT ?, 'mary had a little', 'lamb`
		tt.args = []any{1}
		assertNotOK(t, tt)

		// unclosed identifier
		tt.query = `SELECT ?, "one", "two", "three`
		tt.args = []any{2}
		assertNotOK(t, tt)
	})

	t.Run("sqlite invalid anonymous param", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = DialectSQLite
		tt.args = []any{23}
		tt.wantString = "SELECT 23"

		// ?1 is valid
		tt.query = "SELECT ?1"
		assert(t, tt)

		// ? is valid
		tt.query = "SELECT ?"
		assert(t, tt)

		// $1 is valid
		tt.query = "SELECT $1"
		assert(t, tt)

		// $ is invalid
		tt.query = "SELECT $"
		assertNotOK(t, tt)
	})

	t.Run("not enough params", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.query = "SELECT ?, ?, ?"
		tt.args = []any{1, 2}
		assertNotOK(t, tt)
	})

	t.Run("functions cannot be printed", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.query = "SELECT ?, ?"
		tt.args = []any{1, func() {}}
		assertNotOK(t, tt)
	})

	t.Run("channels cannot be printed", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.query = "SELECT ?, ?"
		tt.args = []any{make(chan int), 2}
		assertNotOK(t, tt)
	})

	t.Run("non driver.Valuer types cannot be printed", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.query = "SELECT ?, ?"
		tt.args = []any{struct{}{}, any(nil)}
		assertNotOK(t, tt)
	})

	t.Run("ordinal param out of bounds", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = DialectSQLite
		tt.query = "SELECT @1, @2, @3"
		tt.args = []any{1, 2}
		assertNotOK(t, tt)
	})

	t.Run("dialect that does not support sql.NamedArg", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = DialectPostgres
		tt.query = "SELECT $test"
		tt.args = []any{sql.Named("test", 123)}
		assertNotOK(t, tt)
	})

	t.Run("sql.NamedArg not provided", func(t *testing.T) {
		t.Parallel()
		var tt TT
		tt.dialect = DialectSQLite
		tt.query = "SELECT :one, :two, :three"
		tt.args = []any{
			sql.Named("one", 1),
			sql.Named("two", 2),
			sql.Named("four", 4),
		}
		assertNotOK(t, tt)
	})
}

func TestSprint(t *testing.T) {
	type TT struct {
		description string
		dialect     string
		value       any
		wantString  string
	}
	singaporeLocation, _ := time.LoadLocation("Asia/Singapore")

	tests := []TT{{
		description: "nil",
		value:       nil,
		wantString:  "NULL",
	}, {
		description: "true",
		value:       true,
		wantString:  "TRUE",
	}, {
		description: "false",
		value:       false,
		wantString:  "FALSE",
	}, {
		description: "sqlserver true",
		dialect:     DialectSQLServer,
		value:       true,
		wantString:  "1",
	}, {
		description: "sqlserver false",
		dialect:     DialectSQLServer,
		value:       false,
		wantString:  "0",
	}, {
		description: "postgres []byte",
		dialect:     DialectPostgres,
		value:       []byte{0xff, 0xff},
		wantString:  `'\xffff'`,
	}, {
		description: "[]byte",
		value:       []byte{0xff, 0xff},
		wantString:  `x'ffff'`,
	}, {
		description: "string",
		value:       "' OR ''test' = '; DROP TABLE users; -- ",
		wantString:  `''' OR ''''test'' = ''; DROP TABLE users; -- '`,
	}, {
		description: "time.Time",
		value:       time.Unix(0, 0).UTC(),
		wantString:  `'1970-01-01 00:00:00'`,
	}, {
		description: "time.Time (SQLServer)",
		dialect:     DialectSQLServer,
		value:       time.Unix(0, 0).UTC(),
		wantString:  `'1970-01-01 00:00:00+00:00'`,
	}, {
		description: "int",
		value:       int(0),
		wantString:  `0`,
	}, {
		description: "int8",
		value:       int8(8),
		wantString:  `8`,
	}, {
		description: "int16",
		value:       int16(16),
		wantString:  `16`,
	}, {
		description: "int32",
		value:       int32(32),
		wantString:  `32`,
	}, {
		description: "int64",
		value:       int64(64),
		wantString:  `64`,
	}, {
		description: "uint",
		value:       uint(0),
		wantString:  `0`,
	}, {
		description: "uint8",
		value:       uint8(8),
		wantString:  `8`,
	}, {
		description: "uint16",
		value:       uint16(16),
		wantString:  `16`,
	}, {
		description: "uint32",
		value:       uint32(32),
		wantString:  `32`,
	}, {
		description: "uint64",
		value:       uint64(64),
		wantString:  `64`,
	}, {
		description: "float32",
		value:       float32(32.32),
		wantString:  `32.31999969482422`,
	}, {
		description: "float64",
		value:       float64(64.6464),
		wantString:  `64.6464`,
	}, {
		description: "sql.NamedArg",
		value:       sql.Named("test", 7),
		wantString:  `7`,
	}, {
		description: "sql.NullBool NULL",
		value:       sql.NullBool{},
		wantString:  `NULL`,
	}, {
		description: "sql.NullBool true",
		value:       sql.NullBool{Valid: true, Bool: true},
		wantString:  `TRUE`,
	}, {
		description: "sql.NullBool false",
		value:       sql.NullBool{Valid: true, Bool: false},
		wantString:  `FALSE`,
	}, {
		description: "sqlserver sql.NullBool NULL",
		dialect:     DialectSQLServer,
		value:       sql.NullBool{},
		wantString:  `NULL`,
	}, {
		description: "sqlserver sql.NullBool true",
		dialect:     DialectSQLServer,
		value:       sql.NullBool{Valid: true, Bool: true},
		wantString:  `1`,
	}, {
		description: "sqlserver sql.NullBool false",
		dialect:     DialectSQLServer,
		value:       sql.NullBool{Valid: true, Bool: false},
		wantString:  `0`,
	}, {
		description: "sql.NullFloat64 NULL",
		value:       sql.NullFloat64{},
		wantString:  `NULL`,
	}, {
		description: "sql.NullFloat64",
		value:       sql.NullFloat64{Valid: true, Float64: 3.0},
		wantString:  `3`,
	}, {
		description: "sql.NullInt64Field NULL",
		value:       sql.NullInt64{},
		wantString:  `NULL`,
	}, {
		description: "sql.NullInt64Field",
		value:       sql.NullInt64{Valid: true, Int64: 5},
		wantString:  `5`,
	}, {
		description: "sql.NullInt32 NULL",
		value:       sql.NullInt32{},
		wantString:  `NULL`,
	}, {
		description: "sql.NullInt32",
		value:       sql.NullInt32{Valid: true, Int32: 7},
		wantString:  `7`,
	}, {
		description: "sql.NullStringField NULL",
		value:       sql.NullString{},
		wantString:  `NULL`,
	}, {
		description: "sql.NullStringField",
		value:       sql.NullString{Valid: true, String: "pp"},
		wantString:  `'pp'`,
	}, {
		description: "sql.NullTimeField NULL",
		value:       sql.NullTime{},
		wantString:  `NULL`,
	}, {
		description: "sql.NullTime",
		value: sql.NullTime{
			Valid: true,
			Time:  time.Unix(0, 0).UTC(),
		},
		wantString: `'1970-01-01 00:00:00'`,
	}, {
		description: "sql.NullTime (Postgres)",
		dialect:     DialectPostgres,
		value: sql.NullTime{
			Valid: true,
			Time:  time.Unix(0, 0).UTC(),
		},
		wantString: `'1970-01-01 00:00:00+00:00'`,
	}, {
		description: "int64 Valuer",
		value:       driverValuer{int64(3)},
		wantString:  `3`,
	}, {
		description: "float64 Valuer",
		value:       driverValuer{64.6464},
		wantString:  `64.6464`,
	}, {
		description: "bool Valuer 1",
		value:       driverValuer{true},
		wantString:  `TRUE`,
	}, {
		description: "bool Valuer 0",
		value:       driverValuer{false},
		wantString:  `FALSE`,
	}, {
		description: "bytes Valuer",
		value:       driverValuer{[]byte{0xab, 0xba}},
		wantString:  `x'abba'`,
	}, {
		description: "string Valuer",
		value:       driverValuer{`'' ha '; DROP TABLE users; --`},
		wantString:  `''''' ha ''; DROP TABLE users; --'`,
	}, {
		description: "time.Time Valuer",
		value:       driverValuer{time.Unix(0, 0).UTC()},
		wantString:  `'1970-01-01 00:00:00'`,
	}, {
		description: "time.Time Valuer (Postgres)",
		dialect:     DialectPostgres,
		value:       driverValuer{time.Unix(0, 0).UTC()},
		wantString:  `'1970-01-01 00:00:00+00:00'`,
	}, {
		description: "time.Time Valuer (Postgres)",
		dialect:     DialectPostgres,
		value:       driverValuer{time.Unix(22, 330000000).In(singaporeLocation)},
		wantString:  `'1970-01-01 07:30:22.33+07:30'`,
	}, {
		description: "string Valuer ptr",
		value:       &driverValuer{`'' ha '; DROP TABLE users; --`},
		wantString:  `''''' ha ''; DROP TABLE users; --'`,
	}, {
		description: "int ptr",
		value: func() *int {
			num := 33
			return &num
		}(),
		wantString: `33`,
	}, {
		description: "nil int ptr",
		value: func() *int {
			var num *int
			return num
		}(),
		wantString: `NULL`,
	}, {
		description: "string ptr",
		value: func() *string {
			str := "test string"
			return &str
		}(),
		wantString: `'test string'`,
	}, {
		description: "nil string ptr",
		value: func() *string {
			var str *string
			return str
		}(),
		wantString: `NULL`,
	}, {
		description: "sql.NullInt64 ptr",
		value: &sql.NullInt64{
			Valid: true,
			Int64: 33,
		},
		wantString: `33`,
	}, {
		description: "sql.NullString ptr",
		value: &sql.NullString{
			Valid:  true,
			String: "test string",
		},
		wantString: `'test string'`,
	}, {
		description: "mysql string",
		dialect:     DialectMySQL,
		value:       "the quick brown fox",
		wantString:  `'the quick brown fox'`,
	}, {
		description: "mysql string newlines in middle",
		dialect:     DialectMySQL,
		value:       "the quick\nbrown\r\nfox",
		wantString:  `CONCAT('the quick', CHAR(10), 'brown', CHAR(13), CHAR(10), 'fox')`,
	}, {
		description: "mysql string newlines at end",
		dialect:     DialectMySQL,
		value:       "\nthe quick brown fox\r\n",
		wantString:  `CONCAT(CHAR(10), 'the quick brown fox', CHAR(13), CHAR(10))`,
	}, {
		description: "postgres string",
		dialect:     DialectPostgres,
		value:       "the quick brown fox",
		wantString:  `'the quick brown fox'`,
	}, {
		description: "postgres string newlines in middle",
		dialect:     DialectPostgres,
		value:       "the quick\nbrown\r\nfox",
		wantString:  `'the quick' || CHR(10) || 'brown' || CHR(13) || CHR(10) || 'fox'`,
	}, {
		description: "postgres string newlines at end",
		dialect:     DialectPostgres,
		value:       "\nthe quick brown fox\r\n",
		wantString:  `CHR(10) || 'the quick brown fox' || CHR(13) || CHR(10)`,
	}, {
		description: "sql.NullString with newlines",
		dialect:     DialectPostgres,
		value: sql.NullString{
			Valid:  true,
			String: "\rthe quick\nbrown fox\r\n",
		},
		wantString: `CHR(13) || 'the quick' || CHR(10) || 'brown fox' || CHR(13) || CHR(10)`,
	}}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.description, func(t *testing.T) {
			t.Parallel()
			gotString, err := Sprint(tt.dialect, tt.value)
			if err != nil {
				t.Fatal(testutil.Callers(), err)
			}
			if diff := testutil.Diff(gotString, tt.wantString); diff != "" {
				t.Error(testutil.Callers(), diff)
			}
		})
	}
}

type tmptable string

var _ Table = (*tmptable)(nil)

func (t tmptable) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	schema, name := "", string(t)
	if i := strings.IndexByte(name, '.'); i >= 0 {
		schema, name = name[:i], name[i+1:]
	}
	if schema != "" {
		buf.WriteString(QuoteIdentifier(dialect, schema) + ".")
	}
	buf.WriteString(QuoteIdentifier(dialect, name))
	return nil
}

func (t tmptable) GetAlias() string { return "" }

func (t tmptable) IsTable() {}

type tmpfield string

var _ Field = (*tmpfield)(nil)

func (f tmpfield) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	table, name := "", string(f)
	if i := strings.IndexByte(name, '.'); i >= 0 {
		table, name = name[:i], name[i+1:]
	}
	if table != "" {
		buf.WriteString(QuoteIdentifier(dialect, table) + ".")
	}
	buf.WriteString(QuoteIdentifier(dialect, name))
	return nil
}

func (f tmpfield) WithPrefix(prefix string) Field {
	body := f
	if i := strings.IndexByte(string(f), '.'); i >= 0 {
		body = f[i+1:]
	}
	if prefix == "" {
		return body
	}
	return tmpfield(prefix + "." + string(body))
}

func (f tmpfield) GetAlias() string { return "" }

func (f tmpfield) IsField() {}

type FaultySQLError struct{}

func (e FaultySQLError) Error() string { return "sql broke" }

var ErrFaultySQL error = FaultySQLError{}

var _ interface {
	Query
	Table
	Field
	Predicate
	Assignment
} = (*FaultySQL)(nil)

type FaultySQL struct{}

func (q FaultySQL) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
	return ErrFaultySQL
}

func (q FaultySQL) SetFetchableFields([]Field) (Query, bool) { return nil, false }

func (q FaultySQL) GetFetchableFields() ([]Field, bool) { return nil, false }

func (q FaultySQL) GetAlias() string { return "" }

func (q FaultySQL) GetDialect() string { return "" }

func (q FaultySQL) IsBoolean() {}

func (q FaultySQL) IsTable() {}

func (q FaultySQL) IsField() {}

func (q FaultySQL) IsAssignment() {}

type driverValuer struct{ value any }

func (v driverValuer) Value() (driver.Value, error) { return v.value, nil }

type dialectValuer struct {
	mysqlValuer driver.Valuer
	valuer      driver.Valuer
}

func (v dialectValuer) DialectValuer(dialect string) (driver.Valuer, error) {
	if dialect == DialectMySQL {
		return v.mysqlValuer, nil
	}
	return v.valuer, nil
}
