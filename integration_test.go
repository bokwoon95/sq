package sq

import (
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/bokwoon95/sq/internal/testutil"
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

type Color int

const (
	ColorInvalid Color = iota
	ColorRed
	ColorGreen
	ColorBlue
)

var colorNames = [...]string{
	ColorInvalid: "",
	ColorRed:     "red",
	ColorGreen:   "green",
	ColorBlue:    "blue",
}

func (c Color) Enumerate() []string { return colorNames[:] }

type Direction string

const (
	DirectionInvalid = Direction("")
	DirectionNorth   = Direction("north")
	DirectionSouth   = Direction("south")
	DirectionEast    = Direction("east")
	DirectionWest    = Direction("west")
)

func (d Direction) Enumerate() []string {
	return []string{
		string(DirectionInvalid),
		string(DirectionNorth),
		string(DirectionSouth),
		string(DirectionEast),
		string(DirectionWest),
	}
}

func TestRow(t *testing.T) {
	type TestTable struct {
		dialect  string
		driver   string
		dsn      string
		teardown string
		setup    string
	}

	tests := []TestTable{{
		dialect:  DialectSQLite,
		driver:   "sqlite3",
		dsn:      "file:/TestRow/sqlite?vfs=memdb&_foreign_keys=true",
		teardown: "DROP TABLE IF EXISTS table00;",
		setup: "CREATE TABLE table00 (" +
			"\n    uuid UUID PRIMARY KEY" +
			"\n    ,data JSON" +
			"\n    ,color TEXT" +
			"\n    ,direction TEXT" +
			"\n    ,weekday TEXT" +
			"\n    ,text_array JSON" +
			"\n    ,int_array JSON" +
			"\n    ,int64_array JSON" +
			"\n    ,int32_array JSON" +
			"\n    ,float64_array JSON" +
			"\n    ,float32_array JSON" +
			"\n    ,bool_array JSON" +
			"\n    ,bytes BLOB" +
			"\n    ,is_active BOOLEAN" +
			"\n    ,price REAL" +
			"\n    ,score BIGINT" +
			"\n    ,name TEXT" +
			"\n    ,updated_at DATETIME" +
			"\n);",
	}, {
		dialect: DialectPostgres,
		driver:  "postgres",
		dsn:     *postgresDSN,
		teardown: "DROP TABLE IF EXISTS table00;" +
			"\nDROP TYPE IF EXISTS direction;" +
			"\nDROP TYPE IF EXISTS color;" +
			"\nDROP TYPE IF EXISTS weekday;",
		setup: "CREATE TYPE color AS ENUM ('red', 'green', 'blue');" +
			"\nCREATE TYPE direction AS ENUM ('north', 'south', 'east', 'west');" +
			"\nCREATE TYPE weekday AS ENUM ('Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday');" +
			"\nCREATE TABLE table00 (" +
			"\n    uuid UUID PRIMARY KEY" +
			"\n    ,data JSONB" +
			"\n    ,color color" +
			"\n    ,direction direction" +
			"\n    ,weekday weekday" +
			"\n    ,text_array TEXT[]" +
			"\n    ,int_array BIGINT[]" +
			"\n    ,int64_array BIGINT[]" +
			"\n    ,int32_array INT[]" +
			"\n    ,float64_array DOUBLE PRECISION[]" +
			"\n    ,float32_array REAL[]" +
			"\n    ,bool_array BOOLEAN[]" +
			"\n    ,is_active BOOLEAN" +
			"\n    ,bytes BYTEA" +
			"\n    ,price DOUBLE PRECISION" +
			"\n    ,score BIGINT" +
			"\n    ,name TEXT" +
			"\n    ,updated_at TIMESTAMPTZ" +
			"\n);",
	}, {
		dialect:  DialectMySQL,
		driver:   "mysql",
		dsn:      *mysqlDSN,
		teardown: "DROP TABLE IF EXISTS table00;",
		setup: "CREATE TABLE table00 (" +
			"\n    uuid BINARY(16) PRIMARY KEY" +
			"\n    ,data JSON" +
			"\n    ,color VARCHAR(255)" +
			"\n    ,direction VARCHAR(255)" +
			"\n    ,weekday VARCHAR(255)" +
			"\n    ,text_array JSON" +
			"\n    ,int_array JSON" +
			"\n    ,int64_array JSON" +
			"\n    ,int32_array JSON" +
			"\n    ,float64_array JSON" +
			"\n    ,float32_array JSON" +
			"\n    ,bool_array JSON" +
			"\n    ,is_active BOOLEAN" +
			"\n    ,bytes LONGBLOB" +
			"\n    ,price DOUBLE PRECISION" +
			"\n    ,score BIGINT" +
			"\n    ,name TEXT" +
			"\n    ,updated_at DATETIME" +
			"\n);",
	}, {
		dialect:  DialectSQLServer,
		driver:   "sqlserver",
		dsn:      *sqlserverDSN,
		teardown: "DROP TABLE IF EXISTS table00;",
		setup: "CREATE TABLE table00 (" +
			"\n    uuid BINARY(16) PRIMARY KEY" +
			"\n    ,data NVARCHAR(MAX)" +
			"\n    ,color NVARCHAR(255)" +
			"\n    ,direction NVARCHAR(255)" +
			"\n    ,weekday NVARCHAR(255)" +
			"\n    ,text_array NVARCHAR(MAX)" +
			"\n    ,int_array NVARCHAR(MAX)" +
			"\n    ,int64_array NVARCHAR(MAX)" +
			"\n    ,int32_array NVARCHAR(MAX)" +
			"\n    ,float64_array NVARCHAR(MAX)" +
			"\n    ,float32_array NVARCHAR(MAX)" +
			"\n    ,bool_array NVARCHAR(MAX)" +
			"\n    ,is_active BIT" +
			"\n    ,bytes VARBINARY(MAX)" +
			"\n    ,price DOUBLE PRECISION" +
			"\n    ,score BIGINT" +
			"\n    ,name NVARCHAR(255)" +
			"\n    ,updated_at DATETIME" +
			"\n);",
	}}

	var TABLE00 = New[struct {
		TableStruct   `sq:"table00"`
		UUID          UUIDField
		DATA          JSONField
		COLOR         EnumField
		DIRECTION     EnumField
		WEEKDAY       EnumField
		TEXT_ARRAY    ArrayField
		INT_ARRAY     ArrayField
		INT64_ARRAY   ArrayField
		INT32_ARRAY   ArrayField
		FLOAT64_ARRAY ArrayField
		FLOAT32_ARRAY ArrayField
		BOOL_ARRAY    ArrayField
		BYTES         BinaryField
		IS_ACTIVE     BooleanField
		PRICE         NumberField
		SCORE         NumberField
		NAME          StringField
		UPDATED_AT    TimeField
	}]("")

	type Table00 struct {
		uuid         uuid.UUID
		data         any
		color        Color
		direction    Direction
		weekday      Weekday
		textArray    []string
		intArray     []int
		int64Array   []int64
		int32Array   []int32
		float64Array []float64
		float32Array []float32
		boolArray    []bool
		bytes        []byte
		isActive     bool
		price        float64
		score        int64
		name         string
		updatedAt    time.Time
	}

	var table00Values = []Table00{{
		uuid:         uuid.UUID([16]byte{15: 1}),
		data:         map[string]any{"lorem ipsum": "dolor sit amet"},
		color:        ColorRed,
		direction:    DirectionNorth,
		weekday:      Monday,
		textArray:    []string{"one", "two", "three"},
		intArray:     []int{1, 2, 3},
		int64Array:   []int64{1, 2, 3},
		int32Array:   []int32{1, 2, 3},
		float64Array: []float64{1, 2, 3},
		float32Array: []float32{1, 2, 3},
		boolArray:    []bool{true, false, false},
		bytes:        []byte{1, 2, 3},
		isActive:     true,
		price:        123,
		score:        123,
		name:         "one two three",
		updatedAt:    time.Unix(123, 0).UTC(),
	}, {
		uuid:         uuid.UUID([16]byte{15: 2}),
		data:         map[string]any{"lorem ipsum": "dolor sit amet"},
		color:        ColorGreen,
		direction:    DirectionSouth,
		weekday:      Tuesday,
		textArray:    []string{"four", "five", "six"},
		intArray:     []int{4, 5, 6},
		int64Array:   []int64{4, 5, 6},
		int32Array:   []int32{4, 5, 6},
		float64Array: []float64{4, 5, 6},
		float32Array: []float32{4, 5, 6},
		boolArray:    []bool{false, true, false},
		bytes:        []byte{4, 5, 6},
		isActive:     true,
		price:        456,
		score:        456,
		name:         "four five six",
		updatedAt:    time.Unix(456, 0).UTC(),
	}, {
		uuid:         uuid.UUID([16]byte{15: 3}),
		data:         map[string]any{"lorem ipsum": "dolor sit amet"},
		color:        ColorBlue,
		direction:    DirectionEast,
		weekday:      Wednesday,
		textArray:    []string{"seven", "eight", "nine"},
		intArray:     []int{7, 8, 9},
		float64Array: []float64{7, 8, 9},
		boolArray:    []bool{false, false, true},
		bytes:        []byte{7, 8, 9},
		isActive:     true,
		price:        789,
		score:        789,
		name:         "seven eight nine",
		updatedAt:    time.Unix(789, 0).UTC(),
	}}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.dialect, func(t *testing.T) {
			if tt.dsn == "" {
				return
			}
			t.Parallel()
			dsn := preprocessDSN(tt.dialect, tt.dsn)
			db, err := sql.Open(tt.driver, dsn)
			if err != nil {
				t.Fatal(testutil.Callers(), err)
			}
			_, err = db.Exec(tt.teardown)
			if err != nil {
				t.Fatal(testutil.Callers(), err)
			}
			_, err = db.Exec(tt.setup)
			if err != nil {
				t.Fatal(testutil.Callers(), err)
			}
			defer func() {
				db.Exec(tt.teardown)
			}()

			// Insert the data.
			result, err := Exec(Log(db), InsertInto(TABLE00).
				ColumnValues(func(col *Column) {
					for _, value := range table00Values {
						col.SetUUID(TABLE00.UUID, value.uuid)
						col.SetJSON(TABLE00.DATA, value.data)
						col.SetEnum(TABLE00.COLOR, value.color)
						col.SetEnum(TABLE00.DIRECTION, value.direction)
						col.SetEnum(TABLE00.WEEKDAY, value.weekday)
						col.SetArray(TABLE00.TEXT_ARRAY, value.textArray)
						col.SetArray(TABLE00.INT_ARRAY, value.intArray)
						col.SetArray(TABLE00.INT64_ARRAY, value.int64Array)
						col.SetArray(TABLE00.INT32_ARRAY, value.int32Array)
						col.SetArray(TABLE00.FLOAT64_ARRAY, value.float64Array)
						col.SetArray(TABLE00.FLOAT32_ARRAY, value.float32Array)
						col.SetArray(TABLE00.BOOL_ARRAY, value.boolArray)
						col.SetBytes(TABLE00.BYTES, value.bytes)
						col.SetBool(TABLE00.IS_ACTIVE, value.isActive)
						col.SetFloat64(TABLE00.PRICE, value.price)
						col.SetInt64(TABLE00.SCORE, value.score)
						col.SetString(TABLE00.NAME, value.name)
						col.SetTime(TABLE00.UPDATED_AT, value.updatedAt)
					}
				}).
				SetDialect(tt.dialect),
			)
			if err != nil {
				t.Fatal(testutil.Callers(), err)
			}
			if diff := testutil.Diff(result.RowsAffected, int64(len(table00Values))); diff != "" {
				t.Error(testutil.Callers(), diff)
			}

			// Fetch the data.
			values, err := FetchAll(VerboseLog(db), From(TABLE00).
				OrderBy(TABLE00.UUID).
				SetDialect(tt.dialect),
				func(row *Row) Table00 {
					var value Table00
					row.UUIDField(&value.uuid, TABLE00.UUID)
					row.JSONField(&value.data, TABLE00.DATA)
					row.EnumField(&value.color, TABLE00.COLOR)
					row.EnumField(&value.direction, TABLE00.DIRECTION)
					row.EnumField(&value.weekday, TABLE00.WEEKDAY)
					row.ArrayField(&value.textArray, TABLE00.TEXT_ARRAY)
					row.ArrayField(&value.intArray, TABLE00.INT_ARRAY)
					row.ArrayField(&value.int64Array, TABLE00.INT64_ARRAY)
					row.ArrayField(&value.int32Array, TABLE00.INT32_ARRAY)
					row.ArrayField(&value.float64Array, TABLE00.FLOAT64_ARRAY)
					row.ArrayField(&value.float32Array, TABLE00.FLOAT32_ARRAY)
					row.ArrayField(&value.boolArray, TABLE00.BOOL_ARRAY)
					value.bytes = row.BytesField(TABLE00.BYTES)
					value.isActive = row.BoolField(TABLE00.IS_ACTIVE)
					value.price = row.Float64Field(TABLE00.PRICE)
					value.score = row.Int64Field(TABLE00.SCORE)
					value.name = row.StringField(TABLE00.NAME)
					value.updatedAt = row.TimeField(TABLE00.UPDATED_AT)
					// make sure Columns, ColumnTypes and Values are all
					// callable inside the rowmapper even for dynamic queries.
					fmt.Println(row.Columns())
					fmt.Println(row.ColumnTypes())
					fmt.Println(row.Values())
					return value
				},
			)
			if err != nil {
				t.Fatal(testutil.Callers(), err)
			}
			if diff := testutil.Diff(values, table00Values); diff != "" {
				t.Error(testutil.Callers(), diff)
			}

			exists, err := FetchExists(Log(db), SelectOne().
				From(TABLE00).
				Where(TABLE00.UUID.EqUUID(table00Values[0].uuid)).
				SetDialect(tt.dialect),
			)
			if err != nil {
				t.Fatal(testutil.Callers(), err)
			}
			if !exists {
				t.Errorf(testutil.Callers()+" expected row with uuid = %q to exist, got false", table00Values[0].uuid.String())
			}
		})
	}
}

func TestRowScan(t *testing.T) {
	table01Values := [][]any{
		{nil, nil, nil, nil, nil, nil},
		{123, int64(123), float64(123), "abc", true, time.Unix(123, 0).UTC()},
		{456, int64(456), float64(456), "def", true, time.Unix(456, 0).UTC()},
		{789, int64(789), float64(789), "ghi", true, time.Unix(789, 0).UTC()},
	}

	type TestTable struct {
		dialect  string
		driver   string
		dsn      string
		teardown string
		setup    string
	}

	tests := []TestTable{{
		dialect:  DialectSQLite,
		driver:   "sqlite3",
		dsn:      "file:/TestRowScan/sqlite?vfs=memdb&_foreign_keys=true",
		teardown: "DROP TABLE IF EXISTS table01;",
		setup: "CREATE TABLE table01 (" +
			"\n    id INT" +
			"\n    ,score BIGINT" +
			"\n    ,price REAL" +
			"\n    ,name TEXT" +
			"\n    ,is_active BOOLEAN" +
			"\n    ,updated_at DATETIME" +
			"\n);",
	}, {
		dialect:  DialectPostgres,
		driver:   "postgres",
		dsn:      *postgresDSN,
		teardown: "DROP TABLE IF EXISTS table01;",
		setup: "CREATE TABLE table01 (" +
			"\n    id INT" +
			"\n    ,score BIGINT" +
			"\n    ,price DOUBLE PRECISION" +
			"\n    ,name TEXT" +
			"\n    ,is_active BOOLEAN" +
			"\n    ,updated_at TIMESTAMPTZ" +
			"\n);",
	}, {
		dialect:  DialectMySQL,
		driver:   "mysql",
		dsn:      *mysqlDSN,
		teardown: "DROP TABLE IF EXISTS table01;",
		setup: "CREATE TABLE table01 (" +
			"\n    id INT" +
			"\n    ,score BIGINT" +
			"\n    ,price DOUBLE PRECISION" +
			"\n    ,name VARCHAR(255)" +
			"\n    ,is_active BOOLEAN" +
			"\n    ,updated_at DATETIME" +
			"\n);",
	}, {
		dialect:  DialectSQLServer,
		driver:   "sqlserver",
		dsn:      *sqlserverDSN,
		teardown: "DROP TABLE IF EXISTS table01;",
		setup: "CREATE TABLE table01 (" +
			"\n    id INT" +
			"\n    ,score BIGINT" +
			"\n    ,price DOUBLE PRECISION" +
			"\n    ,name NVARCHAR(255)" +
			"\n    ,is_active BIT" +
			"\n    ,updated_at DATETIME2" +
			"\n);",
	}}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.dialect, func(t *testing.T) {
			if tt.dsn == "" {
				return
			}
			t.Parallel()
			dsn := preprocessDSN(tt.dialect, tt.dsn)
			db, err := sql.Open(tt.driver, dsn)
			if err != nil {
				t.Fatal(testutil.Callers(), err)
			}
			_, err = db.Exec(tt.teardown)
			if err != nil {
				t.Fatal(testutil.Callers(), err)
			}
			_, err = db.Exec(tt.setup)
			if err != nil {
				t.Fatal(testutil.Callers(), err)
			}
			defer func() {
				db.Exec(tt.teardown)
			}()

			// Insert values.
			result, err := Exec(Log(db), InsertQuery{
				Dialect:     tt.dialect,
				InsertTable: Expr("table01"),
				ColumnMapper: func(col *Column) {
					for _, value := range table01Values {
						col.Set(Expr("id"), value[0])
						col.Set(Expr("score"), value[1])
						col.Set(Expr("price"), value[2])
						col.Set(Expr("name"), value[3])
						col.Set(Expr("is_active"), value[4])
						col.Set(Expr("updated_at"), value[5])
					}
				},
			})
			if err != nil {
				t.Fatal(testutil.Callers(), err)
			}
			if diff := testutil.Diff(result.RowsAffected, int64(len(table01Values))); diff != "" {
				t.Error(testutil.Callers(), diff)
			}

			t.Run("dynamic SQL query", func(t *testing.T) {
				gotValues, err := FetchAll(db,
					Queryf("SELECT {*} FROM table01 WHERE id IS NOT NULL ORDER BY id").SetDialect(tt.dialect),
					func(row *Row) []any {
						var id int
						var score1 int64
						var score2 int32
						var price float64
						var name string
						var isActive bool
						var updatedAt time.Time
						row.Scan(&id, "id")
						row.Scan(&score1, "score")
						row.Scan(&score2, "score")
						if diff := testutil.Diff(score1, int64(score2)); diff != "" {
							panic(fmt.Errorf(testutil.Callers() + diff))
						}
						row.Scan(&price, "price")
						row.Scan(&name, "name")
						row.Scan(&isActive, "is_active")
						row.Scan(&updatedAt, "updated_at")
						return []any{id, score1, price, name, isActive, updatedAt}
					},
				)
				if err != nil {
					t.Fatal(testutil.Callers(), err)
				}
				wantValues := [][]any{
					{123, int64(123), float64(123), "abc", true, time.Unix(123, 0).UTC()},
					{456, int64(456), float64(456), "def", true, time.Unix(456, 0).UTC()},
					{789, int64(789), float64(789), "ghi", true, time.Unix(789, 0).UTC()},
				}
				if diff := testutil.Diff(gotValues, wantValues); diff != "" {
					t.Error(testutil.Callers(), diff)
				}
			})

			t.Run("dynamic SQL query (null values)", func(t *testing.T) {
				gotValue, err := FetchOne(db,
					Queryf("SELECT {*} FROM table01 WHERE id IS NULL").SetDialect(tt.dialect),
					func(row *Row) []any {
						var id int
						var score1 int64
						var score2 int32
						var price float64
						var name string
						var isActive bool
						var updatedAt time.Time
						row.Scan(&id, "id")
						row.Scan(&score1, "score")
						row.Scan(&score2, "score")
						if diff := testutil.Diff(score1, int64(score2)); diff != "" {
							panic(fmt.Errorf(testutil.Callers() + diff))
						}
						row.Scan(&price, "price")
						row.Scan(&name, "name")
						row.Scan(&isActive, "is_active")
						row.Scan(&updatedAt, "updated_at")
						return []any{id, score1, price, name, isActive, updatedAt}
					},
				)
				if err != nil {
					t.Fatal(testutil.Callers(), err)
				}
				wantValue := []any{int(0), int64(0), float64(0), "", false, time.Time{}}
				if diff := testutil.Diff(gotValue, wantValue); diff != "" {
					t.Error(testutil.Callers(), diff)
				}
			})

			t.Run("dynamic SQL query (null values) (using sql.Null structs)", func(t *testing.T) {
				gotValue, err := FetchOne(db,
					Queryf("SELECT {*} FROM table01 WHERE id IS NULL").SetDialect(tt.dialect),
					func(row *Row) []any {
						var id sql.NullInt64
						var score1 sql.NullInt64
						var score2 sql.NullInt32
						var price sql.NullFloat64
						var name sql.NullString
						var isActive sql.NullBool
						var updatedAt sql.NullTime
						row.Scan(&id, "id")
						row.Scan(&score1, "score")
						row.Scan(&score2, "score")
						if diff := testutil.Diff(score1.Int64, int64(score2.Int32)); diff != "" {
							panic(fmt.Errorf(testutil.Callers() + diff))
						}
						row.Scan(&price, "price")
						row.Scan(&name, "name")
						row.Scan(&isActive, "is_active")
						row.Scan(&updatedAt, "updated_at")
						return []any{int(id.Int64), score1.Int64, price.Float64, name.String, isActive.Bool, updatedAt.Time}
					},
				)
				if err != nil {
					t.Fatal(testutil.Callers(), err)
				}
				wantValue := []any{int(0), int64(0), float64(0), "", false, time.Time{}}
				if diff := testutil.Diff(gotValue, wantValue); diff != "" {
					t.Error(testutil.Callers(), diff)
				}
			})

			t.Run("static SQL query", func(t *testing.T) {
				// Raw SQL query with.
				gotValues, err := FetchAll(Log(db),
					Queryf("SELECT id, score, price, name, is_active, updated_at FROM table01 WHERE id IS NOT NULL ORDER BY id").SetDialect(tt.dialect),
					func(row *Row) []any {
						return []any{
							row.Int("id"),
							row.Int64("score"),
							row.Float64("price"),
							row.String("name"),
							row.Bool("is_active"),
							row.Time("updated_at"),
						}
					},
				)
				if err != nil {
					t.Fatal(testutil.Callers(), err)
				}
				wantValues := [][]any{
					{123, int64(123), float64(123), "abc", true, time.Unix(123, 0).UTC()},
					{456, int64(456), float64(456), "def", true, time.Unix(456, 0).UTC()},
					{789, int64(789), float64(789), "ghi", true, time.Unix(789, 0).UTC()},
				}
				if diff := testutil.Diff(gotValues, wantValues); diff != "" {
					t.Error(testutil.Callers(), diff)
				}
			})

			t.Run("static SQL query (raw Values)", func(t *testing.T) {
				gotValues, err := FetchAll(db,
					Queryf("SELECT id, score, price, name, is_active, updated_at FROM table01 WHERE id IS NOT NULL ORDER BY id").SetDialect(tt.dialect),
					func(row *Row) []any {
						columns := row.Columns()
						columnTypes := row.ColumnTypes()
						values := row.Values()
						if len(columns) != len(columnTypes) || len(columnTypes) != len(values) {
							panic(fmt.Errorf(testutil.Callers()+" length of columns/columnTypes/values don't match: %v %v %v", columns, columnTypes, values))
						}
						return values
					},
				)
				if err != nil {
					t.Fatal(testutil.Callers(), err)
				}
				// We need to tweak wantValues depending on the dialect because
				// we are at the mercy of whatever that dialect's database
				// driver decides to return.
				var wantValues [][]any
				switch tt.dialect {
				case DialectSQLite, DialectPostgres, DialectSQLServer:
					wantValues = [][]any{
						{int64(123), int64(123), float64(123), "abc", true, time.Unix(123, 0).UTC()},
						{int64(456), int64(456), float64(456), "def", true, time.Unix(456, 0).UTC()},
						{int64(789), int64(789), float64(789), "ghi", true, time.Unix(789, 0).UTC()},
					}
				case DialectMySQL:
					wantValues = [][]any{
						{[]byte("123"), []byte("123"), []byte("123"), []byte("abc"), []byte("1"), time.Unix(123, 0).UTC()},
						{[]byte("456"), []byte("456"), []byte("456"), []byte("def"), []byte("1"), time.Unix(456, 0).UTC()},
						{[]byte("789"), []byte("789"), []byte("789"), []byte("ghi"), []byte("1"), time.Unix(789, 0).UTC()},
					}
				}
				if diff := testutil.Diff(gotValues, wantValues); diff != "" {
					t.Error(testutil.Callers(), diff)
				}
			})

			t.Run("static SQL query (null values)", func(t *testing.T) {
				gotValue, err := FetchOne(db,
					Queryf("SELECT id, score, price, name, is_active, updated_at FROM table01 WHERE id IS NULL").SetDialect(tt.dialect),
					func(row *Row) []any {
						columns := row.Columns()
						columnTypes := row.ColumnTypes()
						values := row.Values()
						if len(columns) != len(columnTypes) || len(columnTypes) != len(values) {
							panic(fmt.Errorf(testutil.Callers()+" length of columns/columnTypes/values don't match: %v %v %v", columns, columnTypes, values))
						}
						return values
					},
				)
				if err != nil {
					t.Fatal(testutil.Callers(), err)
				}
				if diff := testutil.Diff(gotValue, []any{nil, nil, nil, nil, nil, nil}); diff != "" {
					t.Error(testutil.Callers(), diff)
				}
			})

			t.Run("static SQL query (null values) (using sql.Null structs)", func(t *testing.T) {
				gotValue, err := FetchOne(db,
					Queryf("SELECT id, score, price, name, is_active, updated_at FROM table01 WHERE id IS NULL").SetDialect(tt.dialect),
					func(row *Row) []any {
						return []any{
							row.NullInt64("score"),
							row.NullFloat64("price"),
							row.NullString("name"),
							row.NullBool("is_active"),
							row.NullTime("updated_at"),
						}
					},
				)
				if err != nil {
					t.Fatal(testutil.Callers(), err)
				}
				wantValues := []any{sql.NullInt64{}, sql.NullFloat64{}, sql.NullString{}, sql.NullBool{}, sql.NullTime{}}
				if diff := testutil.Diff(gotValue, wantValues); diff != "" {
					t.Error(testutil.Callers(), diff)
				}
			})
		})
	}
}

func preprocessDSN(dialect string, dsn string) string {
	switch dialect {
	case DialectPostgres:
		before, after, _ := strings.Cut(dsn, "?")
		q, err := url.ParseQuery(after)
		if err != nil {
			return dsn
		}
		if !q.Has("sslmode") {
			q.Set("sslmode", "disable")
		}
		if !q.Has("binary_parameters") {
			q.Set("binary_parameters", "yes")
		}
		return before + "?" + q.Encode()
	case DialectMySQL:
		before, after, _ := strings.Cut(strings.TrimPrefix(dsn, "mysql://"), "?")
		q, err := url.ParseQuery(after)
		if err != nil {
			return dsn
		}
		if !q.Has("allowAllFiles") {
			q.Set("allowAllFiles", "true")
		}
		if !q.Has("multiStatements") {
			q.Set("multiStatements", "true")
		}
		if !q.Has("parseTime") {
			q.Set("parseTime", "true")
		}
		return before + "?" + q.Encode()
	default:
		return dsn
	}
}
