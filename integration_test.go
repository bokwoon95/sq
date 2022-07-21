package sq

import (
	"database/sql"
	"net/url"
	"strings"
	"testing"

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

type SqTest struct {
	uuid       uuid.UUID
	data       any
	color      Color
	direction  Direction
	textArray  []string
	intArray   []int64
	floatArray []float64
	boolArray  []bool
}

var sqtestValues = []SqTest{
	{
		uuid:       uuid.UUID([16]byte{0: 1}),
		data:       map[string]any{"lorem ipsum": "dolor sit amet"},
		color:      ColorRed,
		direction:  DirectionNorth,
		textArray:  []string{"one", "two", "three"},
		intArray:   []int64{1, 2, 3},
		floatArray: []float64{1, 2, 3},
		boolArray:  []bool{true, false, false},
	},
	{
		uuid:       uuid.UUID([16]byte{0: 2}),
		data:       map[string]any{"lorem ipsum": "dolor sit amet"},
		color:      ColorGreen,
		direction:  DirectionSouth,
		textArray:  []string{"four", "five", "six"},
		intArray:   []int64{4, 5, 6},
		floatArray: []float64{4, 5, 6},
		boolArray:  []bool{false, true, false},
	},
	{
		uuid:       uuid.UUID([16]byte{0: 3}),
		data:       map[string]any{"lorem ipsum": "dolor sit amet"},
		color:      ColorBlue,
		direction:  DirectionEast,
		textArray:  []string{"seven", "eight", "nine"},
		intArray:   []int64{7, 8, 9},
		floatArray: []float64{7, 8, 9},
		boolArray:  []bool{false, false, true},
	},
}

type SQTEST struct {
	TableStruct
	UUID        UUIDField
	DATA        JSONField
	COLOR       EnumField
	DIRECTION   EnumField
	TEXT_ARRAY  ArrayField
	INT_ARRAY   ArrayField
	FLOAT_ARRAY ArrayField
	BOOL_ARRAY  ArrayField
}

func Test_Array_Enum_JSON_UUID(t *testing.T) {
	type TT struct {
		dialect  string
		driver   string
		dsn      string
		teardown string
		setup    string
	}

	tests := []TT{{
		dialect:  DialectSQLite,
		driver:   "sqlite3",
		dsn:      "file:/Test_Array_Enum_JSON_UUID/sqlite?vfs=memdb&_foreign_keys=true",
		teardown: "DROP TABLE IF EXISTS sqtest;",
		setup: "CREATE TABLE sqtest (" +
			"\n    uuid UUID PRIMARY KEY" +
			"\n    ,data JSON" +
			"\n    ,color TEXT" +
			"\n    ,direction TEXT" +
			"\n    ,text_array JSON" +
			"\n    ,int_array JSON" +
			"\n    ,float_array JSON" +
			"\n    ,bool_array JSON" +
			"\n);",
	}, {
		dialect: DialectPostgres,
		driver:  "postgres",
		dsn:     *postgresDSN,
		teardown: "DROP TABLE IF EXISTS sqtest;" +
			"\nDROP TYPE IF EXISTS direction;" +
			"\nDROP TYPE IF EXISTS color;",
		setup: "CREATE TYPE color AS ENUM ('red', 'green', 'blue');" +
			"\nCREATE TYPE direction AS ENUM ('north', 'south', 'east', 'west');" +
			"\nCREATE TABLE sqtest (" +
			"\n    uuid UUID PRIMARY KEY" +
			"\n    ,data JSONB" +
			"\n    ,color color" +
			"\n    ,direction direction" +
			"\n    ,text_array TEXT[]" +
			"\n    ,int_array INT[]" +
			"\n    ,float_array NUMERIC[]" +
			"\n    ,bool_array BOOLEAN[]" +
			"\n);",
	}, {
		dialect:  DialectMySQL,
		driver:   "mysql",
		dsn:      *mysqlDSN,
		teardown: "DROP TABLE IF EXISTS sqtest;",
		setup: "CREATE TABLE sqtest (" +
			"\n    uuid BINARY(16) PRIMARY KEY" +
			"\n    ,data JSON" +
			"\n    ,color VARCHAR(255)" +
			"\n    ,direction VARCHAR(255)" +
			"\n    ,text_array JSON" +
			"\n    ,int_array JSON" +
			"\n    ,float_array JSON" +
			"\n    ,bool_array JSON" +
			"\n);",
	}, {
		dialect:  DialectSQLServer,
		driver:   "sqlserver",
		dsn:      *sqlserverDSN,
		teardown: "DROP TABLE IF EXISTS sqtest;",
		setup: "CREATE TABLE sqtest (" +
			"\n    uuid BINARY(16) PRIMARY KEY" +
			"\n    ,data NVARCHAR(MAX)" +
			"\n    ,color NVARCHAR(255)" +
			"\n    ,direction NVARCHAR(255)" +
			"\n    ,text_array NVARCHAR(MAX)" +
			"\n    ,int_array NVARCHAR(MAX)" +
			"\n    ,float_array NVARCHAR(MAX)" +
			"\n    ,bool_array NVARCHAR(MAX)" +
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
				if tt.dialect == DialectSQLServer {
					return
				}
				db.Exec(tt.teardown)
			}()
			tbl := New[SQTEST]("")
			result, err := Exec(Log(db), InsertInto(tbl).
				ColumnValues(func(col *Column) error {
					for _, value := range sqtestValues {
						col.SetUUID(tbl.UUID, value.uuid)
						col.SetJSON(tbl.DATA, value.data)
						col.SetEnum(tbl.COLOR, value.color)
						col.SetEnum(tbl.DIRECTION, value.direction)
						col.SetArray(tbl.TEXT_ARRAY, value.textArray)
						col.SetArray(tbl.INT_ARRAY, value.intArray)
						col.SetArray(tbl.FLOAT_ARRAY, value.floatArray)
						col.SetArray(tbl.BOOL_ARRAY, value.boolArray)
					}
					return nil
				}).
				SetDialect(tt.dialect),
			)
			if err != nil {
				t.Fatal(testutil.Callers(), err)
			}
			if diff := testutil.Diff(result.RowsAffected, int64(len(sqtestValues))); diff != "" {
				t.Error(testutil.Callers(), diff)
			}
			values, err := FetchAll(VerboseLog(db), From(tbl).SetDialect(tt.dialect),
				func(row *Row) (SqTest, error) {
					var value SqTest
					row.UUIDField(&value.uuid, tbl.UUID)
					row.JSONField(&value.data, tbl.DATA)
					row.EnumField(&value.color, tbl.COLOR)
					row.EnumField(&value.direction, tbl.DIRECTION)
					row.ArrayField(&value.textArray, tbl.TEXT_ARRAY)
					row.ArrayField(&value.intArray, tbl.INT_ARRAY)
					row.ArrayField(&value.floatArray, tbl.FLOAT_ARRAY)
					row.ArrayField(&value.boolArray, tbl.BOOL_ARRAY)
					return value, nil
				},
			)
			if err != nil {
				t.Fatal(testutil.Callers(), err)
			}
			if diff := testutil.Diff(values, sqtestValues); diff != "" {
				t.Error(testutil.Callers(), diff)
			}
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
