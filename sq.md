# sq (Structured Query)

<center>
  <img src="https://i.imgur.com/GAxInSb.png" title="code example of a select query using sq" alt="code example of a select query using sq, to give viewers a quick idea of what the library is about" style="max-width:90%;">
</center>

## Introduction to sq #introduction

Github link: [github.com/bokwoon95/sq](https://github.com/bokwoon95/sq)

sq is a type-safe data mapper and query builder for Go. It is not an ORM, but aims to be as convenient as an ORM while retaining the flexibility of a query builder/raw sql.

Notable features:

- Works across SQLite, Postgres, MySQL and SQL Server. [[more info](#set-query-dialect)]
- Each dialect has its own query builder, allowing the full use of dialect-specific features. [[more info](#dialect-specific-features)]
- Declarative schema migrations. [[more info](#declarative-schema)]
- Supports arrays, enums, JSON and UUID. [[more info](#arrays-enums-json-uuid)]
- Query logging. [[more info](#logging)]

## Installation #installation

This package only supports Go 1.18 and above because it uses generics for data mapping.

```shell
$ go get github.com/bokwoon95/sq
$ go install -tags=fts5 github.com/bokwoon95/sqddl@latest
```

## Quickstart #quickstart

Connect to the database.

```go
db, err := sql.Open("postgres", "postgres://username:password@localhost:5432/sakila?sslmode=disable")
```

Define your model structs(s).

```go
type Actor struct {
    ActorID    int
    FirstName  string
    LastName   string
    LastUpdate time.Time
}
```

Use one of the below three functions to run your query.

- **FetchAll(db, query, rowmapper) ([]T, error)**
  - Fetch all results from a query.
  - Equivalent to [sql.Query](https://pkg.go.dev/database/sql#DB.Query).
- **FetchOne(db, query, rowmapper) (T, error)**.
  - Fetch one result from a query.
  - Returns sql.ErrNoRows if no results.
  - Equivalent to [sql.QueryRow](https://pkg.go.dev/database/sql#DB.QueryRow).
- **Exec(db, query) (sq.Result, error)**.
  - Executes a query.
  - Returns the rows affected (and the last insert ID, if it is supported by the dialect).
  - Equivalent to [sql.Exec](https://pkg.go.dev/database/sql#DB.Exec).

### Select example #rawsql-select

#### Fetch all #rawsql-fetch-all

```sql
SELECT actor_id, first_name, last_name FROM actor WHERE first_name = 'DAN'
```

```go
actors, err := sq.FetchAll(db, sq.
    Queryf("SELECT {*} FROM actor WHERE first_name = {}", "DAN").
    SetDialect(sq.DialectPostgres),
    func(row *sq.Row) Actor {
        return Actor{
            ActorID:   row.Int("actor_id"),
            FirstName: row.String("first_name"),
            LastName:  row.String("last_name"),
        }
    },
)
```

#### Fetch one #rawsql-fetch-one

```sql
SELECT actor_id, first_name, last_name FROM actor WHERE actor_id = 18
```

```go
actor, err := sq.FetchOne(db, sq.
    Queryf("SELECT {*} FROM actor WHERE actor_id = {}", 18).
    SetDialect(sq.DialectPostgres),
    func(row *sq.Row) Actor {
        return Actor{
            ActorID:   row.Int("actor_id"),
            FirstName: row.String("first_name"),
            LastName:  row.String("last_name"),
        }
    },
)
```

#### Fetch cursor #rawsql-fetch-cursor

```sql
SELECT actor_id, first_name, last_name FROM actor WHERE first_name = 'DAN'
```

```go
cursor, err := sq.FetchCursor(db, sq.
    Queryf("SELECT {*} FROM actor WHERE first_name = {}", "DAN").
    SetDialect(sq.DialectPostgres),
    func(row *sq.Row) Actor {
        return Actor{
            ActorID:   row.Int("actor_id"),
            FirstName: row.String("first_name"),
            LastName:  row.String("last_name"),
        }
    },
)
if err != nil {
}
defer cursor.Close()

var actors []Actor
for cursor.Next() {
    actor, err := cursor.Result()
    if err != nil {
    }
    actors = append(actors, actor)
}
```

#### Fetch exists #rawsql-fetch-exists

```sql
SELECT EXISTS (SELECT 1 FROM actor WHERE actor_id = 18)
```

```go
exists, err := sq.FetchExists(db, sq.
    Queryf("SELECT 1 FROM actor WHERE actor_id = {}", 18).
    SetDialect(sq.DialectPostgres),
)
```

### Insert example #rawsql-insert

#### Insert one #rawsql-insert-one

```sql
INSERT INTO actor (actor_id, first_name, last_name) VALUES (18, 'DAN', 'TORN')
```

```go
_, err := sq.Exec(db, sq.
    Queryf("INSERT INTO actor (actor_id, first_name, last_name) VALUES {}", sq.RowValue{
        18, "DAN", "TORN",
    }).
    SetDialect(sq.DialectPostgres),
)
```

#### Insert many #rawsql-insert-many

```sql
INSERT INTO actor
    (actor_id, first_name, last_name)
VALUES
    (18, 'DAN', 'TORN'),
    (56, 'DAN', 'HARRIS'),
    (116, 'DAN', 'STREEP')
```

```go
_, err := sq.Exec(db, sq.
    Queryf("INSERT INTO actor (actor_id, first_name, last_name) VALUES {}", sq.RowValues{
        {18, "DAN", "TORN"},
        {56, "DAN", "HARRIS"},
        {166, "DAN", "STREEP"},
    }).
    SetDialect(sq.DialectPostgres),
)
```

### Update example #rawsql-update

```sql
UPDATE actor SET first_name = 'DAN', last_name = 'TORN' WHERE actor_id = 18
```

```go
_, err := sq.Exec(db, sq.
    Queryf("UPDATE actor SET first_name = {}, last_name = {} WHERE actor_id = {}",
        "DAN", "TORN", 18,
    ).
    SetDialect(sq.DialectPostgres),
)
```

### Delete example #rawsql-delete

```sql
DELETE FROM actor WHERE actor_id = 56
```

```go
_, err := sq.Exec(db, sq.
    Queryf("DELETE FROM actor WHERE actor_id = {}", 56).
    SetDialect(sq.DialectPostgres),
)
```

## How the rowmapper works #rowmapper

The [FetchAll/FetchOne/FetchCursor examples in the quickstart](#rawsql-select) use a rowmapper function both as a way of indicating what fields should be selected, as well as encoding how each row should be procedurally mapped back to a model struct.

```go
// The rowmapper function signature should match func(*sq.Row) T.
func(row *sq.Row) Actor {
    return Actor{
        ActorID:   row.Int("actor_id"),
        FirstName: row.String("first_name"),
        LastName:  row.String("last_name"),
    }
}
```

To go into greater detail, the rowmapper is first called in "passive mode" where the `sq.Row` records the fields needed by the SELECT query. Those fields are then injected back into the SELECT query ([via the `{*}` insertion point](#rawsql-select)) and the query is run for real. Then the rowmapper is called in "active mode" where each `sq.Row` method call actually returns a value from the underlying row. The `Actor` result returned by each rowmapper call is then appended into a slice. All this is done generically, so the rowmapper can yield any variable of type `T` and a slice `[]T` will be returned at the end.

**The order in which you call the `sq.Row` methods must be deterministic and must not change between rowmapper invocations**. Don't put an `row.Int()` call inside an if-block, for example.

### Handling errors #rowmapper-handling-errors

If you do any computation in a rowmapper that returns an error, you can panic() with it and the error will be propagated as the error return value of FetchAll/FetchOne/FetchCursor. Try not to do anything that returns an error in the rowmapper.

```go
func(row *sq.Row) Film {
    var film Film
    film.FilmID = row.Int("film_id")
    film.Title = row.String("title")
    film.Description = row.String("description")

    // Pull raw bytes from the DB and unmarshal as JSON.
    b := row.Bytes("special_features")
    err := json.Unmarshal(b, &film.SpecialFeatures)
    if err != nil {
        panic(err)
    }

    // Alternatively you can use row.JSON(), which doesn't
    // require you to do error handling.
    row.JSON(&film.SpecialFeatures, "special_features")

    return film
}
```

### Available methods #sq-row-methods

```go
// These methods are straighforward and return the type associated with their
// name.
//
// NULL values are automatically converted to a zero value: 0 for numbers, the
// empty string for strings, an nil slice for []byte, etc. Use the NullXXX
// method variants if capturing NULL is meaningful to you.
var _ []byte    = row.Bytes("field_name")
var _ bool      = row.Bool("field_name")
var _ float64   = row.Float64("field_name")
var _ int       = row.Int("field_name")
var _ int64     = row.Int64("field_name")
var _ string    = row.String("field_name")
var _ time.Time = row.Time("field_name")

// The sql.NullXXX variants.
var _ sql.NullBool    = row.NullBool("field_name")
var _ sql.NullFloat64 = row.NullFloat64("field_name")
var _ sql.NullInt64   = row.NullInt64("field_name")
var _ sql.NullString  = row.NullString("field_name")
var _ sql.NullTime    = row.NullTime("field_name")

// row.Scan scans the value of field_name into a destination pointer. If the
// pointer type implements sql.Scanner, this is where to use it.
row.Scan(dest, "field_name")

// row.Array scans the value of field_name into a destination slice pointer. Only
// *[]bool, *[]int64, *[]int32, *[]float64, *[]float32 and *[]string are
// supported. On Postgres this value must be an array, while for other dialects
// this value must be a JSON array.
row.Array(sliceDest, "field_name")

// row.JSON scans the value of field_name into a destination pointer that
// json.Unmarshal can unmarshal JSON into. The value must be JSON.
row.JSON(jsonDest, "field_name")

// row.UUID scans the value of field_name into a destination pointer whose
// underlying type must be [16]byte. The value can be BINARY(16) or a UUID string.
row.UUID(uuidDest, "field_name")
```

Additionally there are also the `Field` method variants that accept an `sq.Field` instead of a `string` name. This is relevant if you are [using the query builder](#querybuilder) instead of [raw SQL](#rawsql-select).

```go
var _ []byte    = row.BytesField(tbl.FIELD_NAME)
var _ bool      = row.BoolField(tbl.FIELD_NAME)
var _ float64   = row.Float64Field(tbl.FIELD_NAME)
var _ int       = row.IntField(tbl.FIELD_NAME)
var _ int64     = row.Int64Field(tbl.FIELD_NAME)
var _ string    = row.StringField(tbl.FIELD_NAME)
var _ time.Time = row.TimeField(tbl.FIELD_NAME)

var _ sql.NullBool    = row.NullBoolField(tbl.FIELD_NAME)
var _ sql.NullFloat64 = row.NullFloat64Field(tbl.FIELD_NAME)
var _ sql.NullInt64   = row.NullInt64Field(tbl.FIELD_NAME)
var _ sql.NullString  = row.NullStringField(tbl.FIELD_NAME)
var _ sql.NullTime    = row.NullTimeField(tbl.FIELD_NAME)

row.ScanField(dest, tbl.FIELD_NAME)

row.ArrayField(sliceDest, tbl.FIELD_NAME)

row.JSONField(jsonDest, tbl.FIELD_NAME)

row.UUIDField(uuidDest, tbl.FIELD_NAME)
```

## Setting the dialect of a query #set-query-dialect

Each [sample query in the quickstart](#rawsql-select) has its dialect set to Postgres.

```go
sq.Queryf("SELECT {*} FROM actor WHERE first_name = {}", "DAN").SetDialect(sq.DialectPostgres)
```

This is to generate a Postgres-compatible query, where each curly brace `{}` placeholder is replaced with a Postgres dollar placeholder (e.g. $1, $2, $3). This is the same case for the [query builder](#querybuilder-select). You can choose one of four possible dialects:

```go
const (
    DialectSQLite    = "sqlite"    // placeholders are $1, $2, $3
    DialectPostgres  = "postgres"  // placeholders are $1, $2, $3
    DialectMySQL     = "mysql"     // placeholders are ?, ?, ?
    DialectSQLServer = "sqlserver" // placeholders are @p1, @p2, @p3
)
```

Each dialect that you pick will use the corresponding placeholder type when generating the query. [Ordinal placeholders (`{1}`, `{2}`, `{3}`) and named placeholders (`{foo}`, `{bar}`, `{baz}`)](#ordinal-named-placeholders) are also supported.

You can use the **sq.SQLite**, **sq.Postgres**, **sq.MySQL** and **sq.SQLServer** package-level variables as shorthand for setting the dialect (in order to type less).

```go
sq.SQLite.Queryf(query)    // sq.Queryf(query).SetDialect(sq.DialectSQLite)
sq.Postgres.Queryf(query)  // sq.Queryf(query).SetDialect(sq.DialectPostgres)
sq.MySQL.Queryf(query)     // sq.Queryf(query).SetDialect(sq.DialectMySQL)
sq.SQLServer.Queryf(query) // sq.Queryf(query).SetDialect(sq.DialectSQLServer)
```

## sq's query templating syntax #templating-syntax

sq.Queryf (and sq.Expr) use a Printf-style templating syntax where the format string uses curly brace `{}` placeholders. Here is a basic example for Queryf:

```go
sq.Queryf("SELECT first_name FROM actor WHERE actor_id = {}", 18)
```

```sql
SELECT first_name FROM actor WHERE actor_id = 18
```

Unlike with SQL prepared statements, the curly brace `{}` placeholders are allowed to change the structure of a query (i.e. it can appear anywhere inside a query):

```go
sq.Queryf(
    // format
    "SELECT {} FROM {} WHERE first_name = {}",
    // values
    sq.Fields{sq.Expr("actor_id"), sq.Expr("last_name")},
    sq.Expr("actor"),
    "DAN",
)
```

```sql
SELECT actor_id, last_name FROM actor WHERE first_name = 'DAN'
```

### Escaping the curly brace #escaping-curly-brace

If you wish to actually use curly braces `{}` inside the format string (which is very rare), you must escape the opening curly brace by doubling it up like this: `{{}`.

```go
sq.Queryf("SELECT '{{}', '{{abcd}'")
```

```sql
SELECT '{}', '{abcd}'
```

### Value expansion #value-expansion

Each value passed to the query preprocessor is evaluated based on the following cases in the order shown:
1. If the value [implements the `SQLWriter` interface](#sqlwriter), its `WriteSQL` method is called.
2. Else if the value is a slice, the slice is expanded into a comma separated list.
    - Each item in this list is further evaluated recursively following the same logic.
    - byte slices (`[]byte`) are the exception, they are treated as a unit and do not undergo slice expansion.
3. Otherwise, a dialect-appropriate placeholder is appended to the query string and the value itself is appended to the args.

Here is an example of the three different cases in action.

```go
sq.Queryf(
    "SELECT {} FROM actor WHERE actor_id IN ({}) AND first_name = {}",
    // case 1
    sq.Expr("jsonb_build_object({})", []any{ // case 2
        sq.Literal("first_name"), // case 1
        sq.Expr("first_name"),    // case 1
        sq.Literal("last_name"),  // case 1
        sq.Expr("last_name"),     // case 1
    }),
    // case 2
    []int{18, 56, 116},
    // case 3
    "DAN",
).SetDialect(sq.DialectPostgres)
```

```sql
SELECT jsonb_build_object('first_name', first_name, 'last_name', last_name)
FROM actor
WHERE actor_id IN ($1, $2, $3) AND first_name = $4
-- args: 18, 56, 11, 'DAN'
```

### Ordinal and Named placeholders #ordinal-named-placeholders

The templating syntax supports 3 types of placeholders:
1. Anonymous placeholders `{}`.
2. Ordinal placeholders `{1}`, `{2}`, `{3}`.
    - Ordinal placeholders used 1-based indexing.
3. Named placeholders `{foo}`, `{bar}`, `{baz}`.
    - Named placeholders in the format string must have a corresponding `sql.Named` value.
    - Placeholder names must consist only of unicode letters, numbers `0-9` or underscore `_`.

It is possible for an anonymous placeholder, an ordinal placeholder and a named placeholder to refer to the same value.

```go
sq.Queryf("SELECT {}, {2}, {}, {name}", "Marco", sql.Named("name", "Polo"))
//                    └─────────────┘
//                   All refer to 'Polo'
```

```sql
SELECT 'Marco', 'Polo', 'Polo', 'Polo'
```

#### Anonymous parameter example #anonymous-params

```go
sq.SQLite.Queryf(   "SELECT {}, {}, {}", "foo", "bar", "foo") // SQLite
sq.Postgres.Queryf( "SELECT {}, {}, {}", "foo", "bar", "foo") // Postgres
sq.MySQL.Queryf(    "SELECT {}, {}, {}", "foo", "bar", "foo") // MySQL
sq.SQLServer.Queryf("SELECT {}, {}, {}", "foo", "bar", "foo") // SQLServer
```

```sql
SELECT $1, $2, $3    -- SQLite,    Args: 'foo', 'bar', 'foo'
SELECT $1, $2, $3    -- Postgres,  Args: 'foo', 'bar', 'foo'
SELECT ?, ?, ?       -- MySQL,     Args: 'foo', 'bar', 'foo'
SELECT @p1, @p2, @p3 -- SQLServer, Args: 'foo', 'bar', 'foo'
```

#### Ordinal parameter example #ordinal-params

```go
sq.SQLite.Queryf(   "SELECT {1}, {2}, {1}", "foo", "bar") // SQLite
sq.Postgres.Queryf( "SELECT {1}, {2}, {1}", "foo", "bar") // Postgres
sq.MySQL.Queryf(    "SELECT {1}, {2}, {1}", "foo", "bar") // MySQL
sq.SQLServer.Queryf("SELECT {1}, {2}, {1}", "foo", "bar") // SQLServer
```

```sql
SELECT $1, $2, $1    -- SQLite,    Args: 'foo', 'bar'
SELECT $1, $2, $1    -- Postgres,  Args: 'foo', 'bar'
SELECT ?, ?, ?       -- MySQL,     Args: 'foo', 'bar', 'foo'
SELECT @p1, @p2, @p1 -- SQLServer, Args: 'foo', 'bar'
```

#### Named parameter example #named-params

```go
// SQLite
sq.SQLite.Queryf("SELECT {one}, {two}, {one}",
    sql.Named("one", "foo"),
    sql.Named("two", "bar"),
)
// Postgres
sq.Postgres.Queryf("SELECT {one}, {two}, {one}",
    sql.Named("one", "foo"),
    sql.Named("two", "bar"),
)
// MySQL
sq.MySQL.Queryf("SELECT {one}, {two}, {one}",
    sql.Named("one", "foo"),
    sql.Named("two", "bar"),
)
// SQLServer
sq.SQLServer.Queryf("SELECT {one}, {two}, {one}",
    sql.Named("one", "foo"),
    sql.Named("two", "bar"),
)
```

```sql
SELECT $one, $two, $one -- SQLite,    Args: one: 'foo', two: 'bar'
SELECT $1, $2, $1       -- Postgres,  Args: 'foo', 'bar'
SELECT ?, ?, ?          -- MySQL,     Args: 'foo', 'bar', 'foo'
SELECT @one, @two, @one -- SQLServer, Args: one: 'foo', two: 'bar'
```

### SQLWriter example #sqlwriter

An SQLWriter represents anything that can render itself as SQL. It is the first thing taken into consideration during [value expansion](#value-expansion).

Here is the definition of the SQLWriter interface:

```go
type SQLWriter interface {
    WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error
}
```

As an example, we will create a custom SQLWriter component that renders itself as string `str` for `num` times, where `str` and `num` are parameters:

```go
sq.Queryf("SELECT {}", multiplier{str: "lorem ipsum", num: 5, delim: " "})
```

```sql
SELECT lorem ipsum lorem ipsum lorem ipsum lorem ipsum lorem ipsum
```

This is the implementation of `multiplier`:

```go
type multiplier struct {
    str   string
    num   int
    delim string
}

func (m multiplier) WriteSQL(ctx context.Context, dialect string, buf *bytes.Buffer, args *[]any, params map[string][]int) error {
    for i := 0; i < m.num; i++ {
        if i > 0 {
            buf.WriteString(m.delim)
        }
        buf.WriteString(m.str)
    }
    return nil
}
```

```go
sq.Queryf("SELECT {}", multiplier{str: "foo",         num: 3, delim: "AND"})
sq.Queryf("SELECT {}", multiplier{str: "lorem ipsum", num: 4, delim: ", "})
sq.Queryf("SELECT {}", multiplier{str: "🎉",          num: 6, delim: ""})
```

```sql
SELECT foo AND foo AND foo
SELECT lorem ipsum, lorem ipsum, lorem ipsum, lorem ipsum
SELECT 🎉🎉🎉🎉🎉🎉
```

## Using the query builder #querybuilder

### Table structs #table-structs

To use a query builder, you need to first define your table struct(s).

```go
type ACTOR struct {
    sq.TableStruct // A table struct is marked by embedding sq.TableStruct as the first field.
    ACTOR_ID    sq.NumberField
    FIRST_NAME  sq.StringField
    LAST_NAME   sq.StringField
    LAST_UPDATE sq.TimeField
}
```

You can then instantiate the table using [sq.New()](https://pkg.go.dev/github.com/bokwoon95/sq#New) and use it to create predicates and participate in a query.

```go
a := sq.New[ACTOR]("a")
// actor AS a

a.ACTOR_ID.EqInt(18)
// a.actor_id = 18

a.LAST_UPDATE.IsNotNull()
// a.last_update IS NOT NULL

sq.Select(a.FIRST_NAME, a.LAST_NAME).From(a).Where(a.ACTOR_ID.In([]int{18, 56, 116}))
// SELECT a.first_name, a.last_name FROM actor AS a WHERE a.actor_id IN (18, 56, 116)
```

### Available Field types #field-types

There are 10 available field types that you can use in your [table structs](#table-structs).

- **NumberField** (`int`, `int64`, INT, BIGINT, NUMERIC, etc)
- **StringField** (`string`, TEXT, VARCHAR, etc)
- **TimeField** (`time.Time`, DATE, DATETIME, TIMESTAMP, etc)
- **BooleanField** (`bool`, BOOLEAN, TINYINT, BIT, etc)
- **BinaryField** (`[]byte`, BYTEA, BINARY, etc)
- **ArrayField**
    - Represents a primitive slice type in Go (`[]string`, `[]int64`, `[]int32`, `[]float64`, `[]float32`, `[]bool`)
    - In Postgres, this is a native array (TEXT[], INT[], BIGINT[], NUMERIC[], BOOLEAN[])
    - In other databases, this is a JSON array.
- **EnumField**
    - Represents an "enum" type in Go (`iota`, `string`, take your pick)
    - In Postgres, this is a native enum type (CREATE TYPE AS ENUM)
    - In other databases, this is a plain string.
    - Your Go enum type must [implement the `Enumeration` interface](#enums).
- **JSONField**
    - Represents a Go type that works with `json.Marshal` and `json.Unmarshal`.
    - In Postgres, this is the JSONB or JSON type.
    - In MySQL, this is the JSON type.
    - In other databases, this is a plain string.
- **UUIDField**
    - Represents any type whose underlying type is [16]byte in Go.
    - In Postgres, this is a UUID.
    - In other databases, this is a BINARY(16).
- **AnyField**
    - A catch-all field type that can substitute as any of the 9 other field types.
    - Use this to represent types like `TSVECTOR` that don't have a corresponding representation.

### Field name to column name translation #field-name-translation

The table name and column names are derived by lowercasing the struct name and struct field names. So a struct `ACTOR` will be translated to a table called `actor`, and a field `ACTOR_ID` will be translated to a column called `actor_id`. If that is not what you want, you can specify the desired name inside an `sq` struct tag.

```go
type ACTOR struct {
    sq.TableStruct `sq:"Actor"`
    ACTOR_ID       sq.NumberField `sq:"ActorID"`
    FIRST_NAME     sq.StringField `sq:"FirstName"`
    LAST_NAME      sq.StringField `sq:"LastName"`
    LAST_UPDATE    sq.TimeField   `sq:"LastUpdate"`
}

a := sq.New[ACTOR]("") // "Actor"
a.ACTOR_ID             // "Actor"."ActorID"
a.FIRST_NAME           // "Actor"."FirstName"
```

### Aliasing a table struct #alias-table-struct

sq.New() takes in an alias string as an argument and returns a table with that alias. Leave the alias string blank if you don't want the table to have an alias.

```go
a1 := sq.New[ACTOR]("a") // actor AS a
a1.ACTOR_ID              // a.actor_id

a2 := sq.New[ACTOR]("") // actor
a2.ACTOR_ID             // actor.actor_id
```

### Table structs as a declarative schema #declarative-schema

#### Generating migrations #generating-migrations

Your [table structs](#table-structs) serve as a declarative schema for your tables. The [sqddl tool](https://bokwoon.neocities.org/sqddl.html) is able to parse Go files containing table structs and [generate the necessary migrations](https://bokwoon.neocities.org/sqddl.html#generate) needed to reach that desired schema. The generated migrations can then be [applied using the same sqddl tool](https://bokwoon.neocities.org/sqddl.html#migrate).

```shell
# Generate migrations needed to go from $DATABASE_URL to tables/tables.go and write into ./migrations dir
$ sqddl generate -src "$DATABASE_URL" -dest tables/tables.go -output-dir ./migrations

# Apply the pending migrations in ./migrations dir against the database $DATABASE_URL
$ sqddl migrate -db "$DATABASE_URL" -dir ./migrations
```

For more information on how to express "CREATE TABLE" DDL using tables structs, please check out the [sqddl documentation](https://bokwoon.neocities.org/sqddl.html#table-structs).

#### Generating table structs #generating-table-structs

The reverse is also possible, you can [generate table structs from an existing database](https://bokwoon.neocities.org/sqddl.html#tables). If you have an existing database this is the recommended way to get started, rather than creating the table structs manually to match the database.

```shell
# Generate table structs from $DATABASE_URL and write into tables/tables.go
$ sqddl tables -db "$DATABASE_URL" -file tables/tables.go
```

Once you have your table structs, you can edit your table structs and [generate migrations](#generating-migrations) from them. Note that migration generation only covers [a subset of possible DDL operations](#) so it's possible that you will have to write some migrations by hand.

### Select example #querybuilder-select

#### Fetch all #querybuilder-fetch-all

```sql
SELECT a.actor_id, a.first_name, a.last_name FROM actor AS a WHERE a.first_name = 'DAN'
```

```go
a := sq.New[ACTOR]("a")
actors, err := sq.FetchAll(db, sq.
    From(a).
    Where(a.FIRST_NAME.EqString("DAN")).
    SetDialect(sq.DialectPostgres),
    func(row *sq.Row) Actor {
        return Actor{
            ActorID:    row.IntField(a.ACTOR_ID),
            FirstName:  row.StringField(a.FIRST_NAME),
            LastName:   row.StringField(a.LAST_NAME),
        }
    },
)
```

#### Fetch one #querybuilder-fetch-one

```sql
SELECT a.actor_id, a.first_name, a.last_name FROM actor AS a WHERE a.actor_id = 18
```

```go
a := sq.New[ACTOR]("a")
actor, err := sq.FetchOne(db, sq.
    From(a).
    Where(a.ACTOR_ID.EqInt(18)).
    SetDialect(sq.DialectPostgres),
    func(row *sq.Row) Actor {
        return Actor{
            ActorID:    row.IntField(a.ACTOR_ID),
            FirstName:  row.StringField(a.FIRST_NAME),
            LastName:   row.StringField(a.LAST_NAME),
        }
    },
)
```

#### Fetch cursor #querybuilder-fetch-cursor

```sql
SELECT a.actor_id, a.first_name, a.last_name FROM actor AS a WHERE a.first_name = 'DAN'
```

```go
a := sq.New[ACTOR]("a")
cursor, err := sq.FetchCursor(db, sq.
    From(a).
    Where(a.FIRST_NAME.EqString("DAN")).
    SetDialect(sq.DialectPostgres),
    func(row *sq.Row) Actor {
        return Actor{
            ActorID:    row.IntField(a.ACTOR_ID),
            FirstName:  row.StringField(a.FIRST_NAME),
            LastName:   row.StringField(a.LAST_NAME),
        }
    },
)
if err != nil {
}
defer cursor.Close()

var actor []Actor
for cursor.Next() {
    actor, err := cursor.Result()
    if err != nil {
    }
    actors = append(actors, actor)
}
```

#### Fetch exists #querybuilder-fetch-exists

```sql
SELECT EXISTS (SELECT 1 FROM actor AS a WHERE a.actor_id = 18)
```

```go
a := sq.New[ACTOR]("a")
exists, err := sq.FetchExists(db, sq.
    SelectOne().
    From(a).
    Where(a.ACTOR_ID.EqInt(18)).
    SetDialect(sq.DialectPostgres),
)
```

#### Fetch distinct #querybuilder-fetch-distinct

```sql
SELECT DISTINCT a.first_name FROM actor AS a
```

```go
a := sq.New[ACTOR]("a")
firstNames, err := sq.FetchAll(db, sq.
    SelectDistinct().
    From(a).
    SetDialect(sq.DialectPostgres),
    func(row *sq.Row) string {
        return row.String(a.FIRST_NAME)
    },
)
```

### Insert example #querybuilder-insert

#### Insert one #querybuilder-insert-one

```sql
INSERT INTO actor (actor_id, first_name, last_name) VALUES (18, 'DAN', 'TORN')
```

```go
a := sq.New[ACTOR]("")
_, err := sq.Exec(db, sq.
    InsertInto(a).
    Columns(a.ACTOR_ID, a.FIRST_NAME, a.LAST_NAME).
    Values(18, "DAN", "TORN").
    SetDialect(sq.DialectPostgres),
)
```

#### Insert many #querybuilder-insert-many

```sql
INSERT INTO actor
    (actor_id, first_name, last_name)
VALUES
    (18, 'DAN', 'TORN'),
    (56, 'DAN', 'HARRIS'),
    (116, 'DAN', 'STREEP')
```

```go
a := sq.New[ACTOR]("")
_, err := sq.Exec(db, sq.
    InsertInto(a).
    Columns(a.ACTOR_ID, a.FIRST_NAME, a.LAST_NAME).
    Values(18, "DAN", "TORN").
    Values(56, "DAN", "HARRIS").
    Values(166, "DAN", "STREEP").
    SetDialect(sq.DialectPostgres),
)
```

#### Insert from Select #querybuilder-insert-from-select

```sql
INSERT INTO actor (actor_id, first_name, last_name)
SELECT actor.actor_id, actor.first_name, actor.last_name
FROM actor
WHERE actor.last_update IS NOT NULL
```

```go
a := sq.New[ACTOR]("")
_, err := sq.Exec(db, sq.
    InsertInto(a).
    Columns(a.ACTOR_ID, a.FIRST_NAME, a.LAST_NAME).
    Select(sq.
        Select(a.ACTOR_ID, a.FIRST_NAME, a.LAST_NAME).
        From(a).
        Where(a.LAST_UPDATE.IsNotNull()),
    ).
    SetDialect(sq.DialectPostgres),
)
```

#### Insert one (column mapper) #querybuilder-insert-one-columnmapper

```sql
INSERT INTO actor (actor_id, first_name, last_name) VALUES (18, 'DAN', 'TORN')
```

```go
a := sq.New[ACTOR]("")
_, err := sq.Exec(db, sq.
    InsertInto(a).
    ColumnValues(func(col *sq.Column) error {
        col.SetInt(a.ACTOR_ID, 18)
        col.SetString(a.FIRST_NAME, "DAN")
        col.SetString(a.LAST_NAME, "TORN")
        return nil
    }).
    SetDialect(sq.DialectPostgres),
)
```

#### Insert many (column mapper) #querybuilder-insert-many-columnmapper

```sql
INSERT INTO actor
    (actor_id, first_name, last_name)
VALUES
    (18, 'DAN', 'TORN'),
    (56, 'DAN', 'HARRIS'),
    (116, 'DAN', 'STREEP')
```

```go
actors := []Actor{
    {ActorID: 18, FirstName: "DAN", LastName: "TORN"},
    {ActorID: 56, FirstName: "DAN", LastName: "HARRIS"},
    {ActorID: 166, FirstName: "DAN", LastName: "STREEP"},
}
a := sq.New[ACTOR]("")
_, err := sq.Exec(db, sq.
    InsertInto(a).
    ColumnValues(func(col *sq.Column) error {
        for _, actor := range actors {
            col.SetInt(a.ACTOR_ID, actor.ActorID)
            col.SetString(a.FIRST_NAME, actor.FirstName)
            col.SetString(a.LAST_NAME, actor.LastName)
        }
        return nil
    }).
    SetDialect(sq.DialectPostgres),
)
```

#### How does the Insert column mapper work? #insert-columnmapper

The Insert column mapper works by having the `sq.Column` note down the very first field passed to it. Everytime `sq.Column` sees that field again, it will treat it as starting a new row value.

```go
a := sq.New[ACTOR]("")
q := sq.
    InsertInto(a).
    ColumnValues(func(col *sq.Column) error {
        col.SetInt(a.ACTOR_ID, 1) // every a.ACTOR_ID will mark the start of a new row value
        col.SetString(a.FIRST_NAME, "PENELOPE")
        col.SetString(a.LAST_NAME, "GUINESS")

        col.SetInt(a.ACTOR_ID, 2)
        col.SetString(a.FIRST_NAME, "NICK")
        col.SetString(a.LAST_NAME, "WAHLBERG")

        col.SetInt(a.ACTOR_ID, 3)
        col.SetString(a.FIRST_NAME, "ED")
        col.SetString(a.LAST_NAME, "CHASE")
        return nil
    }).
    SetDialect(sq.DialectPostgres)
```

```sql
INSERT INTO actor
    (actor_id, first_name, last_name)
VALUES
    (1, 'PENELOPE', 'GUINESS'),
    (2, 'NICK', 'WAHLBERG'),
    (3, 'ED', 'CHASE')
```

### Update example #querybuilder-update

```sql
UPDATE actor SET first_name = 'DAN', last_name = 'TORN' WHERE actor.actor_id = 18
```

```go
a := sq.New[ACTOR]("")
_, err := sq.Exec(db, sq.
    Update(a).
    Set(
        a.FIRST_NAME.SetString("DAN"),
        a.LAST_NAME.SetString("TORN"),
    ).
    Where(a.ACTOR_ID.EqInt(18)).
    SetDialect(sq.DialectPostgres),
)
```

#### Update (column mapper) #update-columnmapper

```sql
UPDATE actor SET first_name = 'DAN', last_name = 'TORN' WHERE actor.actor_id = 18
```

```go
a := sq.New[ACTOR]("")
_, err := sq.Exec(db, sq.
    Update(a).
    SetFunc(func(col *sq.Column) error {
        col.SetString(a.FIRST_NAME, "DAN")
        col.SetString(a.LAST_NAME, "TORN")
        return nil
    }).
    Where(a.ACTOR_ID.EqInt(18)).
    SetDialect(sq.DialectPostgres),
)
```

### Delete example #querybuilder-delete-example

```sql
DELETE FROM actor WHERE actor.actor_id = 56
```

```go
a := sq.New[ACTOR]("")
_, err := sq.Exec(db, sq.
    DeleteFrom(a).
    Where(a.ACTOR_ID.EqInt(56)).
    SetDialect(sq.DialectPostgres),
)
```

### Combining predicates (AND and OR) #combining-predicates

`Where()` accepts more than one predicate. By default, those predicates are `AND`-ed together.

```go
a := sq.New[ACTOR]("a")
query := sq.
    Select(a.ACTOR_ID, a.FIRST_NAME, a.LAST_NAME).
    From(a).
    Where(
        a.FIRST_NAME.EqString("BOB"),
        a.LAST_NAME.EqString("THE BUILDER"),
        a.LAST_UPDATE.IsNotNull(),
    )
```

```sql
SELECT a.actor_id, a.first_name, a.last_name
FROM actor AS a
WHERE a.first_name = 'BOB' AND a.last_name = 'THE BUILDER' AND a.last_update IS NOT NULL
```

If you need to `OR` those predicates together, wrap them in `sq.Or()`.

```go
a := sq.New[ACTOR]("a")
query := sq.
    Select(a.actor_id, a.FIRST_NAME, a.LAST_NAME).
    From(a).
    Where(sq.Or( // <-- sq.Or
        a.FIRST_NAME.EqString("BOB"),
        a.LAST_NAME.EqString("THE BUILDER"),
        a.LAST_UPDATE.IsNotNull(),
    ))
```

```sql
SELECT a.actor_id, a.first_name, a.last_name
FROM actor AS a
WHERE a.first_name = 'BOB' OR a.last_name = 'THE BUILDER' OR a.last_update IS NOT NULL
```

### Using expressions in the query builder #expr

If you need to do SQL math or call an SQL function, you need to use sq.Expr() to create an expression. [The same query templating syntax](#templating-syntax) in sq.Queryf() can be used here.

```sql
SELECT a.first_name || ' ' || a.last_name AS fullname
FROM actor AS a
WHERE a.actor_id IN (18, 56, 116)
```

```go
a := sq.New[ACTOR]("a")
q := sq.
    Select(sq.Expr("{} || ' ' || {}", a.FIRST_NAME, a.LAST_NAME).As("fullname")).
    From(a).
    Where(a.ACTOR_ID.In([]int{18, 56, 116})).
    SetDialect(sq.DialectPostgres)
```

sq.Expr() satisfies the `Any` interface and can be used wherever a `Number`, `String`, `Time`, `Boolean`, `Binary`, `Array`, `Enum`, `JSON` or `UUID` interface is expected.

#### Dialect expressions #dialect-expr

Sometimes a query may be the same across different dialects save for some dialect-specific function call or expression, which changes for each dialect. In those cases you can use sq.DialectExpr() to use different expressions depending on the dialect.

```sql
-- The 3 queries below are nearly identical except for the name of their JSON
-- aggregation function.

-- SQLite
SELECT json_group_array(a.last_name)
FROM actor AS a WHERE a.first_name = 'DAN'

-- Postgres
SELECT json_agg(a.last_name)
FROM actor AS a WHERE a.first_name = 'DAN'

-- MySQL
SELECT json_arrayagg(a.last_name)
FROM actor AS a WHERE a.first_name = 'DAN'
```

```go
a := sq.New[ACTOR]("a")
q := sq.
    Select(
        sq.DialectExpr("json_group_array({})", a.LAST_NAME).                // default case
            DialectExpr(sq.DialectPostgres, "json_agg({})", a.LAST_NAME).   // if dialect == sq.DialectPostgres
            DialectExpr(sq.DialectMySQL, "json_arrayagg({})", a.LAST_NAME), // if dialect == sq.DialectMySQL
    ),
    From(a).
    Where(a.FIRST_NAME.EqString("DAN")).
    SetDialect(dialect)
```

Similar to sq.Expr(), sq.DialectExpr() can be used wherever a `Number`, `String`, `Time`, `Boolean`, `Binary`, `Array`, `Enum`, `JSON` or `UUID` interface is expected.

## How do I use dialect-specific features? #dialect-specific-features

There are dialect-specific query builders for each dialect that are accessible through the four package-level variables:
- **sq.SQLite**
- **sq.Postgres**
- **sq.MySQL**
- **sq.SQLServer**

Do note that you can also use the dialect-agnostic query builder ([as shown in the query builder examples)](#querybuilder-select) if you're not using any dialect-specific features. Doing so will make your queries more portable, as you can just [toggle the dialect on the query](#set-query-dialect) and have it work across multiple databases without effort.

### SQLite-specific features #sqlite-specific-features

#### RETURNING #sqlite-returning

```sql
INSERT INTO actor
    (first_name, last_name)
VALUES
    ('PENELOPE', 'GUINESS'),
    ('NICK', 'WAHLBERG'),
    ('ED', 'CHASE')
RETURNING
    actor.actor_id, actor.first_name, actor.last_name
```

```go
a := sq.New[ACTOR]("")
actors, err := sq.FetchAll(db, sq.SQLite.
    InsertInto(a).
    Columns(a.FIRST_NAME, a.LAST_NAME).
    Values("PENELOPE", "GUINESS").
    Values("NICK", "WAHLBERG").
    Values("ED", "CHASE"),
    func(row *sq.Row) Actor {
        return Actor{
            ActorID:   row.IntField(a.ACTOR_ID),
            FirstName: row.StringField(a.FIRST_NAME),
            LastName:  row.StringField(a.LAST_NAME),
        }
    },
)
```

#### LastInsertId #sqlite-last-insert-id

```sql
INSERT INTO actor (first_name, last_name) VALUES ('PENELOPE', 'GUINESS');
SELECT last_insert_rowid();
```

```go
a := sq.New[ACTOR]("")
result, err := sq.Exec(db, sq.SQLite.
    InsertInto(a).
    Columns(a.FIRST_NAME, a.LAST_NAME).
    Values("PENELOPE", "GUINESS"),
)
if err != nil {
}
fmt.Println(result.LastInsertId) // int64
```

#### Insert ignore duplicates #sqlite-insert-ignore-duplicates

```sql
INSERT INTO actor
    (actor_id, first_name, last_name)
VALUES
    (1, 'PENELOPE', 'GUINESS'),
    (2, 'NICK', 'WAHLBERG'),
    (3, 'ED', 'CHASE')
ON CONFLICT DO NOTHING
```

```go
a := sq.New[ACTOR]("")
_, err := sq.Exec(db, sq.SQLite.
    InsertInto(a).
    Columns(a.ACTOR_ID, a.FIRST_NAME, a.LAST_NAME).
    Values(1, "PENELOPE", "GUINESS").
    Values(2, "NICK", "WAHLBERG").
    Values(3, "ED", "CHASE").
    OnConflict().DoNothing(),
)
```

#### Upsert #sqlite-upsert

```sql
INSERT INTO actor
    (actor_id, first_name, last_name)
VALUES
    (1, 'PENELOPE', 'GUINESS'),
    (2, 'NICK', 'WAHLBERG'),
    (3, 'ED', 'CHASE')
ON CONFLICT (actor_id) DO UPDATE SET
    first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name
```

```go
a := sq.New[ACTOR]("")
_, err := sq.Exec(db, sq.SQLite.
    InsertInto(a).
    Columns(a.ACTOR_ID, a.FIRST_NAME, a.LAST_NAME).
    Values(1, "PENELOPE", "GUINESS").
    Values(2, "NICK", "WAHLBERG").
    Values(3, "ED", "CHASE").
    OnConflict(a.ACTOR_ID).DoUpdateSet(
        a.FIRST_NAME.Set(a.FIRST_NAME.WithPrefix("EXCLUDED")),
        a.LAST_NAME.Set(a.LAST_NAME.WithPrefix("EXLCUDED")),
    ),
)
```

#### Update with Join #sqlite-update-with-join

```sql
UPDATE actor
SET last_name = 'DINO'
FROM film_actor
JOIN film ON film.film_id = film_actor.film_id
WHERE film_actor.actor_id = actor.actor_id AND film.title = 'ACADEMY DINOSAUR'
```

```go
a, fa, f := sq.New[ACTOR](""), sq.New[FILM_ACTOR](""), sq.New[FILM]("")
_, err := sq.Exec(db, sq.SQLite.
    Update(a).
    Set(a.LAST_NAME.SetString("DINO")).
    From(fa).
    Join(f, f.FILM_ID.Eq(fa.FILM_ID)).
    Where(
        fa.ACTOR_ID.Eq(a.ACTOR_ID),
        f.TITLE.EqString("ACADEMY DINOSAUR"),
    ),
)
```

#### Delete with Join #sqlite-delete-with-join

This is not technically an SQLite-specific feature as it uses a plain subquery to achieve a Delete with Join. Other databases have their own dialect-specific way of doing this, but this method works across every database and as such I prefer it over the others.

```sql
DELETE FROM actor
WHERE EXISTS (
    SELECT 1
    FROM film_actor
    JOIN film ON film.film_id = film_actor.film_id
    WHERE film_actor.actor_id = actor.actor_id AND film.title = 'ACADEMY DINOSAUR'
)
```

```go
a, fa, f := sq.New[ACTOR](""), sq.New[FILM_ACTOR](""), sq.New[FILM]("")
_, err := sq.Exec(db, sq.SQLite.
    DeleteFrom(a).
    Where(sq.Exists(sq.
        SelectOne().
        From(fa).
        Join(f, f.FILM_ID.Eq(f.FILM_ID)).
        Where(
            fa.ACTOR_ID.Eq(a.ACTOR_ID),
            f.TITLE.EqString("ACADEMY DINOSAUR"),
        ),
    )),
)
```

#### Bulk Update #sqlite-bulk-update

```sql
UPDATE actor
SET
    first_name = tmp.first_name,
    last_name = tmp.last_name
FROM (
        SELECT 1 AS actor_id, 'PENELOPE' AS first_name, 'GUINESS' AS last_name
        UNION ALL
        SELECT 2, 'NICK', 'WAHLBERG'
        UNION ALL
        SELECT 3, 'ED', 'CHASE'
    ) AS tmp
WHERE tmp.actor_id = actor.actor_id
```

```go
a := sq.New[ACTOR]("")
tmp := sq.SelectValues{
    Alias:     "tmp",
    Columns:   []string{"actor_id", "first_name", "last_name"},
    RowValues: [][]any{
        {1, "PENELOPE", "GUINESS"},
        {2, "NICK", "WAHLBERG"},
        {3, "ED", "CHASE"},
    },
}
_, err := sq.Exec(db, sq.SQLite.
    Update(a).
    Set(
        a.FIRST_NAME.Set(tmp.Field("first_name")),
        a.LAST_NAME.Set(tmp.Field("last_name")),
    ).
    From(tmp).
    Where(tmp.Field("actor_id").Eq(a.ACTOR_ID)),
)
```

### Postgres-specific features #postgres-specific-features

#### DISTINCT ON #postgres-distinct-on

```sql
SELECT DISTINCT ON (a.first_name) a.first_name, a.last_name
FROM actor AS a
ORDER BY a.first_name
```

```go
a := sq.New[ACTOR]("a")
actors, err := sq.FetchAll(db, sq.Postgres.
    From(a).
    DistinctOn(a.FIRST_NAME).
    OrderBy(a.FIRST_NAME),
    func(row *sq.Row) Actor {
        return Actor{
            FirstName: row.String(a.FIRST_NAME),
            LastName:  row.String(a.LAST_NAME),
        }
    },
)
```

#### FETCH NEXT, WITH TIES #postgres-fetch-next-with-ties

```sql
SELECT a.first_name
FROM actor AS a
OFFSET 5
FETCH NEXT 10 ROWS WITH TIES
```

```go
a := sq.New[ACTOR]("a")
firstNames, err := sq.FetchAll(db, sq.Postgres.
    From(a).
    Offset(5).
    FetchNext(10).WithTies(),
    func(row *sq.Row) string {
        return row.String(a.FIRST_NAME)
    },
)
```

#### FOR UPDATE, FOR SHARE #postgres-for-update-for-share

**For Update**

```sql
SELECT a.actor_id, a.first_name, a.last_name
FROM actor AS a
WHERE a.first_name = 'DAN'
FOR UPDATE SKIP LOCKED
```

```go
actors, err := sq.FetchAll(db, sq.Postgres.
    From(a).
    Where(a.FIRST_NAME.EqString("DAN")).
    LockRows("FOR UPDATE SKIP LOCKED"),
    func(row *sq.Row) Actor {
        return Actor{
            ActorID:   row.IntField(a.ACTOR_ID),
            FirstName: row.StringField(a.FIRST_NAME),
            LastName:  row.StringField(a.LAST_NAME),
        }
    },
)
```

**For Share**

```sql
SELECT a.actor_id, a.first_name, a.last_name
FROM actor AS a
WHERE a.first_name = 'DAN'
FOR SHARE
```

```go
actors, err := sq.FetchAll(db, sq.Postgres.
    From(a).
    Where(a.FIRST_NAME.EqString("DAN")).
    LockRows("FOR SHARE"),
    func(row *sq.Row) Actor {
        return Actor{
            ActorID:   row.IntField(a.ACTOR_ID),
            FirstName: row.StringField(a.FIRST_NAME),
            LastName:  row.StringField(a.LAST_NAME),
        }
    },
)
```

#### RETURNING #postgres-returning

```sql
INSERT INTO actor
    (first_name, last_name)
VALUES
    ('PENELOPE', 'GUINESS'),
    ('NICK', 'WAHLBERG'),
    ('ED', 'CHASE')
RETURNING
    actor.actor_id, actor.first_name, actor.last_name
```

```go
a := sq.New[ACTOR]("")
actors, err := sq.FetchAll(db, sq.Postgres.
    InsertInto(a).
    Columns(a.FIRST_NAME, a.LAST_NAME).
    Values("PENELOPE", "GUINESS").
    Values("NICK", "WAHLBERG").
    Values("ED", "CHASE"),
    func(row *sq.Row) Actor {
        return Actor{
            ActorID:   row.IntField(a.ACTOR_ID),
            FirstName: row.StringField(a.FIRST_NAME),
            LastName:  row.StringField(a.LAST_NAME),
        }
    },
)
```

#### Insert ignore duplicates #postgres-insert-ignore-duplicates

```sql
INSERT INTO actor
    (actor_id, first_name, last_name)
VALUES
    (1, 'PENELOPE', 'GUINESS'),
    (2, 'NICK', 'WAHLBERG'),
    (3, 'ED', 'CHASE')
ON CONFLICT DO NOTHING
```

```go
a := sq.New[ACTOR]("")
_, err := sq.Exec(db, sq.Postgres.
    InsertInto(a).
    Columns(a.ACTOR_ID, a.FIRST_NAME, a.LAST_NAME).
    Values(1, "PENELOPE", "GUINESS").
    Values(2, "NICK", "WAHLBERG").
    Values(3, "ED", "CHASE").
    OnConflict().DoNothing(),
)
```

#### Upsert #postgres-upsert

```sql
INSERT INTO actor
    (actor_id, first_name, last_name)
VALUES
    (1, 'PENELOPE', 'GUINESS'),
    (2, 'NICK', 'WAHLBERG'),
    (3, 'ED', 'CHASE')
ON CONFLICT (actor_id) DO UPDATE SET
    first_name = EXCLUDED.first_name,
    last_name = EXCLUDED.last_name
```

```go
a := sq.New[ACTOR]("")
_, err := sq.Exec(db, sq.Postgres.
    InsertInto(a).
    Columns(a.ACTOR_ID, a.FIRST_NAME, a.LAST_NAME).
    Values(1, "PENELOPE", "GUINESS").
    Values(2, "NICK", "WAHLBERG").
    Values(3, "ED", "CHASE").
    OnConflict(a.ACTOR_ID).DoUpdateSet(
        a.FIRST_NAME.Set(a.FIRST_NAME.WithPrefix("EXCLUDED")),
        a.LAST_NAME.Set(a.LAST_NAME.WithPrefix("EXLCUDED")),
    ),
)
```

#### Update with Join #postgres-update-with-join

```sql
UPDATE actor
SET last_name = 'DINO'
FROM film_actor
JOIN film ON film.film_id = film_actor.film_id
WHERE film_actor.actor_id = actor.actor_id AND film.title = 'ACADEMY DINOSAUR'
```

```go
a, fa, f := sq.New[ACTOR](""), sq.New[FILM_ACTOR](""), sq.New[FILM]("")
_, err := sq.Exec(db, sq.Postgres.
    Update(a).
    Set(a.LAST_NAME.SetString("DINO")).
    From(fa).
    Join(f, f.FILM_ID.Eq(fa.FILM_ID)).
    Where(
        fa.ACTOR_ID.Eq(a.ACTOR_ID),
        f.TITLE.EqString("ACADEMY DINOSAUR"),
    ),
)
```

#### Delete with Join #postgres-delete-with-join

```sql
DELETE FROM actor
USING film_actor
JOIN film ON film.film_id = film_actor.film_id
WHERE film_actor.actor_id = actor.actor_id AND film.title = 'ACADEMY DINOSAUR'
```

```go
a, fa, f := sq.New[ACTOR](""), sq.New[FILM_ACTOR](""), sq.New[FILM]("")
_, err := sq.Exec(db, sq.Postgres.
    DeleteFrom(a).
    Using(fa).
    Join(f, f.FILM_ID.Eq(fa.FILM_ID)).
    Where(
        fa.ACTOR_ID.Eq(a.ACTOR_ID),
        f.TITLE.EqString("ACADEMY DINOSAUR"),
    ),
)
```

#### Bulk Update #postgres-bulk-update

```sql
UPDATE actor
SET
    first_name = tmp.first_name,
    last_name = tmp.last_name
FROM (VALUES
        (1, 'PENELOPE', 'GUINESS'),
        (2, 'NICK', 'WAHLBERG'),
        (3, 'ED', 'CHASE')
    ) AS tmp (actor_id, first_name, last_name)
WHERE tmp.actor_id = actor.actor_id
```

```go
a := sq.New[ACTOR]("")
tmp := sq.TableValues{
    Alias:     "tmp",
    Columns:   []string{"actor_id", "first_name", "last_name"},
    RowValues: [][]any{
        {1, "PENELOPE", "GUINESS"},
        {2, "NICK", "WAHLBERG"},
        {3, "ED", "CHASE"},
    },
}
_, err := sq.Exec(db, sq.Postgres.
    Update(a).
    Set(
        a.FIRST_NAME.Set(tmp.Field("first_name")),
        a.LAST_NAME.Set(tmp.Field("last_name")),
    ).
    From(tmp).
    Where(tmp.Field("actor_id").Eq(a.ACTOR_ID)),
)
```

### MySQL-specific features #mysql-specific-features

#### FOR UPDATE, FOR SHARE #mysql-for-update-for-share

**For Update**

```sql
SELECT a.actor_id, a.first_name, a.last_name
FROM actor AS a
WHERE a.first_name = 'DAN'
FOR UPDATE SKIP LOCKED
```

```go
actors, err := sq.FetchAll(db, sq.MySQL.
    From(a).
    Where(a.FIRST_NAME.EqString("DAN")).
    LockRows("FOR UPDATE SKIP LOCKED"),
    func(row *sq.Row) Actor {
        return Actor{
            ActorID:   row.IntField(a.ACTOR_ID),
            FirstName: row.StringField(a.FIRST_NAME),
            LastName:  row.StringField(a.LAST_NAME),
        }
    },
)
```

**For Share**

```sql
SELECT a.actor_id, a.first_name, a.last_name
FROM actor AS a
WHERE a.first_name = 'DAN'
FOR SHARE
```

```go
actors, err := sq.FetchAll(db, sq.MySQL.
    From(a).
    Where(a.FIRST_NAME.EqString("DAN")).
    LockRows("FOR SHARE"),
    func(row *sq.Row) Actor {
        return Actor{
            ActorID:   row.IntField(a.ACTOR_ID),
            FirstName: row.StringField(a.FIRST_NAME),
            LastName:  row.StringField(a.LAST_NAME),
        }
    },
)
```

#### LastInsertId #mysql-last-insert-id

```sql
INSERT INTO actor (first_name, last_name) VALUES ('PENELOPE', 'GUINESS');
SELECT last_insert_id();
```

```go
a := sq.New[ACTOR]("")
result, err := sq.Exec(db, sq.MySQL.
    InsertInto(a).
    Columns(a.FIRST_NAME, a.LAST_NAME).
    Values("PENELOPE", "GUINESS"),
)
if err != nil {
}
fmt.Println(result.LastInsertId) // int64
```

#### Insert ignore duplicates #mysql-insert-ignore-duplicates

**ON DUPLICATE KEY UPDATE field = field**

MySQL lacks ON DUPLICATE KEY DO NOTHING but assigning a field to itself is the closest thing we can get. If a field is assigned to itself, MySQL doesn't actually trigger an update (making it do nothing).

```sql
INSERT INTO actor
    (actor_id, first_name, last_name)
VALUES
    (1, 'PENELOPE', 'GUINESS'),
    (2, 'NICK', 'WAHLBERG'),
    (3, 'ED', 'CHASE')
ON DUPLICATE KEY UPDATE
    actor.actor_id = actor.actor_id
```

```go
a := sq.New[ACTOR]("")
_, err := sq.Exec(db, sq.MySQL.
    InsertInto(a).
    Columns(a.ACTOR_ID, a.FIRST_NAME, a.LAST_NAME).
    Values(1, "PENELOPE", "GUINESS").
    Values(2, "NICK", "WAHLBERG").
    Values(3, "ED", "CHASE").
    OnDuplicateKeyUpdate(
        a.ACTOR_ID.Set(a.ACTOR_ID),
    ),
)
```

**INSERT IGNORE**

INSERT IGNORE will ignore all kinds of errors (such as foreign key violations) so use only if you really, really don't care if an INSERT fails.

```sql
INSERT IGNORE INTO actor
    (actor_id, first_name, last_name)
VALUES
    (1, 'PENELOPE', 'GUINESS'),
    (2, 'NICK', 'WAHLBERG'),
    (3, 'ED', 'CHASE')
```

```go
a := sq.New[ACTOR]("")
_, err := sq.Exec(db, sq.MySQL.
    InsertIgnoreInto(a).
    Columns(a.ACTOR_ID, a.FIRST_NAME, a.LAST_NAME).
    Values(1, "PENELOPE", "GUINESS").
    Values(2, "NICK", "WAHLBERG").
    Values(3, "ED", "CHASE"),
)
```

#### Upsert #mysql-upsert

**Row Alias (MySQL 8.0+ onwards)**

```sql
INSERT INTO actor
    (actor_id, first_name, last_name)
VALUES
    (1, 'PENELOPE', 'GUINESS'),
    (2, 'NICK', 'WAHLBERG'),
    (3, 'ED', 'CHASE')
AS new
ON DUPLICATE KEY UPDATE
    actor.first_name = new.first_name,
    actor.last_name = new.last_name
```

```go
a := sq.New[ACTOR]("")
_, err := sq.Exec(db, sq.MySQL.
    InsertInto(a).
    Columns(a.ACTOR_ID, a.FIRST_NAME, a.LAST_NAME).
    Values(1, "PENELOPE", "GUINESS").
    Values(2, "NICK", "WAHLBERG").
    Values(3, "ED", "CHASE").
    As("new").
    OnDuplicateKeyUpdate(
        a.FIRST_NAME.Set(a.FIRST_NAME.WithPrefix("new")),
        a.LAST_NAME.Set(a.LAST_NAME.WithPrefix("new")),
    ),
)
```

**VALUES()**

```sql
INSERT INTO actor
    (actor_id, first_name, last_name)
VALUES
    (1, 'PENELOPE', 'GUINESS'),
    (2, 'NICK', 'WAHLBERG'),
    (3, 'ED', 'CHASE')
ON DUPLICATE KEY UPDATE
    actor.first_name = VALUES(first_name),
    actor.last_name = VALUES(last_name)
```

```go
a := sq.New[ACTOR]("")
_, err := sq.Exec(db, sq.MySQL.
    InsertInto(a).
    Columns(a.ACTOR_ID, a.FIRST_NAME, a.LAST_NAME).
    Values(1, "PENELOPE", "GUINESS").
    Values(2, "NICK", "WAHLBERG").
    Values(3, "ED", "CHASE").
    OnDuplicateKeyUpdate(
        a.FIRST_NAME.Setf("VALUES({})", a.FIRST_NAME.WithPrefix("")),
        a.LAST_NAME.Setf("VALUES({})", a.LAST_NAME.WithPrefix("")),
    ),
)
```

#### Update with Join #mysql-update-with-join

```sql
UPDATE actor
JOIN film_actor ON film_actor.actor_id = actor.actor_id
JOIN film ON film.film_id = film_actor.film_id
SET actor.last_name = 'DINO'
WHERE film.title = 'ACADEMY DINOSAUR'
```

```go
a, fa, f := sq.New[ACTOR](""), sq.New[FILM_ACTOR](""), sq.New[FILM]("")
_, err := sq.Exec(db, sq.MySQL.
    Update(a).
    Join(fa, fa.ACTOR_ID.Eq(a.ACTOR_ID)).
    Join(f, f.FILM_ID.Eq(fa.FILM_ID)).
    Set(a.LAST_NAME.SetString("DINO")).
    Where(f.TITLE.EqString("ACADEMY DINOSAUR")),
)
```

#### Delete with Join #mysql-delete-with-join

```sql
DELETE actor
FROM actor
JOIN film_actor ON film_actor.actor_id = actor.actor_id
JOIN film ON film.film_id = film_actor.film_id
WHERE film.title = 'ACADEMY DINOSAUR'
```

```go
a, fa, f := sq.New[ACTOR](""), sq.New[FILM_ACTOR](""), sq.New[FILM]("")
_, err := sq.Exec(db, sq.MySQL.
    Delete(a).
    From(a)
    Join(fa, fa.ACTOR_ID.Eq(a.ACTOR_ID)).
    Join(f, f.FILM_ID.Eq(fa.FILM_ID)).
    Where(f.TITLE.EqString("ACADEMY DINOSAUR")),
)
```

#### Bulk Update #mysql-bulk-update

```sql
UPDATE actor
JOIN (VALUES
        ROW(1, 'PENELOPE', 'GUINESS'),
        ROW(2, 'NICK', 'WAHLBERG'),
        ROW(3, 'ED', 'CHASE')
    ) AS tmp (actor_id, first_name, last_name) ON tmp.actor_id = actor.actor_id
SET
    first_name = tmp.first_name,
    last_name = tmp.last_name
```

```go
a := sq.New[ACTOR]("")
tmp := sq.TableValues{
    Alias:     "tmp",
    Columns:   []string{"actor_id", "first_name", "last_name"},
    RowValues: [][]any{
        {1, "PENELOPE", "GUINESS"},
        {2, "NICK", "WAHLBERG"},
        {3, "ED", "CHASE"},
    },
}
_, err := sq.Exec(db, sq.MySQL.
    Update(a).
    Join(tmp, tmp.Field("actor_id").Eq(a.ACTOR_ID)).
    Set(
        a.FIRST_NAME.Set(tmp.Field("first_name")),
        a.LAST_NAME.Set(tmp.Field("last_name")),
    ),
)
```

### SQLServer-specific features #sqlserver-specific-features

#### TOP, WITH TIES #sqlserver-top-with-ties

```sql
SELECT TOP 10 WITH TIES a.first_name
FROM actor AS a
```

```go
a := sq.New[ACTOR]("a")
firstNames, err := sq.FetchAll(db, sq.SQLServer.
    From(a).
    Top(10).WithTies(),
    func(row *sq.Row) string {
        return row.String(a.FIRST_NAME)
    },
)
```

#### OUTPUT #sqlserver-output

```sql
INSERT INTO actor
    (first_name, last_name)
OUTPUT
    INSERTED.actor_id, INSERTED.first_name, INSERTED.last_name
VALUES
    ('PENELOPE', 'GUINESS'),
    ('NICK', 'WAHLBERG'),
    ('ED', 'CHASE')
```

```go
a := sq.New[ACTOR]("")
actors, err := sq.FetchAll(db, sq.SQLServer.
    InsertInto(a).
    Columns(a.FIRST_NAME, a.LAST_NAME).
    Values("PENELOPE", "GUINESS").
    Values("NICK", "WAHLBERG").
    Values("ED", "CHASE"),
    func(row *sq.Row) Actor {
        return Actor{
            ActorID:   row.IntField(a.ACTOR_ID),
            FirstName: row.StringField(a.FIRST_NAME),
            LastName:  row.StringField(a.LAST_NAME),
        }
    },
)
```

**INSERTED.* vs DELETED.***

- For Insert queries, OUTPUT fields to use the INSERTED.\* prefix.
- For Delete queries, OUTPUT fields use the DELETED.\* prefix.
- For Update queries, OUTPUT fields use the INSERTED.\* prefix.

Technically both INSERTED.\* and DELETED.\* fields are supported for Update queries, but sq only supports INSERTED.\* because that is how RETURNING behaves in SQLite and Postgres.

#### Insert ignore duplicates #sqlserver-insert-ignore-duplicates

This is technically not an SQL Server-specific feature as SQL Server completely does not support this. You have to employ a workaround using INSERT with SELECT ([https://stackoverflow.com/a/10703792](https://stackoverflow.com/a/10703792)). I'm including the workaround here for completion's sake.

```sql
-- Insert rows that don't exist.
INSERT INTO actor
    (actor_id, first_name, last_name)
SELECT
    actor_id, first_name, last_name
FROM (
    VALUES
        (1, 'PENELOPE', 'GUINESS'),
        (2, 'NICK', 'WAHLBERG'),
        (3, 'ED', 'CHASE')
    ) AS rowvalues (actor_id, first_name, last_name)
WHERE NOT EXISTS (
    SELECT 1 FROM actor WHERE actor.actor_id = rowvalues.actor_id
)
```

```go
a := sq.New[ACTOR]("")
// Insert rows that don't exist.
_, err := sq.Exec(db, sq.SQLServer.
    InsertInto(a).
    Columns(a.ACTOR_ID, a.FIRST_NAME, a.LAST_NAME).
    Select(sq.Queryf("SELECT actor_id, first_name, last_name"+
        "FROM (VALUES {}) AS rowvalues (actor_id, first_name, last_name)"+
        "WHERE NOT EXISTS (SELECT 1 FROM actor WHERE actor.actor_id = rowvalues.actor_id)",
        sq.RowValues{
            {1, "PENELOPE", "GUINESS"},
            {2, "NICK", "WAHLBERG"},
            {3, "ED", "CHASE"},
        },
    )),
)
```

#### Upsert #sqlserver-upsert

This is technically not an SQL Server-specific feature as SQL Server does not support this. You have to employ a 2-step workaround using an UPDATE with JOIN + an INSERT with SELECT ([https://sqlperformance.com/2020/09/locking/upsert-anti-pattern](https://sqlperformance.com/2020/09/locking/upsert-anti-pattern)). I'm including the workaround here for completion's sake.

Avoid using MERGE for upserting.
- [https://www.mssqltips.com/sqlservertip/3074/use-caution-with-sql-servers-merge-statement/](https://www.mssqltips.com/sqlservertip/3074/use-caution-with-sql-servers-merge-statement/)
- [https://michaeljswart.com/2021/08/what-to-avoid-if-you-want-to-use-merge/](https://michaeljswart.com/2021/08/what-to-avoid-if-you-want-to-use-merge/)

```sql
-- Update rows that exist.
UPDATE actor
SET
    first_name = rowvalues.first_name,
    last_name = rowvalues.last_name
FROM
    actor
    JOIN (VALUES
        (1, 'PENELOPE', 'GUINESS'),
        (2, 'NICK', 'WAHLBERG'),
        (3, 'ED', 'CHASE')
    ) AS rowvalues (actor_id, first_name, last_name) ON rowvalues.actor_id = actor.actor_id;

-- Insert rows that don't exist.
INSERT INTO actor
    (actor_id, first_name, last_name)
SELECT
    actor_id, first_name, last_name
FROM (VALUES
        (1, 'PENELOPE', 'GUINESS'),
        (2, 'NICK', 'WAHLBERG'),
        (3, 'ED', 'CHASE')
    ) AS rowvalues (actor_id, first_name, last_name)
WHERE NOT EXISTS (
    SELECT 1 FROM actor WHERE actor.actor_id = rowvalues.actor_id
);
```

```go
a := sq.New[ACTOR]("")
// Update rows that exist.
_, err := sq.Exec(db, sq.SQLServer.
    Update(a).
    Set(
        a.FIRST_NAME.Setf("rowvalues.first_name"),
        a.LAST_NAME.Setf("rowvalues.last_name"),
    ).
    From(a).
    Join(sq.
        Queryf("VALUES {}", sq.RowValues{
            {1, "PENELOPE", "GUINESS"},
            {2, "NICK", "WAHLBERG"},
            {3, "ED", "CHASE"},
        }).
        As("rowvalues (actor_id, first_name, last_name)"),
        sq.Expr("rowvalues.actor_id").Eq(a.ACTOR_ID),
    )
)

// Insert rows that don't exist.
_, err := sq.Exec(db, sq.SQLServer.
    InsertInto(a).
    Columns(a.ACTOR_ID, a.FIRST_NAME, a.LAST_NAME).
    Select(sq.Queryf("SELECT actor_id, first_name, last_name"+
        "FROM (VALUES {}) AS rowvalues (actor_id, first_name, last_name)"+
        "WHERE NOT EXISTS (SELECT 1 FROM actor WHERE actor.actor_id = rowvalues.actor_id)",
        sq.RowValues{
            {1, "PENELOPE", "GUINESS"},
            {2, "NICK", "WAHLBERG"},
            {3, "ED", "CHASE"},
        },
    )),
)
```

#### Update with Join #sqlserver-update-with-join

```sql
UPDATE actor
SET last_name = 'DINO'
FROM actor
JOIN film_actor ON film_actor.actor_id = actor.actor_id
JOIN film ON film.film_id = film_actor.film_id
WHERE film.title = 'ACADEMY DINOSAUR'
```

```go
a, fa, f := sq.New[ACTOR](""), sq.New[FILM_ACTOR](""), sq.New[FILM]("")
_, err := sq.Exec(db, sq.SQLServer.
    Update(a).
    Set(a.LAST_NAME.SetString("DINO")).
    From(a).
    Join(fa, fa.ACTOR_ID.Eq(a.ACTOR_ID)).
    Join(f, f.FILM_ID.Eq(fa.FILM_ID)).
    Where(f.TITLE.EqString("ACADEMY DINOSAUR")),
)
```

#### Delete with Join #sqlserver-delete-with-join

```sql
DELETE actor
FROM actor
JOIN film_actor ON film_actor.actor_id = actor.actor_id
JOIN film ON film.film_id = film_actor.film_id
WHERE film.title = 'ACADEMY DINOSAUR'
```

```go
a, fa, f := sq.New[ACTOR](""), sq.New[FILM_ACTOR](""), sq.New[FILM]("")
_, err := sq.Exec(db, sq.SQLServer.
    Delete(a).
    From(a)
    Join(fa, fa.ACTOR_ID.Eq(a.ACTOR_ID)).
    Join(f, f.FILM_ID.Eq(fa.FILM_ID)).
    Where(f.TITLE.EqString("ACADEMY DINOSAUR")),
)
```

#### Bulk Update #sqlserver-bulk-update

```sql
UPDATE actor
SET
    first_name = tmp.first_name,
    last_name = tmp.last_name
FROM
    actor
    JOIN (VALUES
        (1, 'PENELOPE', 'GUINESS'),
        (2, 'NICK', 'WAHLBERG'),
        (3, 'ED', 'CHASE')
    ) AS tmp (actor_id, first_name, last_name) ON tmp.actor_id = actor.actor_id
```

```go
a := sq.New[ACTOR]("")
tmp := sq.TableValues{
    Alias:     "tmp",
    Columns:   []string{"actor_id", "first_name", "last_name"},
    RowValues: [][]any{
        {1, "PENELOPE", "GUINESS"},
        {2, "NICK", "WAHLBERG"},
        {3, "ED", "CHASE"},
    },
}
_, err := sq.Exec(db, sq.SQLServer.
    Update(a).
    Set(
        a.FIRST_NAME.Set(tmp.Field("first_name")),
        a.LAST_NAME.Set(tmp.Field("last_name")),
    ).
    From(a)
    Join(tmp, tmp.Field("actor_id").Eq(a.ACTOR_ID)),
)
```

## Working with arrays, enums, JSON and UUID #arrays-enums-json-uuid

### Arrays #arrays

Slices of primitive types (`[]string`, `[]int64`, `[]int32`, `[]float64`, `[]float32`, `[]bool`) can be saved into the database. For Postgres, it will be saved as an ARRAY (TEXT[], INT[], BIGINT[], NUMERIC[] or BOOLEAN[]). For other databases, it will be saved as a JSON array.

**Writing arrays**

```go
// Raw SQL
_, err := sq.Exec(db, sq.
    Queryf("INSERT INTO posts (title, body, tags) VALUES {}", sq.RowValue{
        "Hello World!",
        "This is my first blog post.",
        sq.ArrayValue([]string{"introduction", "hello-world", "meta"}),
    }).
    SetDialect(sq.DialectPostgres),
)

// Query Builder
p := sq.New[POSTS]("")
_, err := sq.Exec(db, sq.
    InsertInto(p).
    ColumnValues(func(col *sq.Column) error {
        col.SetString(p.TITLE, "Hello World!")
        col.SetString(p.BODY, "This is my first blog post.")
        col.SetArray(p.TAGS, []string{"introduction", "hello-world", "meta"})
    }).
    SetDialect(sq.DialectPostgres),
)
```

**Reading arrays**

```go
// Raw SQL
posts, err := sq.FetchAll(db, sq.
    Queryf("SELECT {*} FROM posts WHERE post_id IN ({})", []int{1, 2, 3}).
    SetDialect(sq.DialectPostgres),
    func(row *sq.Row) Post {
        var post Post
        post.Title = row.String("title")
        post.Body = row.String("body")
        row.Array(&post.Tags, "tags")
        return post
    },
)

// Query Builder
p := sq.New[POSTS]("")
posts, err := sq.FetchAll(db, sq.
    From(p).
    Where(p.POST_ID.In([]int{1, 2, 3})).
    SetDialect(sq.DialectPostgres),
    func(row *sq.Row) Post {
        var post Post
        post.Title = row.StringField(p.TITLE)
        post.Body = row.StringField(p.BODY)
        row.ArrayField(&post.Tags, p.TAGS)
        return post
    },
)
```

### Enums #enums

A Go type is considered an enum if it implements the `Enumeration` interface:

```go
type Enumeration interface{
    Enumerate() []string
}
```

As an example, this is how an int-based enum and a string-based enum would be implemented:

```go
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
```

```go
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
```

By implementing the `Enumeration` interface, you automatically get enum type validation when writing enums to and reading enums from the database.
- If you try to write an enum value to the database that isn't present in the `Enumerate()` slice, it will be flagged as an error.
- If the database returns an enum value that isn't present in the `Enumerate()` slice, it will be flagged as an error.

**Writing enums**

```go
// Raw SQL
_, err := sq.Exec(db, sq.
    Queryf("INSERT INTO fruits (name, color) VALUES {}", sq.RowValue{
        "apple",
        sq.EnumValue(ColorRed),
    }).
    SetDialect(sq.DialectPostgres),
)

// Query Builder
f := sq.New[FRUITS]("")
_, err := sq.Exec(db, sq.
    InsertInto(f).
    ColumnValues(func(col *sq.Column) error {
        col.SetString(f.NAME, "apple")
        col.SetEnum(f.COLOR, ColorRed)
    }).
    SetDialect(sq.DialectPostgres),
)
```

**Reading enums**

```go
// Raw SQL
fruits, err := sq.FetchAll(db, sq.
    Queryf("SELECT {*} FROM fruits WHERE fruit_id IN ({})", []int{1, 2, 3}).
    SetDialect(sq.DialectPostgres),
    func(row *sq.Row) Fruit {
        var fruit Fruit
        fruit.Name = row.String("name")
        row.Enum(&fruit.Color, "color")
        return fruit
    },
)

// Query Builder
f := sq.New[FRUITS]("")
posts, err := sq.FetchAll(db, sq.
    From(f).
    Where(f.FRUIT_ID.In([]int{1, 2, 3})).
    SetDialect(sq.DialectPostgres),
    func(row *sq.Row) Fruit {
        var fruit Fruit
        fruit.Name = row.StringField(f.NAME)
        row.EnumField(&fruit.Color, f.COLOR)
        return fruit
    },
)
```

### JSON #json

Any Go type that works with `json.Marshal` and `json.Unmarshal` can be saved into the database. For Postgres, it will be saved as JSONB. For MySQL, it will be saved as JSON. For other databases, it will be saved as a JSON string.

**Writing JSON**

```go
// Raw SQL
_, err := sq.Exec(db, sq.
    Queryf("INSERT INTO products (name, price, attributes) VALUES {}", sq.RowValue{
        "Sleeping Bag",
        89.99,
        sq.JSONValue(map[string]any{
            "Length (cm)":    220,
            "Width (cm)":     150,
            "Weight (kg)":    2.96,
            "Color":          "Lake Blue",
            "Fill Material":  "190T Pongee",
            "Outer Material": "Polyester",
        }),
    }).
    SetDialect(sq.DialectPostgres),
)

// Query Builder
p := sq.New[PRODUCTS]("")
_, err := sq.Exec(db, sq.
    InsertInto(p).
    ColumnValues(func(col *sq.Column) error {
        col.SetString(p.NAME, "Sleeping Bag")
        col.SetFloat64(p.PRICE, 89.99)
        col.SetJSON(p.ATTRIBUTES, map[string]any{
            "Length (cm)":    220,
            "Width (cm)":     150,
            "Weight (kg)":    2.96,
            "Color":          "Lake Blue",
            "Fill Material":  "190T Pongee",
            "Outer Material": "Polyester",
        })
    }).
    SetDialect(sq.DialectPostgres),
)
```

**Reading JSON**

```go
// Raw SQL
products, err := sq.FetchAll(db, sq.
    Queryf("SELECT {*} FROM products WHERE product_id IN ({})", []int{1, 2, 3}).
    SetDialect(sq.DialectPostgres),
    func(row *sq.Row) Product {
        var product Product
        product.Name = row.String("name")
        product.Price = row.Float64("price")
        row.JSON(&product.Attributes, "attributes")
        return product
    },
)

// Query Builder
p := sq.New[PRODUCTS]("")
posts, err := sq.FetchAll(db, sq.
    From(p).
    Where(p.PRODUCT_ID.In([]int{1, 2, 3})).
    SetDialect(sq.DialectPostgres),
    func(row *sq.Row) Product {
        var product Product
        product.Name = row.StringField(p.NAME)
        product.Price = row.Float64Field(p.PRICE)
        row.JSONField(&product.Attributes, p.ATTRIBUTES)
        return product
    },
)
```

### UUID #uuid

Any Go type whose underlying type is `[16]byte` can be saved as a UUID into the database. For Postgres, it will be saved as UUID. For other databases, it will be saved as a BINARY(16).

It is likely that the Go UUID library you are using already implements sql.Scanner and driver.Valuer (e.g. [github.com/google/uuid](https://github.com/google/uuid)). You can choose to rely on their built-in SQL behaviour:

- Instead of wrapping the uuid in sq.UUIDValue(), just use the uuid directly.
- Instead of calling col.SetUUID(), just call col.Set().
- Instead of calling row.UUID()/row.UUIDField(), just call row.Scan()/row.ScanField().

The main benefit of using this library's built-in UUID helpers is to have UUID reading/writing work identically across database dialects: for Postgres, if you want to save a UUID you must give it a UUID string. For other databases, if you want to save a UUID as a BINARY(16) you must give it raw UUID bytes. Using this library's UUID helpers means you don't have to manually account for this UUID string/bytes disparity between Postgres and the other DBs.

**Writing UUID**

```go
userID, err := uuid.Parse("d619cde3-7661-4b6e-928e-4d5b239a18a9")
if err != nil {
}

// Raw SQL
_, err = sq.Exec(db, sq.
    Queryf("INSERT INTO users (user_id, name, email) VALUES {}", sq.RowValue{
        sq.UUIDValue(userID),
        "John Doe",
        "john_doe@email.com",
    }).
    SetDialect(sq.DialectPostgres),
)

// Query Builder
u := sq.New[USERS]("")
_, err := sq.Exec(db, sq.
    InsertInto(u).
    ColumnValues(func(col *sq.Column) error {
        col.SetUUID(u.USER_ID, userID)
        col.SetString(u.NAME, "John Doe")
        col.SetString(u.EMAIL, "john_doe@email.com")
    }).
    SetDialect(sq.DialectPostgres),
)
```

**Reading UUID**

```go
// Raw SQL
users, err := sq.FetchAll(db, sq.
    Queryf("SELECT {*} FROM users WHERE email IS NOT NULL").
    SetDialect(sq.DialectPostgres),
    func(row *sq.Row) User {
        var user User
        row.UUID(&user.UserID, "user_id")
        user.Name = row.String("name")
        user.Email = row.String("email")
        return user
    },
)

// Query Builder
u := sq.New[USERS]("")
posts, err := sq.FetchAll(db, sq.
    From(u).
    Where(u.EMAIL.IsNotNull()).
    SetDialect(sq.DialectPostgres),
    func(row *sq.Row) User {
        var user User
        row.UUIDField(&user.UserID, u.USER_ID)
        user.Name = row.StringField(u.NAME)
        user.Email = row.StringField(u.EMAIL)
        return user
    },
)
```

## Logging #logging

Queries can be logged wrapping the database with `sq.Log()` or `sq.VerboseLog()`.

**sq.Log()**

```go
// With logging                    ↓ wrap the db
firstName, err := sq.FetchOne(sq.Log(db), sq.
    Queryf("SELECT {*} FROM actor WHERE last_name IN ({})", []string{"AKROYD", "ALLEN", "WILLIAMS"}),
    func(row *sq.Row) string {
        return row.String("first_name")
    },
)
```

```shell
2022/02/06 15:34:36 [OK] SELECT first_name FROM actor WHERE last_name IN (?, ?, ?) | timeTaken=9.834µs rowCount=9 caller=/Users/bokwoon/Documents/sq/fetch_exec_test.go:74:sq.TestFetchExec
```

**sq.VerboseLog()**

```go
// With verbose logging                ↓ wrap the db
firstName, err := sq.FetchOne(sq.VerboseLog(db), sq.
    Queryf("SELECT {*} FROM actor WHERE last_name IN ({})", []string{"AKROYD", "ALLEN", "WILLIAMS"}),
    func(row *sq.Row) string {
        return row.String("first_name")
    },
)
```

```shell
2022/02/06 15:34:36 [OK] timeTaken=9.834µs rowCount=9 caller=/Users/bokwoon/Documents/sq/fetch_exec_test.go:74:sq.TestFetchExec
----[ Executing query ]----
SELECT first_name FROM actor WHERE last_name IN (?, ?, ?) []interface {}{"AKROYD", "ALLEN", "WILLIAMS"}
----[ with bind values ]----
SELECT first_name FROM actor WHERE last_name IN ('AKROYD', 'ALLEN', 'WILLIAMS')
----[ Fetched result ]----
----[ Row 1 ]----
first_name: 'CHRISTIAN'
----[ Row 2 ]----
first_name: 'SEAN'
----[ Row 3 ]----
first_name: 'KIRSTEN'
----[ Row 4 ]----
first_name: 'CUBA'
----[ Row 5 ]----
first_name: 'MORGAN'
...
(Fetched 9 rows)
```

### Logging without manual sq.Log() wrapping #logging-without-manual-wrapping

To log every query without manually wrapping it in sq.Log(), create a custom DB type that implements the `SqLogger` interface. This is what sq.Log() and sq.VerboseLog() do.

```go
type SqLogger interface {
    SqLogSettings(context.Context, *LogSettings)
    SqLogQuery(context.Context, QueryStats)
}
```

Here is an example, using a custom DB struct that embeds \*sql.DB and using the logger provided by sq.NewLogger().

```go
type MyDB struct {
    *sql.DB
}

var logger = sq.NewLogger(os.Stdout, "", log.Lshortflags, sq.LoggerConfig{
    ShowTimeTaken: true,
    HideArgs:      true,
})

func (db MyDB) SqLogSettings(ctx context.Context, settings *sq.LogSettings) {
    logger.SqLogSettings(ctx, settings)
}

func (db MyDB) SqLogQuery(ctx context.Context, stats sq.QueryStats) {
    logger.SqLogQuery(ctx, stats)
}

actors, err := sq.FetchAll(myDB, sq.
    Queryf("SELECT {*} FROM actor WHERE first_name = {}", "DAN").
    SetDialect(sq.DialectPostgres),
    func(row *sq.Row) Actor {
        return Actor{
            ActorID:   row.Int("actor_id"),
            FirstName: row.String("first_name"),
            LastName:  row.String("last_name"),
        }
    },
)
```

```shell
2022/02/06 15:34:36 [OK] SELECT actor_id, first_name, last_name FROM actor WHERE first_name = $1 | timeTaken=9.834µs rowCount=3
```

### Custom logger #custom-logger

A custom logger can also be used by creating [custom DB type that implements the `SqLogger` interface](#logging-without-manual-wrapping). The logging information is passed in as a `QueryStats` struct, which you can feed into the structured logger of your choice.

```go
// QueryStats represents the statistics from running a query.
type QueryStats struct {
    // Dialect of the query.
    Dialect string

    // Query string.
    Query string

    // Args slice provided with the query string.
    Args []any

    // Params maps param names back to arguments in the args slice (by index).
    Params map[string][]int

    // Err is the error from running the query.
    Err error

    // RowCount from running the query. Not valid for Exec().
    RowCount sql.NullInt64

    // RowsAffected by running the query. Not valid for
    // FetchOne/FetchAll/FetchCursor.
    RowsAffected sql.NullInt64

    // LastInsertId of the query.
    LastInsertId sql.NullInt64

    // Exists is the result of FetchExists().
    Exists sql.NullBool

    // When the query started at.
    StartedAt time.Time

    // Time taken by the query.
    TimeTaken time.Duration

    // The caller file where the query was invoked.
    CallerFile string

    // The line in the caller file that invoked the query.
    CallerLine int

    // The name of the function where the query was invoked.
    CallerFunction string

    // The results from running the query (if it was provided).
    Results string
}
```

As an example, we will create a custom database logger that outputs JSON and only logs if the query took longer than 1 second.

```go
type MyDB struct {
    *sql.DB
}

func (myDB MyDB) SqLogSettings(ctx context.Context, settings *sq.LogSettings) {
    settings.LogAsynchronously = false // Should the logging be dispatched in a separate goroutine?
    settings.IncludeTime = true        // Should timeTaken be included in the QueryStats?
    settings.IncludeCaller = true      // Should caller info be included in the QueryStats?
    settings.IncludeResults = 0        // The first how many rows of results should be included? Leave 0 to not include any results.
}

func (myDB MyDB) SqLogQuery(ctx context.Context, stats sq.QueryStats) {
    if stats.TimeTaken < time.Second {
        return
    }
    output := map[string]any{
        "query":     stats.Query,
        "args":      stats.Args,
        "caller":    stats.CallerFile + ":" + strconv.Itoa(stats.CallerLine)
        "timeTaken": stats.TimeTaken.String(),
    }
    b, err := json.MarshalIndent(output, "", "  ")
    if err != nil {
        log.Println(err.Error())
        return
    }
    log.Println("TOO SLOW! " + string(b))
}
```

```shell
2022/02/06 15:34:36 TOO SLOW! {
  "args": [
    1
  ],
  "caller": "/Users/bokwoon/Documents/sq/fetch_exec_test.go:74",
  "query": "SELECT actor_id, first_name, last_name FROM actor WHERE actor_id = ?",
  "timeTaken": "1.534s"
}
```

## Working with transactions #transactions

Fetch() and Exec() both accept an sq.DB interface, which represents something that can query the database.


```go
// *sql.Conn, *sql.DB and *sql.Tx all implement DB.
type DB interface {
    QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
    ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
    PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
}
```

To use an \*sql.Tx (or an \*sql.Conn), you can pass it in like a normal \*sql.DB.

```go
tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
if err != nil {
    return err
}
// good practice defer tx.Rollback first, if tx.Commit is called then this becomes a no-op.
defer tx.Rollback()

// do operation 1
_, err = sq.Exec(tx, q1)
if err != nil {
    return err
}

// do operation 2
_, err = sq.Exec(tx, q2)
if err != nil {
    return err
}

// do operation 3
_, err = sq.Exec(tx, q3)
if err != nil {
    return err
}

// If all goes well, commit. If anything wrong happened before reaching here we
// just bail and let defer tx.Rollback() kick in
err = tx.Commit()
if err != nil {
    return err
}
// if we reach here, success
```

## Compiling queries #compiling-queries

The cost of query building can be amortized by compiling queries down into a query string and args slice. Compiled queries are reused by supplying a different set of parameters each time you execute them. They can be executed safely in parallel.

### Providing rebindable params #rebindable-params

Any [named parameter](#ordinal-named-placeholders) e.g. `sql.Named()` passed to the query builder can be rebinded later by the compiled query. There are 9 variants of named parameters available.

<div class="table-wrapper">
<table>
<thead>
<tr>
    <th>Parameter</th>
    <th>Description</th>
</tr>
</thead>
<tbody>
<tr>
    <td>sql.Named(<code>name string</code>, <code>value any</code>)</td>
    <td>database/sql's named parameter type</td>
</tr>
<tr>
    <td>sq.Param(<code>name string</code>, <code>value any</code>)</td>
    <td>same as sql.Named, but satisfies the <code>Field</code> interface</td>
</tr>
<tr>
    <td>sq.BinaryParam(<code>name string</code>, <code>b []byte</code>)</td>
    <td>same as sql.Named, but satisfies the <code>Binary</code> interface</td>
</tr>
<tr>
    <td>sq.BooleanParam(<code>name string</code>, <code>b bool</code>)</td>
    <td>same as sql.Named, but satisfies the <code>Boolean</code> interface</td>
</tr>
<tr>
    <td>sq.IntParam(<code>name string</code>, <code>num int</code>)</td>
    <td>same as sql.Named, but satisfies the <code>Number</code> interface</td>
</tr>
<tr>
    <td>sq.Int64Param(<code>name string</code>, <code>num int64</code>)</td>
    <td>same as sql.Named, but satisfies the <code>Number</code> interface</td>
</tr>
<tr>
    <td>sq.Float64Param(<code>name string</code>, <code>num float64</code>)</td>
    <td>same as sql.Named, but satisfies the <code>Number</code> interface</td>
</tr>
<tr>
    <td>sq.StringParam(<code>name string</code>, <code>s string</code>)</td>
    <td>same as sql.Named, but satisfies the <code>String</code> interface</td>
</tr>
<tr>
    <td>sq.TimeParam(<code>name string</code>, <code>t time.Time</code>)</td>
    <td>same as sql.Named, but satisfies the <code>Time</code> interface</td>
</tr>
</tbody>
</table>
</div>

### Rebinding params #rebinding-params

To execute a compiled query, you need to rebind its params by providing it an `sq.Params{}`.

```go
// Compile the query.
q, err := sq.CompileFetch(sq.
    Queryf("SELECT {*} FROM actor WHERE first_name = {first_name}, last_name = {last_name}",
        sql.Named("first_name", nil), // first_name is a rebindable param, with default value nil
        sql.Named("last_name", nil),  // last_name is a rebindable param, with default value nil
    ).
    SetDialect(sq.DialectPostgres),
    func(row *sq.Row) Actor {
        return Actor{
            ActorID:   row.IntField("actor_id"),
            FirstName: row.StringFIeld("first_name"),
            LastName:  row.StringFIeld("last_name"),
        }
    },
)
if err != nil {
}

// Execute the compiled query.
actor, err := q.FetchOne(db, sq.Params{
    "first_name": "DAN",
    "last_name":  "TORN",
})
```

You must rebind every parameter in the query or else an error will be returned. If you wish to use a parameter's default value that was supplied during the building of the query, pass in the sentinel constant **sq.DefaultValue** as the value.

```go
// Execute the compiled query using the default values.
actor, err := q.FetchOne(db, sq.Params{
    "first_name": sq.DefaultValue,
    "last_name":  sq.DefaultValue,
})
```

### CompiledFetch example #compiled-fetch
```go
// getActor is compiled once on startup and is safe to use over and over again.
var getActor = func() *sq.CompiledFetch[ACTOR] {
    q, err := sq.CompileFetch(sq.
        From(a).
        Where(
            // actor_id is a rebindable param, with default value 0
            a.ACTOR_ID.Eq(sq.IntParam("actor_id", 0)),
        ).
        SetDialect(sq.DialectPostgres),
        func(row *sq.Row) Actor {
            return Actor{
                ActorID:   row.IntField(a.ACTOR_ID),
                FirstName: row.StringField(a.FIRST_NAME),
                LastName:  row.StringField(a.LAST_NAME),
            }
        },
    )
    if err != nil {
        panic(err)
    }
    return q
}()

func main() {
    db := openDB()

    actor, err := getActor.FetchOne(db, sq.Params{
        "actor_id": 1,
    })
    fmt.Println(actor) // {ActorID: 1, FirstName: "PENELOPE", LastName: "GUINESS"}

    actor, err = getActor.FetchOne(db, sq.Params{
        "actor_id": 2,
    })
    fmt.Println(actor) // {ActorID: 2, FirstName: "NICK", LastName: "WAHLBERG"}

    actor, err = getActor.FetchOne(db, sq.Params{
        "actor_id": 3,
    })
    fmt.Println(actor) // {ActorID: 3, FirstName: "ED", LastName: "CHASE"}
}
```

### CompiledExec example #compiled-exec
```go
// insertActor is compiled once on startup and is safe to use over and over again.
var insertActor = func() *CompiledExec {
    a := sq.New[ACTOR]("")
    q, err = sq.CompileExec(sq.
        InsertInto(a).
        ColumnValues(func(col *sq.Column) error {
            col.Set(a.ACTOR_ID, sql.Named("actor_id", nil))     // actor_id is a rebindable param, with default value nil
            col.Set(a.FIRST_NAME, sql.Named("first_name", nil)) // first_name is a rebindable param, with default value nil
            col.Set(a.LAST_NAME, sql.Named("last_name", nil))   // last_name is a rebindable param, with default value nil
            return nil
        }).
        SetDialect(sq.DialectPostgres),
    )
    if err != nil {
        panic(err)
    }
    return q
}()

func main() {
    db := openDB()

    _, err := insertActor.Exec(db, sq.Params{
        "actor_id":   1,
        "first_name": "PENELOPE",
        "last_name":  "GUINESS",
    })
    // INSERT INTO actor (actor_id, first_name, last_name) VALUES (1, 'PENELOPE', 'GUINESS')

    _, err = insertActor.Exec(db, sq.Params{
        "actor_id":   2,
        "first_name": "NICK",
        "last_name":  "WAHLBERG",
    })
    // INSERT INTO actor (actor_id, first_name, last_name) VALUES (2, 'NICK', 'WAHLBERG')

    _, err = insertActor.Exec(db, sq.Params{
        "actor_id":   3,
        "first_name": "ED",
        "last_name":  "CHASE",
    })
    // INSERT INTO actor (actor_id, first_name, last_name) VALUES (3, 'ED', 'CHASE')
}
```

### Preparing queries #preparing-queries

Compiled queries can be further prepared by binding it to a database connection (creating a prepared statement).

```go
// Compile a query.
compiledQuery, err := sq.CompileFetch(sq.
    From(a).
    Where(a.ACTOR_ID.Eq(sq.IntParam("actor_id", 0))).
    SetDialect(sq.DialectPostgres),
    func(row *sq.Row) Actor {
        return Actor{
            ActorID:   row.IntField(a.ACTOR_ID),
            FirstName: row.StringField(a.FIRST_NAME),
            LastName:  row.StringField(a.LAST_NAME),
        }
    },
)
if err != nil {
}

// Prepare a compiled query.
preparedQuery, err := compiledQuery.Prepare(db)
if err != nil {
}

// Use the prepared query.
err = preparedQuery.FetchOne(sq.Params{
    "actor_id": 1,
})
```

## Application-side Row Level Security #appliction-side-row-level-security

You can define policies on your table structs such that whenever it is used in a query, it will produce an additional predicate to be added to the query. This roughly emulates Postgres' Row Level Security, except it works completely application-side and supports every database (not just Postgres).

Since table policies are baked directly into the query string, it plays well with database/sql's connection pooling because you don't have to set session-level variables (which force you to use an \*sql.Tx or \*sql.Conn). That means it also plays well with an external connection pooler like PgBouncer, because again no session-level variables are required.

The main downside is that this can be easily bypassed if you reference the table directly with raw SQL instead of using the query builder.

### A PolicyTable example #policytable

To define a table policy, a table struct must implement the `PolicyTable` interface.

```go
type PolicyTable interface {
    Table
    Policy(ctx context.Context, dialect string) (Predicate, error)
}
```

The context is the same context that was passed in to **sq.FetchAllContext**, **sq.FetchOneContext** or **sq.ExecContext**.

As an example, we will define a table `employees` that stores employees for multiple tenants (indicated by the `tenant_id`). Any SELECT, UPDATE or DELETE query that hits the `employees` table must have a `tenant_id` predicate added to it.

**Before**
```sql
SELECT name FROM employees;
UPDATE employees SET name = $1 WHERE employee_id = $2;
DELETE FROM employees WHERE employee_id = $1;
```

**After**
```sql
SELECT name FROM employees WHERE tenant_id = $1;
UPDATE employees SET name = $1 WHERE tenant_id = $2 AND employee_id = $3;
DELETE FROM employees WHERE tenant_id = $1 AND employee_id = $2;
```

Here is how to define the policy on the employees table.

```go
type EMPLOYEES struct {
    sq.TableStruct
    TENANT_ID   sq.NumberField
    EMPLOYEE_ID sq.NumberField
    NAME        sq.StringField
}

func (tbl EMPLOYEES) Policy(ctx context.Context, dialect string) (sq.Predicate, error) {
    tenantID, ok := ctx.Value("tenantID").(int)
    if !ok {
        return nil, errors.New("tenantID not provided")
    }
    return tbl.TENANT_ID.EqInt(tenantID), nil
}
```
Note that if the `tenantID` cannot be retrieved from the context, `(EMPLOYEES).Policy()` returns an error. This means that any invocation of the `EMPLOYEES` table struct will always require the `tenantID` to be in the context or else query building will fail. You may choose to omit this check by simply returning a `nil` Predicate. `nil` Predicates do not get added to the query.

Here is how to use employees table.
```go
// get tenantID from somewhere and put it into the context
ctx := context.Background().WithValue("tenantID", 1)
e := sq.New[EMPLOYEES]("")

// Query 1
names, err := sq.FetchAllContext(ctx, db, sq.From(e),
    func(row *sq.Row) string {
        return row.String(e.NAME)
    },
)
// SELECT employees.name FROM employees WHERE employees.tenant_id = 1

// Query 2
_, err := sq.ExecContext(ctx, db, sq.
    Update(e).
    Set(e.NAME.SetString("BOB")).
    Where(e.EMPLOYEE_ID.EqInt(18)),
)
// UPDATE employees SET name = 'BOB' WHERE employees.tenant_id = 1 AND employees.employee_id = 18

// Query 3
_, err := sq.ExecContext(ctx, db, sq.
    DeleteFrom(e).
    Where(e.EMPLOYEE_ID.EqInt(18)),
)
// DELETE FROM employees WHERE employees.tenant_id = 1 AND employees.employee_id = 18
```

## SQL examples #sql-examples

### IN #in

#### In slice #in-slice

```sql
a.actor_id IN (1, 2, 3)
```

```go
a := sq.New[ACTOR]("a")
a.ACTOR_ID.In([]int{1, 2, 3})
```

#### In RowValues #in-rowvalues

```sql
a.first_name IN ('PENELOPE', 'NICK', 'ED')
(a.first_name, a.last_name) IN (('PENELOPE', 'GUINESS'), ('NICK', 'WAHLBERG'), ('ED', 'CHASE'))
```

```go
a := sq.New[ACTOR]("a")
a.FIRST_NAME.In(sq.RowValue{"PENELOPE", "NICK", "ED"})
sq.RowValue{a.FIRST_NAME, a.LAST_NAME}.In(sq.RowValues{
    {"PENELOPE", "GUINESS"},
    {"NICK", "WAHLBERG"},
    {"ED", "CHASE"},
})
```

#### In Subquery #in-subquery

```sql
(actor.first_name, actor.last_name) IN (
    SELECT a.first_name, a.last_name
    FROM actor AS a
    WHERE a.actor_id <= 3
)
```

```go
actor, a := sq.New[ACTOR](""), sq.New[ACTOR]("a")
sq.RowValue{actor.FIRST_NAME, actor.LAST_NAME}.In(sq.
    Select(a.FIRST_NAME, a.LAST_NAME).
    From(a).
    Where(a.ACTOR_ID.Le(3)),
)
```

### CASE #case

#### Predicate Case #predicate-case

```sql
CASE
    WHEN f.length <= 60 THEN 'short'
    WHEN f.length > 60 AND f.length <= 120 THEN 'medium'
    ELSE 'long'
END AS length_type
```

```go
f := sq.New[FILM]("f")
sq.CaseWhen(f.LENGTH.LeInt(60), "short").
    CaseWhen(sq.And(f.LENGTH.GtInt(60), f.LENGTH.LeInt(120)), "medium").
    Else("long").
    As("length_type")
```

#### Simple Case #simple-case

```sql
CASE f.rating
    WHEN 'G' THEN 'family'
    WHEN 'PG' THEN 'teens'
    WHEN 'PG-13' THEN 'teens'
    WHEN 'R' THEN 'adults'
    WHEN 'NC-17' THEN 'adults'
    ELSE 'unknown'
END AS audience
```

```go
f := sq.New[FILM]("f")
sq.Case(f.RATING).
    When("G", "family").
    When("PG", "teens").
    When("PG-13", "teens").
    When("R", "adults").
    When("NC-17", "adults").
    Else("unknown").
    As("Audience")
```

### EXISTS #exists

#### Where Exists #where-exists

```sql
SELECT c.customer_id, c.first_name, c.last_name
FROM customers AS c
WHERE EXISTS (
    SELECT 1
    FROM orders AS o
    WHERE o.customer_id = c.customer_id
    GROUP BY o.customer_id
    HAVING COUNT(*) > 2
)
ORDER BY c.first_name, c.last_name
```

```go
c, o := sq.New[CUSTOMERS]("c"), sq.New[ORDERS]("o")
customers, err := sq.FetchAll(db, sq.
    From(c).
    Where(sq.Exists(sq.
        SelectOne().
        From(o).
        Where(o.CUSTOMER_ID.Eq(c.CUSTOMER_ID)).
        GroupBy(o.CUSTOMER_ID).
        Having(sq.Expr("COUNT(*) > 2")),
    )).
    OrderBy(c.FIRST_NAME, c.LAST_NAME),
    func(row *sq.Row) Customer {
        return Customer{
            CustomerID: row.Int(c.CUSTOMER_ID),
            FirstName:  row.String(c.FIRST_NAME),
            LastName:   row.String(c.LAST_NAME),
        }
    },
)
```

#### Where Not Exists #where-not-exists

```sql
SELECT p.product_id, p.product_name
FROM products AS p
WHERE NOT EXISTS (
    SELECT 1
    FROM order_details AS od
    WHERE p.product_id = od.product_id
)
```

```go
p, od := sq.New[PRODUCTS]("p"), sq.New[ORDER_DETAILS]("od")
products, err := sq.FetchAll(db, sq.
    From(p).
    Where(sq.NotExists(sq.
        SelectOne().
        From(od).
        Where(p.PRODUCT_ID.Eq(od.PRODUCT_ID)),
    )),
    func(row *sq.Row) Product {
        return Product{
            ProductID:   row.Int(p.PRODUCT_ID),
            ProductName: row.String(p.PRODUCT_NAME),
        }
    },
)
```

### Subqueries #subqueries

A Subquery is a SelectQuery nested inside another SelectQuery.

**Using SelectQuery as Field**

```sql
SELECT
    city.city,
    (SELECT country.country
        FROM country
        WHERE country.country_id = city.country_id) AS country
FROM
    city
WHERE
    city.city = 'Vancouver'
```

```go
city, country := sq.New[CITY](""), sq.New[COUNTRY]("")
results, err := sq.FetchAll(db, sq.
    From(city).
    Where(city.CITY.EqString("Vancouver")).
    SetDialect(sq.DialectPostgres),
    func(row *sq.Row) Result {
        return Result{
            City:    row.StringField(city.CITY),
            Country: row.StringField(sq.
                Select(country.COUNTRY).
                From(country).
                Where(country.COUNTRY_ID.Eq(city.COUNTRY_ID)).
                As("country"),
            ),
        }
    },
)
```

**Using SelectQuery as Table**

```sql
SELECT
    film.title,
    film_stats.actor_count
FROM
    film
    JOIN (
        SELECT film_actor.film_id, COUNT(*) AS actor_count
        FROM film_actor
        GROUP BY film_actor.film_id
    ) AS film_stats ON film_stats.film_id = film.film_id
```

```go
film, film_actor := sq.New[FILM](""), sq.New[FILM_ACTOR]("")
// create the subquery
film_stats := sq.Postgres.
    Select(
        film_actor.FILM_ID,
        sq.CountStar().As("actor_count"),
    ).
    From(film_actor).
    GroupBy(film_actor.FILM_ID).
    As("film_stats")
// use the subquery
results, err := sq.FetchAll(db, sq.
    From(film).
    Join(film_stats, film_stats.Field("field_id").Eq(film.FILM_ID)),
    func(row *sq.Row) Result {
        return Result{
            Title:      row.String(film.TITLE),
            ActorCount: row.Int(film_stats.Field("actor_count")),
        }
    },
)
```

### WITH (Common Table Expressions) #common-table-expressions

Common Table Expressions (CTEs) are an alternative to [subqueries](#subqueries).

```sql
WITH film_stats AS (
    SELECT film_id, COUNT(*) AS actor_count
    FROM film_actor
    GROUP BY film_id
)
SELECT
    film.title,
    film_stats.actor_count
FROM
    film
    JOIN film_stats ON film_stats.film_id = film.film_id
```

```go
film, film_actor := sq.New[FILM](""), sq.New[FILM_ACTOR]("")
// create the CTE
film_stats := sq.NewCTE("film_stats", nil, sq.Postgres.
    Select(
        film_actor.FILM_ID,
        sq.CountStar().As("actor_count"),
    ).
    From(film_actor).
    GroupBy(film_actor.FILM_ID),
)
// use the CTE
results, err := sq.FetchAll(db, sq.Postgres.
    With(film_stats).
    From(film).
    Join(film_stats, film_stats.Field("field_id").Eq(film.FILM_ID)),
    func(row *sq.Row) Result {
        return Result{
            Title:      row.String(film.TITLE),
            ActorCount: row.Int(film_stats.Field("actor_count")),
        }
    },
)
```

**Recursive Common Table Expressions**

```sql
WITH RECURSIVE counter (n) AS (
    SELECT 1
    UNION ALL
    SELECT counter.n + 1 FROM counter WHERE counter.n + 1 <= 100
)
SELECT counter.n FROM counter;
```

```go
counter := sq.NewRecursiveCTE("counter", []string{"n"}, sq.UnionAll(
    sq.Queryf("SELECT 1"),
    sq.Queryf("SELECT counter.n + 1 FROM counter WHERE counter.n + 1 <= {}", 100)
))
sq.Postgres.With(counter).Select(counter.Field("n")).From(counter)
```

### Aggregate functions #aggregate-functions

sq provides some built-in aggregate functions. They return an `sq.Expression` and so can [pretty much be used everywhere](#expr).

```go
func Count(field Field) Expression
func CountStar() Expression
func Sum(num Number) Expression
func Avg(num Number) Expression
func Min(field Field) Expression
func Max(field Field) Expression
```

### Window functions #window-functions

sq provides some built-in window functions. They return an `sq.Expression` and so can [pretty much be used everywhere](#expr).

```go
func CountOver(field Field, window Window) Expression
func CountStarOver(window Window) Expression
func SumOver(num Number, window Window) Expression
func AvgOver(num Number, window Window) Expression
func MinOver(field Field, window Window) Expression
func MaxOver(field Field, window Window) Expression
func RowNumberOver(window Window) Expression
func RankOver(window Window) Expression
func DenseRankOver(window Window) Expression
func CumeDistOver(window Window) Expression
func FirstValueOver(window Window) Expression
func LastValueOver(window Window) Expression
```

**Missing window functions**

The `LeadOver`, `LagOver` and `NtileOver` window functions do not have a representative Go function because they can be overloaded (they have multiple signatures) while Go functions cannot. If you need them, use an `sq.Expr()` as a stand-in.

```sql
LEAD(a.actor_id) OVER (PARTITION BY a.first_name)
LEAD(a.actor_id, 2) OVER (PARTITION BY a.first_name)
LEAD(a.actor_id, 2, 5) OVER (PARTITION BY a.first_name)
```

```go
a := sq.New[ACTOR]("a")
sq.Expr("LEAD({}) OVER (PARTITION BY {})", a.ACTOR_ID, a.FIRST_NAME)
sq.Expr("LEAD({}, {}) OVER (PARTITION BY {})", a.ACTOR_ID, 2, a.FIRST_NAME)
sq.Expr("LEAD({}, {}, {}) OVER (PARTITION BY {})", a.ACTOR_ID, 2, 5, a.FIRST_NAME)
```

**Using window functions**

To use a window function, you must create a window using `sq.PartitionBy()`, `sq.OrderBy()` or `sq.BaseWindow()`. You can also pass in `nil` to represent the empty window.
```sql
-- Example 1
SELECT COUNT(*) OVER ()
-- Example 2
SELECT SUM(a.actor_id) OVER (PARTITION BY a.first_name)
-- Example 3
SELECT AVG(a.actor_id) OVER (
    PARTITION BY a.first_name, a.last_name
    ORDER BY a.LAST_UPDATE DESC
    RANGE BETWEEN 5 PRECEDING AND 10 FOLLOWING
)
```

```go
a := sq.New[ACTOR]("a")
// Example 1
sq.Postgres.Select(sq.CountStarOver(nil))
// Example 2
sq.Postgres.Select(sq.SumOver(a.ACTOR_ID, sq.PartitionBy(a.FIRST_NAME)))
// Example 3
sq.Postgres.Select(sq.AvgOver(a.ACTOR_ID, sq.
    PartitionBy(a.FIRST_NAME, a.LAST_NAME).
    OrderBy(a.LAST_UPDATE.Desc()).
    Frame("RANGE BETWEEN 5 PRECEDING AND 10 FOLLOWING"),
))
```

SQLite, Postgres and MySQL support the named windows as part of the SELECT query. This allows you to reuse a window definition without having to specify it over and over.

```sql
SELECT
    SUM(a.actor_id) OVER w1,
    MIN(a.actor_id) OVER w2,
    AVG(a.actor_id) OVER (w1 ORDER BY a.last_update)
FROM
    actor AS a
WINDOW
    w1 AS (PARTITION BY a.first_name),
    w2 AS (PARTITION BY a.last_name)
```

```go
a := sq.New[ACTOR]("a")
w1 := sq.NamedWindow{Name: "w1", Definition: sq.PartitionBy(a.FIRST_NAME)}
w2 := sq.NamedWindow{Name: "w2", Definition: sq.PartitionBy(a.LAST_NAME)}
sq.Postgres.
    Select(
        sq.SumOver(a.ACTOR_ID, w1),
        sq.MinOver(a.ACTOR_ID, w2),
        sq.AvgOver(a.ACTOR_ID, sq.BaseWindow(w1).OrderBy(a.LAST_UPDATE)),
    ).
    From(a).
    Window(w1, w2)
```

### UNION, INTERSECT, EXCEPT #union-intersect-except

**Union**

```sql
SELECT t1.field FROM t1
UNION
SELECT t2.field FROM t2
UNION
SELECT t3.field FROM t3
```

```go
sq.Union(
    sq.Select(t1.FIELD).From(t1),
    sq.Select(t2.FIELD).From(t2),
    sq.Select(t3.FIELD).From(t3),
)
```

**Intersect**

```sql
SELECT t1.field FROM t1
INTERSECT
SELECT t2.field FROM t2
INTERSECT
SELECT t3.field FROM t3
```

```go
sq.Intersect(
    sq.Select(t1.FIELD).From(t1),
    sq.Select(t2.FIELD).From(t2),
    sq.Select(t3.FIELD).From(t3),
)
```

**Intersect**

```sql
SELECT t1.field FROM t1
EXCEPT
SELECT t2.field FROM t2
EXCEPT
SELECT t3.field FROM t3
```

```go
sq.Except(
    sq.Select(t1.FIELD).From(t1),
    sq.Select(t2.FIELD).From(t2),
    sq.Select(t3.FIELD).From(t3),
)
```

### ORDER BY #orderby

```sql
SELECT a.first_name FROM actor AS a ORDER BY a.actor_id DESC
SELECT a.last_name FROM actor AS a ORDER BY a.actor_id ASC NULLS FIRST
```

```go
a := sq.New[ACTOR]("a")
sq.Select(a.FIRST_NAME).From(a).OrderBy(a.ACTOR_ID.Desc())
sq.Select(a.LAST_NAME).From(a).OrderBy(a.ACTOR_ID.Asc().NullsFirst())
```
