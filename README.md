[![GoDoc](https://img.shields.io/badge/pkg.go.dev-sq-blue)](https://pkg.go.dev/github.com/bokwoon95/sq)
![tests](https://github.com/bokwoon95/sq/actions/workflows/tests.yml/badge.svg?branch=main)
[![Go Report Card](https://goreportcard.com/badge/github.com/bokwoon95/sq)](https://goreportcard.com/report/github.com/bokwoon95/sq)
[![Coverage Status](https://coveralls.io/repos/github/bokwoon95/sq/badge.svg?branch=main)](https://coveralls.io/github/bokwoon95/sq?branch=main)

<img src="https://raw.githubusercontent.com/bokwoon95/sq/main/header.png" title="code example of a select query using sq" alt="code example of a select query using sq, to give viewers a quick idea of what the library is about" style="max-width:90%;">

# sq (Structured Query)

[one-page documentation](https://bokwoon.neocities.org/sq.html)

sq is a type-safe data mapper and query builder for Go. Its concept is simple: you provide a callback function that maps a row to a struct, generics ensure that you get back a slice of structs at the end. Additionally, mentioning a column in the callback function automatically adds it to the SELECT clause so you don't even have to explicitly mention what columns you want to select: the [act of mapping a column is the same as selecting it](#select-example-raw-sql). This eliminates a source of errors where you have specify the columns twice (once in the query itself, once to the call to rows.Scan) and end up missing a column, getting the column order wrong or mistyping a column name.

Notable features:

- Works across SQLite, Postgres, MySQL and SQL Server. [[more info](https://bokwoon.neocities.org/sq.html#set-query-dialect)]
- Each dialect has its own query builder, allowing you to use dialect-specific features. [[more info](https://bokwoon.neocities.org/sq.html#dialect-specific-features)]
- Declarative schema migrations. [[more info](https://bokwoon.neocities.org/sq.html#declarative-schema)]
- Supports arrays, enums, JSON and UUID. [[more info](https://bokwoon.neocities.org/sq.html#arrays-enums-json-uuid)]
- Query logging. [[more info](https://bokwoon.neocities.org/sq.html#logging)]

# Installation

This package only supports Go 1.18 and above.

```shell
$ go get github.com/bokwoon95/sq
$ go install -tags=fts5 github.com/bokwoon95/sqddl@latest
```

# Features

- IN
    - [In Slice](https://bokwoon.neocities.org/sq.html#in-slice) - `a IN (1, 2, 3)`
    - [In RowValues](https://bokwoon.neocities.org/sq.html#in-rowvalues) - `(a, b, c) IN ((1, 2, 3), (4, 5, 6), (7, 8, 9))`
    - [In Subquery](https://bokwoon.neocities.org/sq.html#in-subquery) - `(a, b) IN (SELECT a, b FROM tbl WHERE condition)`
- CASE
    - [Predicate Case](https://bokwoon.neocities.org/sq.html#predicate-case) - `CASE WHEN a THEN b WHEN c THEN d ELSE e END`
    - [Simple case](https://bokwoon.neocities.org/sq.html#simple-case) - `CASE expr WHEN a THEN b WHEN c THEN d ELSE e END`
- EXISTS
    - [Where Exists](https://bokwoon.neocities.org/sq.html#where-exists)
    - [Where Not Exists](https://bokwoon.neocities.org/sq.html#where-not-exists)
    - [Select Exists](https://bokwoon.neocities.org/sq.html#querybuilder-fetch-exists)
- [Subqueries](https://bokwoon.neocities.org/sq.html#subqueries)
- [WITH (Common Table Expressions)](https://bokwoon.neocities.org/sq.html#common-table-expressions)
- [Aggregate functions](https://bokwoon.neocities.org/sq.html#aggregate-functions)
- [Window functions](https://bokwoon.neocities.org/sq.html#window-functions)
- [UNION, INTERSECT, EXCEPT](https://bokwoon.neocities.org/sq.html#union-intersect-except)
- [INSERT from SELECT](https://bokwoon.neocities.org/sq.html#querybuilder-insert-from-select)
- RETURNING
    - [SQLite RETURNING](https://bokwoon.neocities.org/sq.html#sqlite-returning)
    - [Postgres RETURNING](https://bokwoon.neocities.org/sq.html#postgres-returning)
- LastInsertId
    - [SQLite LastInsertId](https://bokwoon.neocities.org/sq.html#sqlite-last-insert-id)
    - [MySQL LastInsertId](https://bokwoon.neocities.org/sq.html#mysql-last-insert-id)
- Insert ignore duplicates
    - [SQLite Insert ignore duplicates](https://bokwoon.neocities.org/sq.html#sqlite-insert-ignore-duplicates)
    - [Postgres Insert ignore duplicates](https://bokwoon.neocities.org/sq.html#postgres-insert-ignore-duplicates)
    - [MySQL Insert ignore duplicates](https://bokwoon.neocities.org/sq.html#mysql-insert-ignore-duplicates)
    - [SQL Server Insert ignore duplicates](https://bokwoon.neocities.org/sq.html#sqlserver-insert-ignore-duplicates)
- Upsert
    - [SQLite Upsert](https://bokwoon.neocities.org/sq.html#sqlite-upsert)
    - [Postgres Upsert](https://bokwoon.neocities.org/sq.html#postgres-upsert)
    - [MySQL Upsert](https://bokwoon.neocities.org/sq.html#mysql-upsert)
    - [SQL Server Upsert](https://bokwoon.neocities.org/sq.html#sqlserver-upsert)
- Update with Join
    - [SQLite Update with Join](https://bokwoon.neocities.org/sq.html#sqlite-update-with-join)
    - [Postgres Update with Join](https://bokwoon.neocities.org/sq.html#postgres-update-with-join)
    - [MySQL Update with Join](https://bokwoon.neocities.org/sq.html#mysql-update-with-join)
    - [SQL Server Update with Join](https://bokwoon.neocities.org/sq.html#sqlserver-update-with-join)
- Delete with Join
    - [SQLite Delete with Join](https://bokwoon.neocities.org/sq.html#sqlite-delete-with-join)
    - [Postgres Delete with Join](https://bokwoon.neocities.org/sq.html#postgres-delete-with-join)
    - [MySQL Delete with Join](https://bokwoon.neocities.org/sq.html#mysql-delete-with-join)
    - [SQL Server Delete with Join](https://bokwoon.neocities.org/sq.html#sqlserver-delete-with-join)
- Bulk Update
    - [SQLite Bulk Update](https://bokwoon.neocities.org/sq.html#sqlite-bulk-update)
    - [Postgres Bulk Update](https://bokwoon.neocities.org/sq.html#postgres-bulk-update)
    - [MySQL Bulk Update](https://bokwoon.neocities.org/sq.html#mysql-bulk-update)
    - [SQL Server Bulk Update](https://bokwoon.neocities.org/sq.html#sqlserver-bulk-update)

## SELECT example (Raw SQL)

```go
db, err := sql.Open("postgres", "postgres://username:password@localhost:5432/sakila?sslmode=disable")

actors, err := sq.FetchAll(db, sq.
    Queryf("SELECT {*} FROM actor AS a WHERE a.actor_id IN ({})",
        []int{1, 2, 3, 4, 5},
    ).
    SetDialect(sq.DialectPostgres),
    func(row *sq.Row) Actor {
        return Actor{
            ActorID:     row.Int("a.actor_id"),
            FirstName:   row.String("a.first_name"),
            LastName:    row.String("a.last_name"),
            LastUpdate:  row.Time("a.last_update"),
        }
    },
)
```

## SELECT example (Query Builder)

To use the query builder, you must first [define your table structs](https://bokwoon.neocities.org/sq.html#table-structs).

```go
db, err := sql.Open("postgres", "postgres://username:password@localhost:5432/sakila?sslmode=disable")

a := sq.New[ACTOR]("a")
actors, err := sq.FetchAll(db, sq.
    From(a).
    Where(a.ACTOR_ID.In([]int{1, 2, 3, 4, 5})).
    SetDialect(sq.DialectPostgres),
    func(row *sq.Row) Actor {
        return Actor{
            ActorID:     row.IntField(a.ACTOR_ID),
            FirstName:   row.StringField(a.FIRST_NAME),
            LastName:    row.StringField(a.LAST_NAME),
            LastUpdate:  row.TimeField(a.LAST_UPDATE),
        }
    },
)
```

## INSERT example (Raw SQL)

```go
db, err := sql.Open("postgres", "postgres://username:password@localhost:5432/sakila?sslmode=disable")

_, err := sq.Exec(db, sq.
    Queryf("INSERT INTO actor (actor_id, first_name, last_name) VALUES {}", sq.RowValues{
        {18, "DAN", "TORN"},
        {56, "DAN", "HARRIS"},
        {166, "DAN", "STREEP"},
    }).
    SetDialect(sq.DialectPostgres),
)
```

## INSERT example (Query Builder)

To use the query builder, you must first [define your table structs](https://bokwoon.neocities.org/sq.html#table-structs).

```go
db, err := sql.Open("postgres", "postgres://username:password@localhost:5432/sakila?sslmode=disable")

a := sq.New[ACTOR]("a")
_, err := sq.Exec(db, sq.
    InsertInto(a).
    Columns(a.ACTOR_ID, a.FIRST_NAME, a.LAST_NAME).
    Values(18, "DAN", "TORN").
    Values(56, "DAN", "HARRIS").
    Values(166, "DAN", "STREEP").
    SetDialect(sq.DialectPostgres),
)
```

For a more detailed overview, look at the [Quickstart](https://bokwoon.neocities.org/sq.html#quickstart).

## Project Status

sq is done for my use case (hence it may seem inactive, but it's just complete). At this point I'm just waiting for people to ask questions or file feature requests under [discussions](https://github.com/bokwoon95/sq/discussions).

## Contributing

See [START\_HERE.md](https://github.com/bokwoon95/sq/blob/main/START_HERE.md).
