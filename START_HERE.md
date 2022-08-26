This document describes how the codebase is organized. It is meant for people who are contributing to the codebase (or are just casually browsing).

Files are written in such a way that **each successive file in the list below only depends on files that come before it**. This self-enforced restriction makes deep architectural changes trivial because you can essentially blow away the entire codebase and rewrite it from scratch file-by-file, complete with working tests every step of the way. Please adhere to this file order when submitting pull requests.

- [**sq.go**](https://github.com/bokwoon95/sq/blob/main/sq.go)
    - Core interfaces: SQLWriter, DB, Query, Table, PolicyTable, Window, Field, Predicate, Assignment, Any, Array, Binary, Boolean, Enum, JSON, Number, String, UUID, Time, Enumeration, DialectValuer,
    - Data types: Result, TableStruct, ViewStruct.
    - Misc utility functions.
- [**fmt.go**](https://github.com/bokwoon95/sq/blob/main/fmt.go)
    - Two important string building functions that everything else is built on: [Writef](https://pkg.go.dev/github.com/bokwoon95/sq#Writef) and [WriteValue](https://pkg.go.dev/github.com/bokwoon95/sq#WriteValue).
    - Data types: Parameter, BinaryParameter, BooleanParameter, NumberParameter, StringParameter, TimeParameter.
    - Utility functions: QuoteIdentifier, EscapeQuote, Sprintf, Sprint.
- [**builtins.go**](https://github.com/bokwoon95/sq/blob/main/builtins.go)
    - Builtin data types that are built on top of Writef and WriteValue: Expression (Expr), CustomQuery (Queryf), VariadicPredicate, assignment, RowValue, RowValues, Fields.
    - Builtin functions that are built on top of Writef and WriteValue: Eq, Ne, Lt, Le, Gt, Ge, Exists, NotExists, In.
- [**fields.go**](https://github.com/bokwoon95/sq/blob/main/fields.go)
    - All of the field types: AnyField, ArrayField, BinaryField, BooleanField, EnumField, JSONField, NumberField, StringField, UUIDField, TimeField.
    - Data types: Identifier, Timestamp.
    - Functions: [New](https://pkg.go.dev/github.com/bokwoon95/sq#New), ArrayValue, EnumValue, JSONValue, UUIDValue.
- [**cte.go**](https://github.com/bokwoon95/sq/blob/main/cte.go)
    - CTE represents an SQL common table expression (CTE).
    - UNION, INTERSECT, EXCEPT.
- [**joins.go**](https://github.com/bokwoon95/sq/blob/main/joins.go)
    - The various SQL joins.
- [**row_column.go**](https://github.com/bokwoon95/sq/blob/main/row_column.go)
    - Row and Column methods.
- [**window.go**](https://github.com/bokwoon95/sq/blob/main/window.go)
    - SQL windows and window functions.
- [**select_query.go**](https://github.com/bokwoon95/sq/blob/main/select_query.go)
    - SQL SELECT query builder.
- [**insert_query.go**](https://github.com/bokwoon95/sq/blob/main/insert_query.go)
    - SQL INSERT query builder.
- [**update_query.go**](https://github.com/bokwoon95/sq/blob/main/update_query.go)
    - SQL UPDATE query builder.
- [**delete_query.go**](https://github.com/bokwoon95/sq/blob/main/delete_query.go)
    - SQL DELETE query builder.
- [**logger.go**](https://github.com/bokwoon95/sq/blob/main/logger.go)
    - sq.Log and sq.VerboseLog.
- [**fetch_exec.go**](https://github.com/bokwoon95/sq/blob/main/fetch_exec.go)
    - FetchCursor, FetchOne, FetchAll, Exec.
    - CompiledFetch, CompiledExec.
    - PreparedFetch, PreparedExec.
- [**misc.go**](https://github.com/bokwoon95/sq/blob/main/misc.go)
    - Misc SQL constructs.
    - ValueExpression, LiteralValue, DialectExpression, CaseExpression, SimpleCaseExpression.
    - SelectValues (`SELECT ... UNION ALL SELECT ... UNION ALL SELECT ...`)
    - TableValues (`VALUES (...), (...), (...)`).
- [**integration_test.go**](https://github.com/bokwoon95/sq/blob/main/integration_test.go)
    - Tests that interact with a live database i.e. SQLite, Postgres, MySQL and SQL Server.

## Testing

Add tests if you add code.

To run tests, use:

```shell
$ go test . # -failfast -shuffle=on -coverprofile=coverage
```

There are tests that require a live database connection. They will only run if you provide the corresponding database URL in the test flags:

```shell
$ go test . -postgres $POSTGRES_URL -mysql $MYSQL_URL -sqlserver $SQLSERVER_URL # -failfast -shuffle=on -coverprofile=coverage
```

You can consider using the [docker-compose.yml defined in the sqddl repo](https://github.com/bokwoon95/sqddl/blob/main/docker-compose.yml) to spin up Postgres, MySQL and SQL Server databases that are reachable at the following URLs:

```shell
# docker-compose up -d
POSTGRES_URL='postgres://user1:Hunter2!@localhost:5456/sakila?sslmode=disable'
MYSQL_URL='root:Hunter2!@tcp(localhost:3330)/sakila?multiStatements=true&parseTime=true'
SQLSERVER_URL='sqlserver://sa:Hunter2!@localhost:1447'
```

## Documentation

Documentation is contained entirely within [sq.md](https://github.com/bokwoon95/sq/blob/main/sq.md) in the project root directory. You can view the output at [https://bokwoon.neocities.org/sq.html](https://bokwoon.neocities.org/sq.html). The documentation is regenerated everytime a new commit is pushed to the main branch, so to change the documentation just change sq.md and submit a pull request.

You can preview the output of sq.md locally by installing [github.com/bokwoon95/mddocs](https://github.com/bokwoon95/mddocs) and running it with sq.md as the argument.

```shell
$ go install github/bokwoon95/mddocs@latest
$ mddocs
Usage:
mddocs project.md              # serves project.md on a localhost connection
mddocs project.md project.html # render project.md into project.html

$ mddocs sq.md
serving sq.md at localhost:6060
```

To add a new section and register it in the table of contents, append a `#headerID` to the end of a header (replace `headerID` with the actual header ID). The header ID should only contain unicode letters, digits, hyphen `-` and underscore `_`.

```text
## This is a header.

## This is a header with a headerID. #header-id <-- added to table of contents
```
