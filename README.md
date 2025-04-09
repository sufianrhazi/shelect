# Shelect: SQL for local files

Shelect lets you run SQL on data in your local filesystem.

Use it for easy, local ad-hoc analysis of CSV/TSV/JSON files.

It supports all functions and SQL syntax supported by [SQLite](https://sqlite.org/).


## How to use it

```
$ shelect 'SELECT * FROM "./examples/people.csv"'
id | name
---+-------
1  | Alice
2  | Bob
3  | Carlos
4  | Dani
$ shelect 'SELECT * FROM "./examples/values.json"'
id | value
---+------
1  | 10
2  | 20
3  | 30
3  | 40
$ shelect 'SELECT * FROM "./examples/values.json" JOIN "./examples/people.csv" USING (id)'
id | value | name
---+-------+-------
1  | 10    | Alice
2  | 20    | Bob
3  | 30    | Carlos
3  | 40    | Carlos
$ shelect 'SELECT name, sum(value) FROM "./examples/people.csv" LEFT JOIN "./examples/values.json" USING (id) GROUP BY 1'
name   | SUM(value)
-------+-----------
Alice  | 10
Bob    | 20
Carlos | 70
Dani   | NULL
```

It also can function as a repl when invoked without a query.


## How it works

An in-memory SQLite database is created. Before executing each query, the query is parsed and all tables accessed are
first imported from the filesystem into a table with that name.

So if you run `SELECT * from "./my-file.csv"` a table named `"./my-file.csv"` will be created by opening `./my-file.csv`
and attempting to read it as JSON, CSV, or TSV.

JSON files must be arrays of objects, where each object is a key-value mapping where keys correspond to column names.

CSV and TSV files must have a single row with column names followed by data rows.

All columns are created as `TEXT` data types. Please refer to the SQLite documentation for supported SQL functions and
functionality. 


## How to install

shelect is on pypi: https://pypi.org/project/shelect/

Via [pipx](https://pipx.pypa.io/stable/):

```
$ pipx install shelect
```

Via pip:

```
$ python3 -m pip install shelect
```
