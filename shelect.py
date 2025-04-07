#!/usr/bin/env python3
import argparse
import csv
import io
import json
import sqlite3
import sys
from pathlib import Path
from sqlglot import parse_one, exp
from textwrap import wrap

DEBUG = False

def load_table(conn, table_name, content):
    """
    Given string content of a file, parse as JSON/CSV and load into table_name
    """
    stripped = content.lstrip()

    if stripped.startswith("{") or stripped.startswith("["):
        return load_json_from_string(conn, table_name, content)
    else:
        return load_csv_from_string(conn, table_name, content)

def load_file_into_sqlite(conn, table_name):
    path = Path(table_name)

    if path == Path('-') or path == Path('stdin'):
        content = sys.stdin.read()
        return load_table(conn, table_name, content)

    with open(path, 'r') as f:
        content = f.read()
        return load_table(conn, table_name, content)

def load_csv_from_string(conn, table_name, content):
    # csv.Sniffer is bizarrely bad at determining information about the file 
    #
    # We use a simple heuristic:
    # - lineterminator: First line looks like a \r\n? Then \r\n; otherwise \n
    # - deliminator: Limited to either , or \t
    #
    # The other dialect fields are guessed by the sniffer given the first line
    # only, to avoid surprises with the *intense* regex
    first_line = content.partition('\n')[0]
    lineterminator = '\r\n' if first_line.endswith('\r') else '\n'
    dialect = csv.Sniffer().sniff(first_line.strip(), [',', '\t'])
    dialect.lineterminator = lineterminator

    with io.StringIO(content) as f:
        reader = csv.DictReader(f, dialect=dialect)

        columns = reader.fieldnames
        create_temp_table(conn, table_name, columns)
        rows = [tuple(row[col] for col in columns) for row in reader]

    insert_rows(conn, table_name, columns, rows)

def load_json_from_string(conn, table_name, content):
    data = json.loads(content)

    if not isinstance(data, list) or not all(isinstance(obj, dict) for obj in data):
        raise ValueError(f"Expected stdin JSON to be a top-level array of objects.")

    columns = list(data[0].keys())
    rows = [tuple(row.get(col) for col in columns) for row in data]

    create_temp_table(conn, table_name, columns)
    insert_rows(conn, table_name, columns, rows)

def create_temp_table(conn, table_name, columns):
    col_defs = ", ".join(f'"{col}" TEXT' for col in columns)
    conn.execute(f'CREATE TEMP TABLE "{table_name}" ({col_defs})')

def insert_rows(conn, table_name, columns, rows):
    placeholders = ", ".join(["?"] * len(columns))
    col_names = ", ".join(f'"{col}"' for col in columns)
    conn.executemany(
        f'INSERT INTO "{table_name}" ({col_names}) VALUES ({placeholders})',
        rows
    )

def parse_args():
    parser = argparse.ArgumentParser(description="Run SELECT queries on local CSV/JSON files using SQLite.")
    parser.add_argument("query", type=str, help="SQL SELECT statement referencing local files")
    parser.add_argument("--format", "-o", choices=["csv", "json", "table"], default="table",
                        help="Output format: csv, json, or table (default)")
    return parser.parse_args()

def extract_tables(ast):
    """
    Traverse the AST and extract all table references.
    """
    with_bindings = set()
    tables = set()

    for with_node in ast.find_all(exp.With):
        for with_exp in with_node.expressions:
            with_bindings.add(with_exp.alias)

    for table in ast.find_all(exp.Table):
        tables.add(table.name)

    return tables - with_bindings

def output_results(cursor, output_format):
    """
    Given a cursor into an executed query, print the results
    """
    if output_format == "csv":
        writer = csv.writer(sys.stdout)
        headers_written = False
        for row in cursor:
            if not headers_written:
                headers = [desc[0] for desc in cursor.description]
                writer.writerow(headers)
                headers_written = True
            writer.writerow(row)

    elif output_format == "json":
        headers = [desc[0] for desc in cursor.description]
        rows = [dict(zip(headers, row)) for row in cursor]
        print(json.dumps(rows, indent=2))

    elif output_format == "table":
        headers = [desc[0] for desc in cursor.description]
        rows = list(cursor)

        col_widths = [len(h) for h in headers]
        for row in rows:
            for i, val in enumerate(row):
                col_widths[i] = max(col_widths[i], len(str(val)))

        def format_row(row):
            return " | ".join(str(val).ljust(col_widths[i]) for i, val in enumerate(row))

        divider = "-+-".join("-" * w for w in col_widths)
        print(format_row(headers))
        print(divider)
        for row in rows:
            print(format_row(row))

def main():
    args = parse_args()
    try:
        ast = parse_one(args.query, dialect="sqlite")
    except Exception as e:
        print(f"SQL syntax error: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        tables = extract_tables(ast)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    # Load files into temp tables
    conn = sqlite3.connect(":memory:")
    for table_name in tables:
        if DEBUG:
            print(f"\nLoading table {table_name}...")
        try:
            load_file_into_sqlite(conn, table_name)
        except Exception as e:
            print(f'Error loading table data from {table_name}: {e}', file=sys.stderr)
            sys.exit(1)

    # Execute rewritten SQL
    sql_to_execute = ast.sql(dialect="sqlite")
    cursor = conn.execute(sql_to_execute)
    output_results(cursor, args.format)

if __name__ == "__main__":
    main()
