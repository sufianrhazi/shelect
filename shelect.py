#!/usr/bin/env python3
import argparse
import csv
import sqlite3
import json
import sys
from pathlib import Path
from sqlglot import parse_one, exp

DEBUG = False

def detect_csv_dialect(path):
    """
    Attempt to detect whether the file is CSV or TSV.
    """
    with open(path, newline='', encoding='utf-8') as f:
        sample = f.read(2048)
    sniffer = csv.Sniffer()
    dialect = sniffer.sniff(sample)
    return dialect

def load_file_into_sqlite(conn, alias, file_path):
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    if path.suffix.lower() == ".csv":
        return load_csv(conn, alias, path)

    elif path.suffix.lower() == ".json":
        return load_json(conn, alias, path)

    else:
        raise ValueError(f"Unsupported file type: {file_path}")

def load_csv(conn, alias, path):
    dialect = detect_csv_dialect(path)

    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f, dialect=dialect)
        columns = reader.fieldnames

        # Create table
        create_temp_table(conn, alias, columns)

        # Insert data
        rows = [tuple(row[col] for col in columns) for row in reader]
        insert_rows(conn, alias, columns, rows)

def load_json(conn, alias, path):
    with open(path, encoding='utf-8') as f:
        data = json.load(f)

    if not isinstance(data, list) or not all(isinstance(obj, dict) for obj in data):
        raise ValueError(f"Expected JSON file {path} to be a top-level array of objects.")

    columns = list(data[0].keys())
    rows = [tuple(row.get(col) for col in columns) for row in data]

    create_temp_table(conn, alias, columns)
    insert_rows(conn, alias, columns, rows)

def create_temp_table(conn, alias, columns):
    col_defs = ", ".join(f'"{col}" TEXT' for col in columns)
    conn.execute(f'CREATE TEMP TABLE "{alias}" ({col_defs})')

def insert_rows(conn, alias, columns, rows):
    placeholders = ", ".join(["?"] * len(columns))
    col_names = ", ".join(f'"{col}"' for col in columns)
    conn.executemany(
        f'INSERT INTO "{alias}" ({col_names}) VALUES ({placeholders})',
        rows
    )

def parse_args():
    parser = argparse.ArgumentParser(description="Run SELECT queries on local CSV/JSON files using SQLite.")
    parser.add_argument("query", type=str, help="SQL SELECT statement referencing local files")
    return parser.parse_args()

def rewrite_table_paths(ast):
    """
    Modify the AST in-place to replace any file-based table references with just their alias name.
    Example: FROM "./data.csv" AS a → FROM a
    """
    for table in ast.find_all(exp.Table):
        table_name = table.name
        alias = table.args.get("alias")

        if alias and ("/" in table_name or table_name.endswith((".csv", ".json"))):
            # Replace table name with alias
            table.set("this", exp.to_identifier(alias.name))
            table.set("alias", None)

def extract_file_tables(ast):
    """
    Traverse the AST and extract all table references with aliases.
    Raise if a file path is used as a table name but no alias is provided.
    """
    file_tables = {}

    for table in ast.find_all(exp.Table):
        table_name = table.name
        alias = table.args.get("alias")

        if "/" in table_name or table_name.endswith((".csv", ".json")):
            if not alias:
                raise ValueError(f'Missing alias for file path table "{table_name}"')
            file_tables[alias.name] = table_name

    return file_tables

def main():
    args = parse_args()
    try:
        ast = parse_one('SELECT ' + args.query, dialect="sqlite")
    except Exception as e:
        print(f"SQL syntax error: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        file_tables = extract_file_tables(ast)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    if not file_tables:
        print("No file-based tables found in SQL query.", file=sys.stderr)
        sys.exit(1)

    # Load files into temp tables
    conn = sqlite3.connect(":memory:")
    for alias, file_path in file_tables.items():
        if DEBUG:
            print(f"\nLoading {file_path} into table {alias}...")
        load_file_into_sqlite(conn, alias, file_path)

    # Rewrite the query to use only aliases as table names
    rewrite_table_paths(ast)
    rewritten_sql = ast.sql(dialect="sqlite")
    if DEBUG:
        print("\nRewritten query:")
        print(rewritten_sql)

    # Execute rewritten SQL
    rewritten_sql = ast.sql(dialect="sqlite")
    cursor = conn.execute(rewritten_sql)
    rows = cursor.fetchall()
    headers = [d[0] for d in cursor.description]

    if DEBUG:
        print("\nQuery result:")
    print("\t".join(headers))
    for row in rows:
        print("\t".join(map(str, row)))

if __name__ == "__main__":
    main()
