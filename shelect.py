#!/usr/bin/env python3
import argparse
import sys
from sqlglot import parse_one, exp

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
    Traverse the AST and extract all table references with their aliases.
    Only keep tables whose name looks like a file path (contains '/' or ends in .csv/.json).
    """
    file_tables = {}

    for table in ast.find_all(exp.Table):
        table_name = table.name
        alias = table.args.get("alias")
        alias_name = alias.name if alias else table_name

        if "/" in table_name or table_name.endswith((".csv", ".json")):
            file_tables[alias_name] = table_name

    return file_tables

def main():
    args = parse_args()
    try:
        ast = parse_one('SELECT ' + args.query, dialect="sqlite")
    except Exception as e:
        print(f"Error parsing SQL: {e}", file=sys.stderr)
        sys.exit(1)

    file_tables = extract_file_tables(ast)
    if not file_tables:
        print("No file-based tables found in SQL query.", file=sys.stderr)
        sys.exit(1)

    print("Found file references:")
    for alias, path in file_tables.items():
        print(f"  {alias}: {path}")

    # Rewrite the query to use only aliases as table names
    rewrite_table_paths(ast)
    rewritten_sql = ast.sql(dialect="sqlite")
    print("\nRewritten query:")
    print(rewritten_sql)

    # TODO: Load files into in-memory SQLite tables
    # TODO: Replace file paths in AST with temp table names
    # TODO: Execute query on the rewritten SQL

if __name__ == "__main__":
    main()
