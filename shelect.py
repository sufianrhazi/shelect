#!/usr/bin/env python3
import argparse
import csv
import cmd
import io
import json
import readline
import sqlite3
import sys
from pathlib import Path
from sqlglot import Tokenizer, parse, exp, TokenType
from textwrap import wrap

DEBUG = False

def parse_args():
    parser = argparse.ArgumentParser(description="Run SELECT queries on local CSV/JSON files using SQLite.")
    parser.add_argument(
        "query",
        nargs="*",
        type=str,
        help="SQL SELECT statement referencing local files"
    )
    parser.add_argument(
        "--format",
        "-o",
        choices=["csv", "json", "table"],
        default="table" if sys.stdout.isatty() else 'csv',
        help="Output format: table (default if tty), csv (default otherwise), json"
    )
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

class Shelect:
    def __init__(self, output_format):
        self.loaded_files = set()
        self.output_format = output_format
        self.conn = sqlite3.connect(':memory:')

    def create_temp_table(self, table_name, columns):
        col_defs = ", ".join(f'"{col}" TEXT' for col in columns)
        self.conn.execute(f'CREATE TEMP TABLE "{table_name}" ({col_defs})')

    def insert_rows(self, table_name, columns, rows):
        placeholders = ", ".join(["?"] * len(columns))
        col_names = ", ".join(f'"{col}"' for col in columns)
        self.conn.executemany(
            f'INSERT INTO "{table_name}" ({col_names}) VALUES ({placeholders})',
            rows
        )

    def load_json_from_string(self, table_name, content):
        data = json.loads(content)

        if not isinstance(data, list) or not all(isinstance(obj, dict) for obj in data):
            raise ValueError(f"Expected stdin JSON to be a top-level array of objects.")

        columns = list(data[0].keys())
        rows = [tuple(row.get(col) for col in columns) for row in data]

        self.create_temp_table(table_name, columns)
        self.insert_rows(table_name, columns, rows)

    def load_csv_from_string(self, table_name, content):
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
            self.create_temp_table(table_name, columns)
            rows = [tuple(row[col] for col in columns) for row in reader]

        self.insert_rows(table_name, columns, rows)

    def load_file_table_content(self, table_name, content):
        """
        Given string content of a file, parse as JSON/CSV and load into table_name
        """
        stripped = content.lstrip()

        if stripped.startswith("{") or stripped.startswith("["):
            return self.load_json_from_string(table_name, content)
        else:
            return self.load_csv_from_string(table_name, content)

    def load_file_table(self, table_name):
        path = Path(table_name)

        if path == Path('-') or path == Path('stdin'):
            content = sys.stdin.read()
            return self.load_file_table_content(table_name, content)

        with open(path, 'r') as f:
            content = f.read()
            return self.load_file_table_content(table_name, content)

    def run_statement(self, ast):
        try:
            tables = extract_tables(ast)
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            raise e

        # Load files into temp tables
        for table_name in tables:
            if table_name not in self.loaded_files:
                if DEBUG:
                    print(f"\nLoading table {table_name}...")
                try:
                    self.load_file_table(table_name)
                except Exception as e:
                    print(f'Error loading table data from {table_name}: {e}', file=sys.stderr)
                    raise e
                self.loaded_files.add(table_name)

        # Execute rewritten SQL
        sql_to_execute = ast.sql(dialect="sqlite")
        cursor = self.conn.execute(sql_to_execute)

        self.print_results(cursor)

    def print_results(self, cursor):
        """
        Given a cursor into an executed query, print the results
        """
        if self.output_format == "csv":
            writer = csv.writer(sys.stdout)
            headers_written = False
            for row in cursor:
                if not headers_written:
                    headers = [desc[0] for desc in cursor.description]
                    writer.writerow(headers)
                    headers_written = True
                writer.writerow(row)

        elif self.output_format == "json":
            headers = [desc[0] for desc in cursor.description]
            rows = [dict(zip(headers, row)) for row in cursor]
            print(json.dumps(rows, indent=2))

        elif self.output_format == "table":
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


def readline_completer(text, state):
    pass

class Repl(cmd.Cmd):
    intro = "Type SQL statements ending in ';' or Ctrl+D to exit."
    ORIG_PROMPT = '>>> '
    CONT_PROMPT = '... '
    prompt = ">>> "

    def __init__(self, shelect):
        super().__init__()
        self.shelect = shelect
        self.buffer = []

        # Set up simple tab completion
        readline.set_completer(self.complete_hook)
        readline.parse_and_bind("tab: complete")

    def complete_hook(self, text, state):
        # Always complete to TODO
        options = ["TODO"]
        if state < len(options):
            return options[state]
        return None

    def default(self, line):
        self.buffer.append(line)

        # Join buffer and tokenize to see if we reached end of statement
        joined = "\n".join(self.buffer).strip()
        if not joined:
            return

        tokens = Tokenizer().tokenize(joined)
        if not tokens or tokens[-1].token_type != TokenType.SEMICOLON:
            # Not a complete statement yet
            self.prompt = self.CONT_PROMPT
            return

        # Full SQL statement received
        statement = joined
        self.buffer = []
        self.prompt = self.ORIG_PROMPT

        try:
            statements = parse(statement, dialect="sqlite")
        except Exception as e:
            print(f"SQL parse error: {e}", file=sys.stderr)
            return

        for statement in statements:
            if statement:
                self.shelect.run_statement(statement)

    def do_exit(self, arg):
        """Exit the REPL."""
        return True

    def do_quit(self, arg):
        """Exit the REPL."""
        return True

    def do_EOF(self, arg):
        """Exit on Ctrl-D."""
        return True


def main():
    args = parse_args()
    shelect = Shelect(args.format)
    if not args.query:
        if sys.stdin.isatty():
            Repl(shelect).cmdloop()
    else:
        for query in args.query:
            try:
                statements = parse(query, dialect="sqlite")
            except Exception as e:
                print(f"SQL syntax error: {e}", file=sys.stderr)
                raise e
            for statement in statements:
                if statement:
                    try:
                        shelect.run_statement(statement)
                    except Exception as e:
                        sys.exit(1)

if __name__ == "__main__":
    main()
