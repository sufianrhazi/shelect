import csv
import io
import json
import sqlite3
import sys
from pathlib import Path

from .ast_utils import extract_tables

class Engine:
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
        tables = extract_tables(ast)

        # Load files into temp tables
        for table_name in tables:
            if table_name not in self.loaded_files:
                try:
                    self.load_file_table(table_name)
                except Exception:
                    raise Exception(f'Error loading table data from {table_name}')
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

            def format_val(val):
                if val is None:
                    return 'NULL'
                if val is False:
                    return 'FALSE'
                if val is True:
                    return 'TRUE'
                return str(val)

            def format_row(row):
                return " | ".join(format_val(val).ljust(col_widths[i]) for i, val in enumerate(row))

            divider = "-+-".join("-" * w for w in col_widths)
            print(format_row(headers))
            print(divider)
            for row in rows:
                print(format_row(row))
