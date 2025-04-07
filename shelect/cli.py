import argparse
import signal
import sys
from sqlglot import parse

from .engine import Engine
from .repl import Repl

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

def main():
    args = parse_args()
    engine = Engine(args.format)

    if not args.query:
        if sys.stdin.isatty():
            repl = Repl(engine)
            repl.cmdloop()
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
                        engine.run_statement(statement)
                    except Exception as e:
                        print(f"Error running SQL: {e}", file=sys.stderr)
                        sys.exit(1)

