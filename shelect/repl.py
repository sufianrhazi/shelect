import cmd
import sys
from sqlglot import Tokenizer, parse, TokenType

class Repl(cmd.Cmd):
    intro = "Type SQL statements ending in ';' or Ctrl+D to exit."
    ORIG_PROMPT = '>>> '
    CONT_PROMPT = '... '
    prompt = ">>> "

    def __init__(self, engine):
        super().__init__()
        self.engine = engine
        self.buffer = []

    def cmdloop(self):
        while True:
            try:
                super().cmdloop()
                return
            except KeyboardInterrupt:
                # Mimic python's shell, printing KeyboardInterrupt
                print("\nKeyboardInterrupt")
                # Clear intro to avoid re-printing
                self.intro = ''

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
                try:
                    self.engine.run_statement(statement)
                except Exception as e:
                    print(f"Error running SQL: {e}", file=sys.stderr)

    def do_exit(self, arg):
        """Exit the REPL."""
        return True

    def do_quit(self, arg):
        """Exit the REPL."""
        return True

    def do_EOF(self, arg):
        """Exit on Ctrl-D."""
        return True
