import io

class OutputFake:
    def __init__(self):
        self._buffer = io.StringIO('')

    def print(self, msg):
        self._buffer.write(msg + '\n')

    def get_as_file(self):
        return self._buffer

    def test_get_output(self):
        return self._buffer.getvalue()
