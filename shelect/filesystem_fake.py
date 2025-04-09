import io
import sys

class FilesystemFake:
    def __init__(self):
        self._files = {}
        self._stdin = ''

    def test_set_file(self, path, contents):
        self._files[path] = contents

    def get_stdin(self, path):
        return io.StringIO(self._stdin)

    def open_file(self, path):
        try:
            return io.StringIO(self._files[path])
        except KeyError:
            raise FileNotFoundError(f'No such file or directory: {rep(path)}')
