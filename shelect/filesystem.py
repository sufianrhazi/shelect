import sys

class Filesystem:
    def get_stdin(self, path):
        return sys.stdin

    def open_file(self, path):
        return open(path, 'r')
