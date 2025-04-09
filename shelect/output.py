import sys

class Output:
    def print(self, msg):
        print(msg)

    def get_as_file(self):
        return sys.stdout
