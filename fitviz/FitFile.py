from collections import namedtuple

import fitdecode


class FitFile:
    def __init__(self, path: str):
        self.path = path
