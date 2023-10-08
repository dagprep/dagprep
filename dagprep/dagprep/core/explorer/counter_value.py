from typing import List
from dagprep.core.base.transformation import Transformation


class CounterValue:
    def __init__(self, keys: List[str]) -> None:
        self.d = {key: 0 for key in keys}

    def count(self, key: str) -> int:
        if key not in self.d:
            self.d[key] = 1
            return 1
        self.d[key] += 1
        return self.d[key]

    def is_ready(self, transformation: Transformation) -> bool:
        return self.get(transformation.name) == len(transformation.depends_on)

    def get(self, key: str) -> int:
        if key not in self.d:
            self.d[key] = 0
        return self.d.get(key)
