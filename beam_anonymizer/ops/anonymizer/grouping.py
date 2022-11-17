from typing import List

from .base import Anonymizer


class Grouping(Anonymizer):
    def __init__(self, keys: List[str], boundaries: List[float], group_names: List[str]):
        super().__init__(keys)
        self.boundaries = boundaries
        self.group_names = group_names
        assert len(self.boundaries) + 1 == len(self.group_names), "Boundaries + 1 and Group Names must be same lengths."

    def anonymize(self, item: str):
        from bisect import bisect_left

        index = bisect_left(self.boundaries, item)
        return self.group_names[index]
