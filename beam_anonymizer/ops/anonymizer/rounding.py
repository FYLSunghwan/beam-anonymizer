from typing import List, Optional

from .base import Anonymizer


class Rounding(Anonymizer):
    def __init__(self, keys: List[str], n: Optional[int] = None):
        super().__init__(keys)
        self.n = n

    def anonymize(self, item: int) -> int:
        return round(item, self.n)
