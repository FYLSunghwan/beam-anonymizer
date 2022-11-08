from typing import List, Optional

from .base import Anonymizer


class PartialSuppression(Anonymizer):
    def __init__(self, keys: List[str], max_items: int, delimiter: str = " "):
        super().__init__(keys)
        self.max_items = max_items
        self.delimiter = delimiter

    def anonymize(self, item: str) -> str:
        items = item.split(self.delimiter)
        if len(items) > self.max_items:
            return self.delimiter.join(items[:self.max_items])
        return item
