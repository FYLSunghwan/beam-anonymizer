from typing import List

from .base import Anonymizer


class Masking(Anonymizer):
    def __init__(self, keys: List[str], max_mask_len: int, align_left: bool = True):
        super().__init__(keys)
        self.max_mask_len = max_mask_len
        self.align_left = align_left

    def anonymize(self, item: str):
        if self.align_left:
            annonymized = "*" * min(self.max_mask_len, len(item)) + item[self.max_mask_len :]
        else:
            reverse_item = item[::-1]
            annonymized = "*" * min(self.max_mask_len, len(reverse_item)) + reverse_item[self.max_mask_len :]
            annonymized = annonymized[::-1]
        return annonymized
