from typing import Callable, List

from .base import Anonymizer


class RecordSuppression(Anonymizer):
    def __init__(self, keys: List[str], condition_fn: Callable, id_key: str = "일련번호"):
        super().__init__(keys)
        self.condition_fn = condition_fn
        self.id_key = id_key

    def process(self, element):
        delete_condition = False
        for key in self.keys:
            if self.condition_fn(getattr(element, key)):
                delete_condition = True
                break

        if delete_condition:
            replace_dict = {}
            for key in element._fields:
                if key != self.id_key:
                    replace_dict[key] = None
            element = element._replace(**replace_dict)

        yield element
