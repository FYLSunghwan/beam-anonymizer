from typing import List

import apache_beam as beam


class Anonymizer(beam.DoFn):
    def __init__(self, keys: List[str]):
        self.keys = keys

    def anonymize(self, item):
        raise NotImplementedError("Must Implement anonymize()")

    def process(self, element):
        replace_dict = {}
        for key in self.keys:
            replace_dict[key] = self.anonymize(getattr(element, key))
        element = element._replace(**replace_dict)
        yield element
