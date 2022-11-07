from typing import List

import apache_beam as beam
from apache_beam.pvalue import PCollection
from apache_beam.transforms.ptransform import PTransform


class QuasiIdentifiersToKey(beam.DoFn):
    def __init__(self, quasi_identifiers: List[str]):
        self.quasi_identifiers = quasi_identifiers

    def process(self, element: str):
        key = ""
        for quasi_identifier in self.quasi_identifiers:
            key += str(getattr(element, quasi_identifier))
        yield key, element


class GetKAnonymity(PTransform):
    """
    주어진 데이터로부터 K-익명성 수치를 측정합니다.
    """

    def __init__(self, quasi_identifiers: List[str]):
        self.quasi_identifiers = quasi_identifiers

    def expand(self, pcoll: PCollection) -> PCollection:
        """
        K 익명성 수치를 측정합니다.
        """
        quasi_keyed = pcoll | "QuasiIdentifiersToKey" >> beam.ParDo(QuasiIdentifiersToKey(self.quasi_identifiers))
        duplicates = quasi_keyed | "CountSameQuasiIdentifiers" >> beam.combiners.Count.PerKey()
        k_anonymity = duplicates | "MinimumKAnonymity" >> beam.combiners.Top.Of(10, key=lambda x: x[1], reverse=True)
        distribution = (
            duplicates
            | "KvSwap" >> beam.KvSwap()
            | "GroupByKey" >> beam.GroupByKey()
            | "FilterHighKAnonymity" >> beam.Filter(lambda x: x[0] < 100)
            | "FormatK" >> beam.Map(lambda x: f"K: {x[0]}, Count: {len(x[1])}")
        )

        return {"k_anonymity": k_anonymity, "distribution": distribution}
