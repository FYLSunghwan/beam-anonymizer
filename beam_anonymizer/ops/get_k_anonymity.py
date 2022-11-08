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
        top_duplicates = duplicates | "MinimumKAnonymity" >> beam.combiners.Top.Of(10, key=lambda x: x[1], reverse=True)
        distribution = (
            duplicates
            | "KvSwap" >> beam.KvSwap()
            | "GroupByKey" >> beam.GroupByKey()
            | "FilterHighKAnonymity" >> beam.Filter(lambda x: x[0] < 200)
            | "CountItems" >> beam.Map(lambda x: (x[0], len(x[1])))
            | "ToList" >> beam.transforms.combiners.ToList()
            | "Sort" >> beam.Map(lambda x: sorted(x, key=lambda y: y[0]))
            | "Format" >> beam.Map(lambda x: "\n".join([f"K{item[0]}\t{item[1]}" for item in x]))
        )

        return {"top_duplicates": top_duplicates, "distribution": distribution}
