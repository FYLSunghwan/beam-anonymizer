from typing import Any, Callable, List, Iterable

import apache_beam as beam
from apache_beam.pvalue import PCollection
from apache_beam.transforms.ptransform import PTransform


class SortAndAggregate(beam.DoFn):
    def __init__(self, key: str, min_batch: int, aggregate_fn: Callable[[List[Any]], Any]):
        self.min_batch = min_batch
        self.key = key
        self.aggregate_fn = aggregate_fn

    def process(self, element: List[Any]) -> Iterable[Any]:
        element.sort(key=lambda x: getattr(x, self.key))
        batches = [element[i : i + self.min_batch] for i in range(0, len(element), self.min_batch)]
        if len(batches) > 1 and len(batches[-1]) < self.min_batch:
            batches[-2].extend(batches[-1])
            batches.pop()

        for batch in batches:
            aggregated = self.aggregate_fn([getattr(x, self.key) for x in batch])
            for item in batch:
                yield item._replace(**{self.key: aggregated})


class MicroAggregation(PTransform):
    def __init__(self, key: str, min_batch: int, aggregate_fn: Callable[[List[Any]], Any] = lambda x: sum(x) / len(x)):
        self.min_batch = min_batch
        self.key = key
        self.aggregate_fn = aggregate_fn

    def expand(self, pcoll: PCollection) -> PCollection:
        return (
            pcoll.pipeline
            | "CreateNone" >> beam.Create([None])
            | beam.Map(lambda _, pcoll_as_side: pcoll_as_side, pcoll_as_side=beam.pvalue.AsList(pcoll))
            | beam.ParDo(SortAndAggregate(self.key, self.min_batch, self.aggregate_fn))
        )
