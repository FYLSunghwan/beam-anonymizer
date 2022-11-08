import typing

import apache_beam as beam
import pytest
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from beam_anonymizer.ops.anonymizer import (
    Grouping,
    Masking,
    MicroAggregation,
    PartialSuppression,
    RecordSuppression,
    Rounding,
)


class Item(typing.NamedTuple):
    num: float
    item1: str
    item2: str


def test_grouping():
    input = [
        beam.Row(num=1, item1=1, item2="Group1"),
        beam.Row(num=2, item1=2, item2="Group1"),
        beam.Row(num=3, item1=3, item2="Group2"),
        beam.Row(num=4, item1=4, item2="Group2"),
        beam.Row(num=5, item1=5, item2="Group2"),
        beam.Row(num=6, item1=6, item2="Group3"),
    ]
    expected = [
        Item(num=1, item1="Group1", item2="Group1"),
        Item(num=2, item1="Group1", item2="Group1"),
        Item(num=3, item1="Group2", item2="Group2"),
        Item(num=4, item1="Group2", item2="Group2"),
        Item(num=5, item1="Group2", item2="Group2"),
        Item(num=6, item1="Group3", item2="Group3"),
    ]

    with TestPipeline() as p:
        output = (
            p
            | beam.Create(input)
            | beam.ParDo(Grouping(["item1"], [2, 5], ["Group1", "Group2", "Group3"])).with_output_types(Item)
        )
        assert_that(output, equal_to(expected))


def test_masking():
    input = [
        beam.Row(num=1, item1="123456789", item2="dummy"),
        beam.Row(num=2, item1="123", item2="dummy"),
    ]
    expected = [
        Item(num=1, item1="******789", item2="dummy"),
        Item(num=2, item1="***", item2="dummy"),
    ]

    with TestPipeline() as p:
        output = (
            p
            | beam.Create(input)
            | beam.ParDo(Masking(["item1"], 6, True)).with_output_types(Item)
        )
        assert_that(output, equal_to(expected))


def test_masking_right():
    input = [
        beam.Row(num=1, item1="123456789", item2="dummy"),
        beam.Row(num=2, item1="123", item2="dummy"),
    ]
    expected = [
        Item(num=1, item1="123******", item2="dummy"),
        Item(num=2, item1="***", item2="dummy"),
    ]

    with TestPipeline() as p:
        output = (
            p
            | beam.Create(input)
            | beam.ParDo(Masking(["item1"], 6, False)).with_output_types(Item)
        )
        assert_that(output, equal_to(expected))


def test_micro_aggregation():
    input = [
        beam.Row(num=1, item1="Item1", item2="Item2"),
        beam.Row(num=2, item1="Item1", item2="Item2"),
        beam.Row(num=3, item1="Item1", item2="Item2"),
        beam.Row(num=4, item1="Item1", item2="Item2"),
        beam.Row(num=5, item1="Item1", item2="Item2"),
        beam.Row(num=6, item1="Item1", item2="Item2"),
        beam.Row(num=7, item1="Item1", item2="Item2"),
    ]
    expected = [
        Item(num=2, item1="Item1", item2="Item2"),
        Item(num=2, item1="Item1", item2="Item2"),
        Item(num=2, item1="Item1", item2="Item2"),
        Item(num=5.5, item1="Item1", item2="Item2"),
        Item(num=5.5, item1="Item1", item2="Item2"),
        Item(num=5.5, item1="Item1", item2="Item2"),
        Item(num=5.5, item1="Item1", item2="Item2"),
    ]

    with TestPipeline() as p:
        output = (
            p
            | beam.Create(input)
            | MicroAggregation("num", 3).with_output_types(Item)
        )
        assert_that(output, equal_to(expected))


def test_partial_suppression():
    input = [
        beam.Row(num=1, item1="Partial1 Partial2 Partial3", item2="Item2"),
        beam.Row(num=2, item1="Partial4 Partial5 Partial6 Partial7", item2="Item2"),
        beam.Row(num=3, item1="Partial8", item2="Item2"),
    ]
    expected = [
        Item(num=1, item1="Partial1 Partial2", item2="Item2"),
        Item(num=2, item1="Partial4 Partial5", item2="Item2"),
        Item(num=3, item1="Partial8", item2="Item2"),
    ]

    with TestPipeline() as p:
        output = (
            p
            | beam.Create(input)
            | beam.ParDo(PartialSuppression(["item1"], 2)).with_output_types(Item)
        )
        assert_that(output, equal_to(expected))
