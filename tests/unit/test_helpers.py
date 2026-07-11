"""Tests for helper utilities."""

from __future__ import annotations

import numpy as np
import pytest

from retail_lakehouse.config.settings import WeightedOption
from retail_lakehouse.utils.helpers import (
    choose_weighted,
    generate_id_sequence,
    round_currency,
    weighted_choice,
)


def test_generate_id_sequence() -> None:
    ids = generate_id_sequence(1, 5)
    assert list(ids) == [1, 2, 3, 4, 5]


def test_generate_id_sequence_rejects_negative_count() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        generate_id_sequence(1, -1)


def test_weighted_choice_respects_distribution() -> None:
    rng = np.random.default_rng(42)
    options = [
        WeightedOption(name="a", weight=0.9),
        WeightedOption(name="b", weight=0.1),
    ]
    results = [weighted_choice(rng, options) for _ in range(1000)]
    assert results.count("a") > results.count("b")


def test_choose_weighted_returns_item() -> None:
    rng = np.random.default_rng(0)
    items = ["x", "y", "z"]
    weights = [1.0, 1.0, 1.0]
    assert choose_weighted(rng, items, weights) in items


def test_round_currency() -> None:
    assert round_currency(12.3456) == 12.35
