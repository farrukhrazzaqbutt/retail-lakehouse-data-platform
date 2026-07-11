"""General-purpose helper functions for synthetic data generation."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Sequence, TypeVar

import numpy as np

from retail_lakehouse.config.settings import WeightedOption

T = TypeVar("T")


def ensure_directory(path: Path) -> Path:
    """
    Create a directory if it does not exist.

    Args:
        path: Directory path to create.

    Returns:
        The same path for chaining.
    """
    path.mkdir(parents=True, exist_ok=True)
    return path


def generate_id_sequence(start: int, count: int) -> np.ndarray:
    """
    Generate a contiguous array of integer identifiers.

    Args:
        start: First identifier value.
        count: Number of identifiers to generate.

    Returns:
        NumPy array of int64 IDs.
    """
    if count < 0:
        raise ValueError("count must be non-negative")
    return np.arange(start, start + count, dtype=np.int64)


def weighted_choice(
    rng: np.random.Generator,
    options: Sequence[WeightedOption],
) -> str:
    """
    Select a categorical value using normalized weights.

    Args:
        rng: NumPy random generator for reproducibility.
        options: Weighted categorical options.

    Returns:
        Selected option name.
    """
    if not options:
        raise ValueError("options must not be empty")
    weights = np.array([option.weight for option in options], dtype=np.float64)
    total = weights.sum()
    if total <= 0:
        raise ValueError("sum of weights must be positive")
    probabilities = weights / total
    index = int(rng.choice(len(options), p=probabilities))
    return options[index].name


def choose_weighted(
    rng: np.random.Generator,
    items: Sequence[T],
    weights: Sequence[float],
) -> T:
    """
    Choose an item from a sequence using explicit weights.

    Args:
        rng: NumPy random generator.
        items: Values to choose from.
        weights: Relative weights for each item.

    Returns:
        Selected item.
    """
    if len(items) != len(weights):
        raise ValueError("items and weights must have the same length")
    if not items:
        raise ValueError("items must not be empty")
    weight_array = np.array(weights, dtype=np.float64)
    total = weight_array.sum()
    if total <= 0:
        raise ValueError("sum of weights must be positive")
    index = int(rng.choice(len(items), p=weight_array / total))
    return items[index]


def round_currency(value: float) -> float:
    """Round a monetary value to two decimal places."""
    return round(float(value), 2)
