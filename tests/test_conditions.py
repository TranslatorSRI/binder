"""Test generating SQL conditions."""
from binder.util import build_conditions

from .logging_setup import setup_logger


setup_logger()


def test_condition():
    """Test condition generation."""
    assert build_conditions(**{
        "a": 5,
    }) == ("a == ?", (5,))
    assert build_conditions(**{
        "a": 5,
        "b": 4,
    }) == ("(a == ?) AND (b == ?)", (5, 4))
    assert build_conditions(**{
        "$or": [
            {"a": 5},
            {"b": 4},
        ],
        "c": 3,
    }) == ("((a == ?) OR (b == ?)) AND (c == ?)", (5, 4, 3))
    assert build_conditions(**{
        "a": {"$ge": 5},
    }) == ("a >= ?", (5,))
    assert build_conditions(**{
        "a": {"$in": [1, 2]},
    }) == ("a in (?, ?)", (1, 2))
