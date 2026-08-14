"""
Unit tests for data generator utility functions.
"""

from decimal import Decimal
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent / "data-generator"))
from faker_generator import random_money


def test_random_money_range():
    min_val = Decimal("10.00")
    max_val = Decimal("100.00")
    for _ in range(50):
        val = random_money(min_val, max_val)
        assert min_val <= val <= max_val
        # Ensure 2 decimal places
        assert val.as_tuple().exponent == -2


def test_random_money_exact():
    min_val = Decimal("50.00")
    max_val = Decimal("50.00")
    val = random_money(min_val, max_val)
    assert val == Decimal("50.00")
