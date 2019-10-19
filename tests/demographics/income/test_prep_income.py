import pytest
from nbc_analysis.demographics.prep_income import main as prep_income
from nbc_analysis.utils.debug_utils import retval


def test_prepare_income(config):
    retval(config)
