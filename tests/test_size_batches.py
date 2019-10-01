# -*- coding: utf-8 -*-
import pytest
from nbc_analysis import get_config, size_batches
from nbc_analysis.utils.debug_utils import retval

__author__ = "William McAbee"
__copyright__ = "William McAbee"
__license__ = "mit"

# TODO: Implement method for controlling sequencing between tests

CONFIG_F = 'default'


def test_size_batches():
    size_batches(config_f=CONFIG_F)
    return None
