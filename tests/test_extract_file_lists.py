# -*- coding: utf-8 -*-
import pytest
from pathlib import Path

from nbc_analysis import get_config, get_test_data, extract_file_lists, extract_events
from nbc_analysis.utils.debug_utils import retval
import numpy as np

__author__ = "William McAbee"
__copyright__ = "William McAbee"
__license__ = "mit"

# TODO: Implement method for controlling sequencing between tests

CONFIG_F = 'default'


def test_get_config():
    config = get_config(overrides={}, config_f='default')
    assert config['VIDEO_END_BUCKET'] == 'nbc-event'
    return config


def _get_days():
    days = get_test_data('days.csv')
    config = test_get_config()
    days_limit = config.get('DAYS_LIMIT', None)
    days = days[:days_limit].day.astype(np.str).tolist()
    return days


def test_extract_file_lists():
    days = _get_days()

    extract_file_lists(days=days, config_f=CONFIG_F)
    return None
