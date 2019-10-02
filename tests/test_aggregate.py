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


@pytest.mark.skip()
def test_get_config():
    config = get_config(overrides={}, config_f='default')
    assert config['VIDEO_END_BUCKET'] == 'nbc-event'
    return config


def test_extract_events():
    return extract_events(config_f=CONFIG_F)


"""

@pytest.mark.skip()
def test_proc_events():
    return proc_events()


@pytest.mark.skip()
def test_build_aggregate_run():
    return build_aggregate_run()


@pytest.mark.skip()
def test_agg_video_end():
    return agg_video_end()


@pytest.mark.skip()
def test_concat_filtered_events():
    return concat_filtered_events()


@pytest.mark.skip()
def test_all():
    # run_extracts()
    test_size_batches()
    return proc_events()
"""
