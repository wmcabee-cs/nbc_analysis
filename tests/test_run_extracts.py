# -*- coding: utf-8 -*-
import pytest
from pathlib import Path

from nbc_analysis import get_config, get_test_data, list_by_day, extract_events
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


@pytest.mark.skip()
def test_list_by_day():
    days = get_test_data('days.csv')
    config = test_get_config()
    batch_limit = config.get('BATCH_LIMIT', None)
    days = days[:batch_limit].day.astype(np.str).tolist()

    list_by_day(days=days, config_f=CONFIG_F)
    return None


def test_extract_events():
    return extract_events(config_f=CONFIG_F)


@pytest.mark.skip()
def test_size_batches():
    return size_batches()


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
