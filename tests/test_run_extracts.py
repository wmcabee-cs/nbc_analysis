# -*- coding: utf-8 -*-
import pytest
from pathlib import Path

from nbc_analysis import run_extracts
from nbc_analysis import proc_events
from nbc_analysis import size_batches
from nbc_analysis import build_aggregate_run
from nbc_analysis import agg_video_end
from nbc_analysis import concat_filtered_events
from nbc_analysis import get_config
from importlib.resources import read_text, read_binary
from nbc_analysis.utils.debug_utils import retval

from nbc_analysis import get_test_data

__author__ = "William McAbee"
__copyright__ = "William McAbee"
__license__ = "mit"


def test_run_extracts():
    days = get_test_data('days.csv')
    days = days[:2]
    return run_extracts(days=days, use_default_config=True)


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
