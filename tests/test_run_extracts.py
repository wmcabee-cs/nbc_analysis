# -*- coding: utf-8 -*-
import pytest
from pathlib import Path

from nbc_analysis import run_extracts
from nbc_analysis import proc_events
from nbc_analysis import size_batches
from nbc_analysis import build_aggregate_run
from nbc_analysis import agg_video_end
from nbc_analysis import concat_filtered_events

__author__ = "William McAbee"
__copyright__ = "William McAbee"
__license__ = "mit"


@pytest.mark.skip()
def test_run_extracts():
    return run_extracts()


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


def test_concat_filtered_events():
    return concat_filtered_events()


@pytest.mark.skip()
def test_all():
    # run_extracts()
    test_size_batches()
    return proc_events()
