# -*- coding: utf-8 -*-
import pytest
from pathlib import Path

from nbc_analysis import run_extracts
from nbc_analysis import proc_events
from nbc_analysis import size_batches

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


def test_all():
    # run_extracts()
    test_size_batches()
    return proc_events()
