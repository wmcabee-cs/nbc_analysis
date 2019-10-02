# -*- coding: utf-8 -*-
import pytest
from pathlib import Path

from nbc_analysis import extract_events
from nbc_analysis.utils.debug_utils import retval
import numpy as np

CONFIG_F = 'default'


def test_extract_events():
    return extract_events(config_f=CONFIG_F)
