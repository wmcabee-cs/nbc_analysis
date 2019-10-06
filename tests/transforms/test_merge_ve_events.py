from pathlib import Path
import pandas as pd
from numpy.random import randint
import numpy as np
from toolz import first, merge

from itertools import groupby

from nbc_analysis import get_config
from nbc_analysis.transforms import merge_video_ends, get_ip_info, IPInfo, init_ip_db

import json


def _get_test_config():
    return get_config(config_f='default',
                      overrides={
                          'BATCHES_D': '/Users/wmcabee/Dropbox (Cognitive Scale)/NBC Analysis/data/NBC2/batches'})


def _get_ip_db():
    return init_ip_db(config_f=_get_test_config())


def test_merge_ve_events():
    return merge_video_ends(config_f=_get_test_config())


def test_get_ip_info(ips_db, ips):
    return get_ip_info(ips_db=ips_db, ips=ips)
