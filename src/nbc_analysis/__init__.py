# -*- coding: utf-8 -*-
from pkg_resources import get_distribution, DistributionNotFound

try:
    # Change here if project is renamed and does not equal the package name
    dist_name = __name__
    __version__ = get_distribution(dist_name).version
except DistributionNotFound:
    __version__ = 'unknown'
finally:
    del get_distribution, DistributionNotFound

# import to top namespace
from .utils.config_utils import get_config
from nbc_analysis.batch.extract_file_lists import main as extract_file_lists
from nbc_analysis.batch.extract_events import main as extract_events
from nbc_analysis.batch.size_batches.main import main as size_batches
from nbc_analysis.utils.log_utils import get_logger, fmt_cfg
from nbc_analysis.calendar import create_day_calendar
from nbc_analysis.cli import cli

# from .agg_video_end.main import main as agg_video_end
# from .build_aggregate_run.main import main as build_aggregate_run
# from .concat_filtered_events.main import main as concat_filtered_events
# from .load_profiles.main import main as load_profiles
# from .load_profiles.main import load_schema

DATA_PATH = 'nbc_analysis.data'


def get_test_data(filename):
    import pandas as pd
    from importlib.resources import path as data_path
    with  data_path(DATA_PATH, filename) as fh:
        return pd.read_csv(fh)


def list_test_data():
    from importlib.resources import contents
    return list(x for x in contents(DATA_PATH) if not x.startswith('_'))
