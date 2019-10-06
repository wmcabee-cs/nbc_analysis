from typing import Dict, Optional
import os
import yaml
from toolz import merge
from pathlib import Path
from .file_utils import init_dir
from .debug_utils import retval

CONFIG_TOP = Path.home() / '.config' / 'nbc_analysis'

DEFAULT_CONFIG = {

    'VIDEO_END_BUCKET': 'nbc-event',
    'BATCHES_D': '$DATA_TOP/NBC2/batches',
    'FILE_LISTS_D': '$DATA_TOP/NBC2/file_lists',
    'BATCH_SPEC_D': '$DATA_TOP/NBC2/batch_spec',
    'DAYS_D': '$DATA_TOP/NBC2/days',
    'ANALYSIS_D': '$DATA_TOP/NBC2/ana',
    'BATCH_SIZE': 2 * 10 ** 8,  # start new batch when cummulative size gets to this limit
    'WORK_D': '$DATA_TOP/NBC2/work',
    'GEOLITE2_DB': Path.home() / '_NBC/datasets/GeoLite2-City_20191001/GeoLite2-City.mmdb',

    # FOR DEVELOPMENT
    'DAYS_LIMIT': 2,  # FOR DEV. Number of days to process before stopping
    'LIMIT_FILES_PER_DAY': None,  # 2000,  # FOR DEV. Max # of files in the file list each day
    'LIMIT_FILE_LISTS': None,  # FOR DEV. # of days that will be included in batch spec file
    'BATCH_LIMIT': 2,
    'BATCH_FILES_LIMIT': 2,

    ############
    # ?? 'LIMIT_EVENTS_PER_BATCH': 50000,  # FOR DEV. Normally should be None

    # 'BATCHES_D': '$DATA_TOP/NBC2/batches',
    # 'LIMIT_EVENT_CNT': 3000,
    # 'CONCAT_D': '$DATA_TOP/NBC2/concat',
    # 'AGGREGATES_D': '$DATA_TOP/NBC2/aggregates',
    # 'EVENT_LIMIT': 100,
    # 'DAYS': MONTH_DAYS,
    # 'CORTEX_SCHEMA_VERSION': 'nbc/User',
}


def check_data_top():
    data_top = os.environ.get('DATA_TOP', None)
    if data_top is None:
        raise Exception("Must set environment variable DATA_TOP to run this script")


def write_example_config(config_top):
    example_config_f = config_top / "config_example.yaml"
    with example_config_f.open('w') as fh:
        yaml.dump(DEFAULT_CONFIG, fh, default_flow_style=False)
        print(f">> created example config file '{example_config_f}'")


def _get_config(config_f):
    if config_f == 'default':
        config = DEFAULT_CONFIG
        print(">> Using default config")
    else:
        config_f = Path(config_f)
        if not config_f.is_file():
            raise Exception(f"config file not found, '{config_f}'")

        config = yaml.safe_load(config_f.read_text())
        print(f">> loaded config '{config_f}'")
    return config


def get_config(*, overrides: Optional[Dict] = None,
               config_f: Optional[str] = None) -> Dict:
    if isinstance(config_f, dict):
        return config_f

    check_data_top()
    init_dir(CONFIG_TOP, exist_ok=True, parents=True)
    default_config_f = CONFIG_TOP / "config.yaml"
    config_f = config_f or default_config_f
    write_example_config(config_top=CONFIG_TOP)

    # merge overrides
    config = _get_config(config_f)
    if overrides is not None and len(overrides) != 0:
        print(f'>> WARNING: Overriding config file values with : {overrides}')
        config = merge(config, overrides)

    # Expand directories
    dir_keys = list(filter(lambda x: x.endswith('_D'), config.keys()))
    for name in dir_keys:
        config[name] = os.path.expandvars(config[name])

    return config
