from typing import Dict, Optional
import os
import yaml
from toolz import merge
from pathlib import Path
from .file_utils import init_dir
from .debug_utils import retval

CONFIG_TOP = Path.home() / '.config' / 'nbc_analysis'

SAMPLE_DAYS = [
    '20190701',
    '20190702',
    '20190703',
    '20190704',
    '20190705',
    '20190706',
    '20190707',
    '20190708',
    '20190709',
    '20190710',
    '20190711',
    '20190712',
    '20190713',
    '20190714',
    '20190715',
    '20190716',
    '20190717',
    '20190718',
    '20190719',
    '20190720',
    '20190721',
    '20190722',
    '20190723',
    '20190724',
    '20190725',
    '20190726',
    '20190727',
    '20190728',
    '20190729',
    '20190730',
    '20190731',
]
SAMPLE_DAYS = SAMPLE_DAYS[:2]


DEFAULT_CONFIG = {

    # Temporary way to pass in list of days
    'DAYS': SAMPLE_DAYS,
    'VIDEO_END_BUCKET': 'nbc-event',
    'LIMIT_BATCH_CNT': 2,
    'LIMIT_EVENTS_PER_BATCH': 10,
    'EXTRACTS_D': '$DATA_TOP/NBC2/extracts',

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


def get_config(overrides: Dict, config_f=None) -> Dict:
    check_data_top()
    init_dir(CONFIG_TOP, exist_ok=True, parents=True)
    default_config_f = CONFIG_TOP / "extracts.yaml"
    config_f = config_f or default_config_f

    always_replace = Path(CONFIG_TOP / "always_replace_config.txt").is_file()

    if always_replace or not config_f.is_file():
        with config_f.open('w') as fh:
            yaml.safe_dump(DEFAULT_CONFIG, fh)
            print(f">> created default config file,config_f={config_f}")

    config = yaml.safe_load(config_f.read_text())
    config = merge(overrides, config)

    # Expand directories
    dir_keys = list(filter(lambda x: x.endswith('_D'), config.keys()))
    for name in dir_keys:
        config[name] = os.path.expandvars(config[name])

    return config
