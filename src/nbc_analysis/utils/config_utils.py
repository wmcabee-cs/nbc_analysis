from typing import Dict, Optional
import os
import yaml
from pathlib import Path
from .file_utils import init_dir
from .debug_utils import retval

CONFIG_TOP = Path.home() / '.config' / 'nbc_analysis'

MONTH_DAYS = [
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
SAMPLE_DAYS = ['20190701', '20190702']

DEFAULT_CONFIG = {
    'EVENT_SET_D': '$DATA_TOP/NBC2/event_set',
    'BATCHES_D': '$DATA_TOP/NBC2/batches',
    'LIMIT': 3000,
    'RAW_EVENTS_BUCKET': 'nbc-digital-cloned',
    'EXTRACT_SPECS': {
        'android': {'prefix': 'NBCProd/Android/NBC_{day}'},
        # 'roku': {'prefix': 'NBCProd/Roku/NBC_{day}'},
        # 'web': {'prefix': 'NBCProd/Web/NBC_App_{day}'},
        # 'ios': {'prefix': 'NBCProd/iOS/NBCUniversal_{day}'},
        # 'tvOS': {'prefix': 'NBCProd/tvOS/NBC_{day}'},
    },
    'PLATFORM': 'android',
    'BATCH_LIMIT': 1,
    'EVENT_LIMIT': 100,
    'DAYS': SAMPLE_DAYS,
    # 'DAYS': MONTH_DAYS,
    'EVENT_SETS_IN_BATCH': 10,
}


def check_data_top():
    data_top = os.environ.get('DATA_TOP', None)
    if data_top is None:
        raise Exception("Must set environment variable DATA_TOP to run this script")


def get_config(config_f=None) -> Dict:
    check_data_top()
    init_dir(CONFIG_TOP, exist_ok=True, parents=True)
    config_f = CONFIG_TOP / "extracts.yaml"
    always_replace = Path(CONFIG_TOP / "always_replace_config.txt").is_file()

    if always_replace or not config_f.is_file():
        with config_f.open('w') as fh:
            print(f"created example config file at: {config_f}")
            yaml.safe_dump(DEFAULT_CONFIG, fh)

    config = yaml.safe_load(config_f.read_text())

    # Expand directories
    for name in ['EVENT_SET_D', 'BATCHES_D']:
        config[name] = os.path.expandvars(config[name])

    return config
