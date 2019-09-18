from typing import Dict, Optional
import os
import yaml
from pathlib import Path
from .file_utils import init_dir
from .debug_utils import retval

CONFIG_TOP = Path.home() / '.config' / 'nbc_analysis'

DEFAULT_CONFIG = {
    'EVENT_SET_D': '$DATA_TOP/NBC2/event_set',
    'LIMIT': 3000,
    'RAW_EVENTS_BUCKET': 'nbc-digital-cloned',
    'EXTRACT_SPECS': {
        'android': {'prefix': 'NBCProd/Android/NBC_{day}'},
        # 'roku': {'prefix': 'NBCProd/Roku/NBC_{day}'},
        # 'web': {'prefix': 'NBCProd/Web/NBC_App_{day}'},
        #'ios': {'prefix': 'NBCProd/iOS/NBCUniversal_{day}'},
        #'tvOS': {'prefix': 'NBCProd/tvOS/NBC_{day}'},
    },
    # 'DAYS': ['20190719', '20190720', '20190721', '20190722'],
    'DAYS': ['20190701', '20190702'],
}


def check_data_top():
    data_top = os.environ.get('DATA_TOP', None)
    if data_top is None:
        raise Exception("Must set environment variable DATA_TOP to run this script")


def get_config(config_f=None) -> Dict:
    check_data_top()
    if not config_f:
        init_dir(CONFIG_TOP, exist_ok=True, parents=True)
        config_f = CONFIG_TOP / "extracts.yaml"

    if not config_f.is_file():
        with config_f.open('w') as fh:
            print(f"created example config file at: {config_f}")
            yaml.safe_dump(DEFAULT_CONFIG, fh)

    config = yaml.safe_load(config_f.read_text())

    config['EVENT_SET_D'] = os.path.expandvars(config['EVENT_SET_D'])

    return config
