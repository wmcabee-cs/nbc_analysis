import s3fs
from pathlib import Path
from toolz import update_in
from nbc_analysis.utils.toml_utils import get_config
from nbc_analysis.app_io.clickstream_events import list_files


def _get_config():
    config = get_config('test')

    config = update_in(config, ['event_extract', 'file_list_limit'], lambda x: 10)
    config['ARGS'] = dict(
        start_day_key=20190730,
        end_day_key=20190731,
    )
    return config


def test_list_files():
    config = _get_config()
    list_files(config=config, **config['ARGS'])
