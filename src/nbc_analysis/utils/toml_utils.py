from typing import Dict, Optional, Union, Any
import os
from toolz import merge
from pathlib import Path
from .file_utils import init_dir
from .debug_utils import retval
from collections import namedtuple
import toml

from .log_utils import get_logger, fmt_cfg

log = get_logger(__name__)

TEST_CONFIG = '''

[demographics] 
demographics_d = '{NBC_DATA_TOP}/demographics'

# input files
zip2income_input_f = '{NBC_PROJ_TOP}/datasets/ACS_17_5YR_S2503_with_ann.csv'
subnet2zip_input_f = '{NBC_PROJ_TOP}/datasets/GeoLite2-City-CSV_20191001.zip'

# output files
subnet2inc_filename = 'subnet2inc'


# For development 
#record_limit=2000 # Load only first N records of zipcode dataset. For development

[normalize]
normalize_d = '{NBC_DATA_TOP}/normalize'
'''


def _check_dir(environ_name, init):
    adir = os.environ.get(environ_name, None)
    if adir is None:
        raise Exception(f'Must define environment variable {environ_name}')
    adir = Path(adir)
    if init:
        init_dir(adir, exist_ok=True)
    if not adir.is_dir:
        raise Exception(f'Must first create directory {environ_name}, "{adir}"')
    return adir


def _get_fs_environ(init=False):
    # Get DATA_TOP
    FileSystemEnviron = namedtuple('FileSystemEnviron', ['NBC_DATA_TOP', 'NBC_PROJ_TOP', 'NBC_CONFIG_F'])

    return FileSystemEnviron(NBC_DATA_TOP=_check_dir('NBC_DATA_TOP', init=init),
                             NBC_PROJ_TOP=_check_dir('NBC_PROJ_TOP', init=init),
                             NBC_CONFIG_F=os.environ.get('NBC_CONFIG_F', None),
                             )


def _proc_value(key, value):
    if isinstance(value, dict):
        return _proc_dict(value)
    if isinstance(value, str):
        if key.endswith('_f') or value.endswith('_d'):
            return Path(value)
        return value
    return value


def _proc_dict(adict):
    return {key: _proc_value(key, value) for key, value in adict.items()}


def _convert_to_pathlib(config):
    return _proc_dict(config)


def _parse_config(config_text):
    return toml.loads(config_text)


def _merge_overrides(config, overrides):
    return config


def _get_config_text(config, fs_environ):
    """ Get config text by
    if 'test', test configuration
    if config is path, text at that path
    if NBC_CONFIG_F set, text at that path
    otherwise use ~/.config/nbc_analysis/config.toml
    """

    if config == 'test':
        return TEST_CONFIG

    if config is None:
        config = fs_environ.NBC_CONFIG_F or str(Path.home() / '.config' / 'nbc_analysis' / 'config.toml')
    assert isinstance(config, str)

    config = Path(config)
    if not config.is_file():
        raise Exception(f"Could not find config file '{config}'")

    config_text = config.read_text()
    return config_text


def _replace_variables(config_text, variables):
    return config_text.format(**variables)


def _get_config_dict(config, config_text, variables, fs_environ):
    if isinstance(config, Dict):
        return config
    if config_text is None:
        config_text = _get_config_text(config=config, fs_environ=fs_environ)

    variables = merge(fs_environ._asdict(), variables)
    config_text = _replace_variables(config_text, variables=variables)
    config = _parse_config(config_text=config_text)
    return config


def get_config(config: Optional[Union[str, Dict[str, Any]]] = None,
               init: Optional[bool] = False,
               variables: Optional[Dict[str, Any]] = None,
               config_text: Optional[str] = None
               ) -> Dict:
    variables = {} if variables is None else variables
    if config and config_text:
        raise Exception("Can only specify config or config_text, not both")

    fs_environ = _get_fs_environ(init=init)
    config = _get_config_dict(config=config,
                              config_text=config_text,
                              variables=variables,
                              fs_environ=fs_environ)
    config = _convert_to_pathlib(config)
    return config
