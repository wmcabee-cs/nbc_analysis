from typing import Dict, Optional
import os
import yaml
from toolz import merge
from pathlib import Path
from .file_utils import init_dir
from .debug_utils import retval

CONFIG_TOP = Path.home() / '.config' / 'nbc_analysis'
init_dir(CONFIG_TOP, exist_ok=True, parents=True)

# CHANGE TO TOML

DEFAULT_CONFIG = {
    'CALENDAR_START_DAY_KEY': '20170101',
    'CALENDAR_END_DAY_KEY': '20210101',

    'CALENDAR_D': '$NBC_DATA_TOP/calendar',
    'ANALYSIS_D': '$NBC_DATA_TOP/ana',
    'RUNS_D': '$NBC_DATA_TOP/runs',
    'WORK_D': '$NBC_DATA_TOP/work',

    'VIDEO_END_BUCKET': 'nbc-event',
    'VIDEO_END_PARTITIONS_BUCKET': 'nbc-partitions-video-end',
    'BATCH_SIZE': 2 * 10 ** 8,  # start new batch when cummulative size gets to this limit
    'GEOLITE2_DB': '$NBC_DATA_TOP/datasets/GeoLite2-City_20191001/GeoLite2-City.mmdb',
    'VIEWER_PARTITION_NUM': 60,
    'VIEWER_PARTITION_D': '$NBC_DATA_TOP/viewer_partitions',

    # FOR DEVELOPMENT
    'LIMIT_FILES_PER_DAY': 2000,  # 2000,  # FOR DEV. Max # of files in the file list each day
    'BATCH_LIMIT': 2,
    'BATCH_FILES_LIMIT': 2,

    # 'LIMIT_FILE_LISTS': 10,  # FOR DEV. # of days that will be included in batch spec file
}
WEEK_PATHS = {
    'BATCHES_D': '{run_d}/{week_id}/batches',
    'FILE_LISTS_D': '{run_d}/{week_id}/file_lists',
    'BATCH_SPEC_D': '{run_d}/{week_id}/batch_spec',
}


def check_data_top():
    data_top = os.environ.get('NBC_DATA_TOP', None)
    if data_top is None:
        raise Exception("Must set environment variable NBC_DATA_TOP to run these scripts")


def write_config_yaml(outfile, config):
    with outfile.open('w') as fh:
        yaml.dump(config, fh, default_flow_style=False)
        print(f">> created config file '{outfile}'")


def read_config_yaml(infile):
    infile = Path(infile)
    if not infile.is_file():
        raise Exception(f"ERROR,Config file not found,path='{infile}'")

    config = yaml.safe_load(infile.read_text())
    print(f">> loaded config '{infile}'")
    return config


def write_example_config(outdir):
    outdir = outdir or '.'
    outdir = Path(outdir)
    example_config_f = outdir / "config_example.yaml"
    write_config_yaml(outfile=example_config_f, config=DEFAULT_CONFIG)
    print(f">> created example config file '{example_config_f}'")


def _get_config(config):
    if isinstance(config, dict):
        return config

    if config == 'default':
        print(">> Using default config")
        return DEFAULT_CONFIG

    config_f = config or CONFIG_TOP / "config.yaml"
    config_f = Path(config_f)
    config = read_config_yaml(config_f)
    return config


def _get_run_config_f(runs_d, run):
    # Check if path to run directory

    if run is None:
        raise Exception("Must pass a run")

    # option 1. path to config file
    path = Path(run)
    if path.is_file():
        run_config_f = path
        return run_config_f

    # option 2. path to directory containing config file
    if path.is_dir():
        run_config_f = path / 'run_config.yaml'
        if run_config_f.is_file():
            return run_config_f

    # option 3. run id under work directory

    run_id = run
    run_d = runs_d / run_id
    run_config_f = run_d / 'run_config.yaml'
    if run_config_f.is_file():
        return run_config_f
    raise Exception(f"Unable to locate run config using run_id={run_id},run_config_f={run_config_f}")


def get_config(*, config: Optional[str] = None, **overrides) -> Dict:
    check_data_top()

    config = _get_config(config)
    if overrides is not None and len(overrides) != 0:
        print(f'>> WARNING: Overriding config file values with : {overrides}')
        config = merge(config, overrides)

    # Expand directories

    for name, value in config.items():
        if isinstance(value, str):
            config[name] = os.path.expandvars(value)

    return config


def get_run_config(config, run):
    config = get_config(config=config)
    runs_d = Path(config['RUNS_D'])

    run_config_f = _get_run_config_f(runs_d=runs_d, run=run)
    run_config = read_config_yaml(infile=run_config_f)

    run_d = str(run_config_f.parent)
    run_config = merge(config, run_config)
    run_config['RUN_D'] = run_d
    return run_config


def get_week_config(run_config, week_id, days):
    run_d = run_config['RUN_D']
    week_paths = {key: value.format(run_d=run_d, week_id=week_id) for key, value in WEEK_PATHS.items()}
    cfg = {'WEEK_ID': week_id, 'DAYS': list(days), 'WEEK_D': f"{run_d}/{week_id}"}
    week_config = merge(run_config, week_paths, cfg)
    return week_config
