from pathlib import Path
from nbc_analysis.utils.config_utils import get_config
from toolz import take, partial
from nbc_analysis.utils.file_utils import init_dir
import pandas as pd
import numpy as np

from nbc_analysis.utils.debug_utils import retval
from ...utils.log_utils import get_logger

log = get_logger(__name__)


def size_batches(path: str, batch_size: int) -> pd.DataFrame:
    df = pd.read_csv(path)
    df = df.sort_values('file_dt')
    df.reset_index(inplace=True, drop=True)
    df.index.name = 'order_idx'
    df.reset_index(inplace=True)
    df['batch_num'] = df['size'].cumsum() // batch_size
    prefix = path.name.split('.')[0]
    df['batch_id'] = df.batch_num.map(lambda x: prefix + f"_{x:04d}")
    return df


def get_file_lists(file_lists_d, limit_file_lists):
    reader = file_lists_d.glob('ve_*')
    reader = take(limit_file_lists, reader)
    return reader


def main(week_config):
    run_id = week_config['RUN_ID']
    week_id = week_config['WEEK_ID']

    log.info(f"start size_batches,run_id={run_id},week_id={week_id}")
    batch_spec_d = Path(week_config['BATCH_SPEC_D'])
    batch_size = week_config['BATCH_SIZE']
    file_lists_d = Path(week_config['FILE_LISTS_D'])
    limit_file_lists = week_config.get('LIMIT_FILE_LISTS')

    init_dir(batch_spec_d, rmtree=True)

    reader = get_file_lists(file_lists_d=file_lists_d, limit_file_lists=limit_file_lists)
    files = list(reader)

    if len(files) == 0:
        log.warning(f"no events found,week_id={week_id}, skipping")
        return None

    reader = map(partial(size_batches, batch_size=batch_size), files)
    df = pd.concat(reader)
    df.sort_values(['batch_id', 'order_idx'], inplace=True)

    outfile = batch_spec_d / 'batch_to_file.csv'
    df.to_csv(outfile, index=False)
    log.info(f"wrote outfile={outfile},records={len(df)}")

    df = df.groupby(['batch_id', 'day', 'batch_num']).agg(
        {'size': np.sum, 'file': np.size, 'file_dt': np.min}).reset_index()
    df = df.rename(columns={'size': 'total_size', 'file': 'file_cnt', 'file_dt': 'file_dt_min'})

    outfile = batch_spec_d / 'batches.csv'
    df.to_csv(outfile, index=False)
    log.info(f"wrote outfile={outfile},log={len(df)}")
    return df
