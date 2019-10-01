from pathlib import Path
from nbc_analysis.utils.config_utils import get_config
from toolz import take, partial
from nbc_analysis.utils.file_utils import init_dir
import pandas as pd
import numpy as np

from nbc_analysis.utils.debug_utils import retval


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


def get_file_lists(config, ):
    indir = Path(config['FILE_LISTS_D'])
    limit_file_lists = config.get('LIMIT_FILE_LISTS')

    reader = indir.glob('ve_*')
    reader = take(limit_file_lists, reader)
    return reader


def main(config_f):
    config = get_config(config_f=config_f)
    batch_size = config['BATCH_SIZE']
    batch_spec_d = Path(config['BATCH_SPEC_D'])

    init_dir(batch_spec_d, rmtree=True)

    reader = get_file_lists(config)
    reader = map(partial(size_batches, batch_size=batch_size), reader)
    df = pd.concat(reader)
    df.sort_values(['batch_id', 'order_idx'], inplace=True)

    outfile = batch_spec_d / 'batch_to_file.csv'
    df.to_csv(outfile, index=False)
    print(f">> wrote outfile={outfile},cnt={len(df)}")

    df = df.groupby(['batch_id', 'day', 'batch_num']).agg(
        {'size': np.sum, 'file': np.size, 'file_dt': np.min}).reset_index()
    df = df.rename(columns={'size': 'total_size', 'file': 'file_cnt', 'file_dt': 'file_dt_min'})

    outfile = batch_spec_d / 'batches.csv'
    df.to_csv(outfile, index=False)
    print(f">> wrote outfile={outfile},cnt={len(df)}")
    return df
