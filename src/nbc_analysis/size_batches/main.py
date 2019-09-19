from toolz import take
import pandas as pd
from pathlib import Path
import pprint

from ..utils.config_utils import get_config
from ..utils.file_utils import init_dir

from ..utils.debug_utils import retval


def write_event_set_list(df, name, outdir):
    outfile = outdir / f'batches_{name}.csv'
    df.to_csv(outfile, index=False)
    print(f">> wrote event_set_list={outfile},records={len(df)}")


def read_event_sets(event_set_d, pattern, limit=None):
    event_set_d = Path(event_set_d)
    reader = event_set_d.glob(pattern)
    if limit is not None:
        reader = take(limit, reader)
    reader = (pd.read_csv(file) for file in reader)
    df = pd.concat(reader)
    df = df.rename(columns={'name': 'key'})

    return df


def main(config_f=None):
    # get inputs from config file
    print(">> init proc events")
    config = get_config(config_f)
    batch_size = config['EVENT_SETS_IN_BATCH']

    print(f">> start proc run, config={config}")
    event_set_d = Path(config['EVENT_SET_D'])
    work_d = Path(config['BATCHES_D'])
    init_dir(work_d, exist_ok=True, rmtree=False)

    name = 'android'
    df = read_event_sets(event_set_d, pattern=f'{name}*.csv', limit=None)
    df = df.sort_values('key')
    df = df.reset_index(drop=True)
    df['batch_num'] = df.index // batch_size
    df['batch_item'] = df.index % batch_size
    df['batch_id'] = 'b' + df['batch_num'].map("{:05d}".format) + '_' + df.extract + '_' + df['day'].astype(str)

    write_event_set_list(df, outdir=work_d, name=name)
    return df
