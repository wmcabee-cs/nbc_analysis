from typing import Dict
from pathlib import Path
from toolz import concatv, first

from ..file_utils import init_dir
import pandas as pd
from ...utils.debug_utils import retval


def write_event_batches(config, reader):
    # read configuration
    extract_d = Path(config['EVENT_BATCHES_D'])
    extract_d = init_dir(extract_d, parents=True, exist_ok=True, rmtree=True)

    for (day, asof_dt), df in reader:
        filename = f've_{day}_{asof_dt}.csv.gz'
        outfile = extract_d / filename
        df.to_csv(outfile, index=False)
        print(f">> wrote {outfile},cnt={len(df)}")


def mk_batch_id(file):
    file = file.stem
    batch_id = file.replace('.csv', '').replace('.gz', '')
    return  batch_id


def read_event_batches(config):
    batches_d = Path(config['EVENT_BATCHES_D'])
    batches = sorted(concatv(batches_d.glob('*.csv'), batches_d.glob('*.csv.gz')))
    reader = ((mk_batch_id(file), pd.read_csv(file)) for file in batches)
    return reader
