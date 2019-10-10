from typing import Dict
from pathlib import Path
from toolz import concatv, first

from ..file_utils import init_dir
import pandas as pd
from ...utils.debug_utils import retval


def write_file_lists(week_config, reader):
    # read configuration
    outdir = Path(week_config['FILE_LISTS_D'])
    outdir = init_dir(outdir, parents=True, exist_ok=True, rmtree=True)

    for day, df in reader:
        filename = f've_{day}.csv.gz'
        outfile = outdir / filename
        if len(df) == 0:
            print(f">> No events for dat={day}, skipping")
        else:
            df.to_csv(outfile, index=False)
            print(f">> wrote {outfile},cnt={len(df)}")

def write_event_batches(config, reader):
    # read configuration
    extract_d = Path(config['EVENT_BATCHES_D'])
    extract_d = init_dir(extract_d, parents=True, exist_ok=True, rmtree=True)

    for (day, asof_dt), df in reader:
        filename = f've_{day}_{asof_dt}.csv.gz'
        outfile = extract_d / filename
        df.to_csv(outfile, index=False)
        print(f">> wrote {outfile},cnt={len(df)}")


def read_event_batches(batch_spec_d, batch_limit, batch_files_limit):
    infile = batch_spec_d / 'batch_to_file.csv'
    files = pd.read_csv(infile)

    if batch_limit is not None:
        print(f">> WARNING: Limit batch count,batch_limit={batch_limit}")
    if batch_files_limit is not None:
        print(f">> WARNING: Limit files in batch to first n,batch_files_limit={batch_files_limit}")

    def get_files_for_batch(batch):
        df = files[files.batch_id == batch.batch_id]
        if batch_files_limit is not None:
            df = df.iloc[:batch_files_limit]
        df = df.copy()
        df.set_index('order_idx', inplace=True)
        return batch, df

    infile = batch_spec_d / 'batches.csv'
    batches = pd.read_csv(infile)
    if batch_limit is not None:
        batches = batches[:batch_limit]
    reader = batches.itertuples(name="Batch")
    reader = map(get_files_for_batch, reader)

    return reader
