from typing import Dict
from pathlib import Path

from ..file_utils import init_dir
import pandas as pd


def write_event_batches(config, reader):
    # read configuration
    extract_d = Path(config['EVENT_BATCHES_D'])
    extract_d = init_dir(extract_d, parents=True, exist_ok=True, rmtree=True)

    for (day, asof_dt), df in reader:
        filename = f've_{day}_{asof_dt}.csv.gz'
        outfile = extract_d / filename
        df.to_csv(outfile, index=False)
        print(f">> wrote {outfile},cnt={len(df)}")


def read_event_batches(config, batches):
    if batches is None:
        batches_d = Path(config['EVENT_BATCHES_D'])
        batches = sorted(batches_d.glob('*'))
    reader = ((file, pd.read_csv(file)) for file in batches)
    return reader
