from nbc_analysis import get_config
from nbc_analysis.utils.file_utils import init_dir
from pathlib import Path

import pandas as pd
from toolz import take
from nbc_analysis.utils.debug_utils import retval


def main(config_f, **overrides):
    config = get_config(config_f=config_f, overrides=overrides)
    indir = Path(config['BATCHES_D'])
    outdir = Path(config['WORK_D'])
    init_dir(outdir, exist_ok=True)

    outfile = outdir / 'merged_ve_events.parquet'

    reader = indir.glob('*')
    # reader = take(10, reader)
    files = list(reader)

    print(f">> Loading batches,batch_cnt={len(files)},indir={indir}")
    reader = map(pd.read_parquet, files)
    df = pd.concat(reader)
    print(">> Building index on {mpid,event_start_unixtime_ms}")
    df.set_index(['mpid', 'event_start_unixtime_ms'], inplace=True)
    print(">> Sorting index")
    df.sort_index(inplace=True)
    # print(f">> writing to disk,outfile={outfile},records={len(df)}")
    # df.to_parquet(outfile, index=True)
    print(f">> end merge ve_events,outfile={outfile},records={len(df)}")

    return df
