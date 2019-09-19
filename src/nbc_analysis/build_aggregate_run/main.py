from ..utils.config_utils import get_config
from ..utils.file_utils import init_dir
from pathlib import Path
from toolz import concatv
import pandas as pd
import pprint


def main():
    config = get_config()
    print(f">> start build aggregations,config={config}")

    platform = config['PLATFORM']
    batches_d = Path(config['BATCHES_D'])
    aggregates_d = Path(config['AGGREGATES_D'])

    indir = batches_d / platform

    init_dir(aggregates_d, exist_ok=True)

    reader = concatv(indir.glob('*.csv.gz'), indir.glob('*.csv'))
    reader = sorted(str(x) for x in reader)
    df = pd.Series(reader).to_frame('batch_f')
    outfile = aggregates_d / f'aggregates_{platform}.csv'
    df.to_csv(outfile, index=False)
    print(f">> end build aggregations,outfile={outfile},len={len(df)}")

    return df
