import pandas as pd
from pathlib import Path
from nbc_analysis import get_config
from nbc_analysis.utils.file_utils import init_dir


def get_batch_id(file):
    return file.stem.split('.')[0]


def get_day(batch_id):
    day = batch_id.split('_')[1]
    return day


def calc_profiles(file):
    batch_id = get_batch_id(file)
    day = get_day(batch_id)
    df = pd.read_parquet(file, columns=['mpid'])
    print(f">> read {batch_id}, {len(df)}")
    df = df.mpid.value_counts()
    df = df.to_frame().reset_index()
    df['batch_id'] = batch_id
    df['day'] = day
    df.rename(columns={'mpid': 'event_cnt', 'index': 'mpid'}, inplace=True)
    df['batch_unique_cnt'] = 1
    return df


def write_viewer_counts(config_f, indir):
    config = get_config(config_f=config_f)
    indir = Path(indir)
    analysis_d = Path(config['ANALYSIS_D'])
    init_dir(analysis_d, exist_ok=True, rmtree=True)
    files = indir.glob('*')
    df = pd.concat(map(calc_profiles, files))
    outfile = analysis_d / 'viewer_counts.parquet'
    df.to_parquet(outfile, index=False)
    print(f"wrote viewer counts,file={outfile}],cnt={len(df)}")
    return df
