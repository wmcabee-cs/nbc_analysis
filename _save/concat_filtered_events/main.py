from nbc_analysis.utils.config_utils import get_config
from nbc_analysis.utils.debug_utils import retval
from nbc_analysis.utils.file_utils import init_dir
from pathlib import Path
import pandas as pd
import re
from toolz import first
from itertools import starmap
from functools import partial


def group_files_by_day(indir):
    def parse_name(file):
        return re.search(r"(?P<day>\d+)\.csv.gz", file).group('day'), file

    reader = map(str, indir.glob('*.csv.gz'))
    reader = map(parse_name, reader)
    df = pd.DataFrame.from_records(reader, columns=['day', 'agg_file'], index=['day'])
    df.sort_index(inplace=True)
    return df.groupby(level='day')


def proc_day(day, df, outdir):
    print(f">> starting concat for day {day}")
    reader = map(pd.read_csv, df.agg_file)
    df = pd.concat(reader)
    df['day'] = day
    df.set_index(['mpid', 'day', 'end_ts'], inplace=True)
    df.sort_index(inplace=True)
    df = df.reset_index(level='end_ts')
    index_names = df.index.names
    grouping = df.groupby(level=index_names)
    dx = grouping[['video_cnt', 'ad_watch_cnt', 'ad_duration_watched', 'duration_watched']].sum()
    dx['ads_per_video_cnt_avg'] = dx.ad_watch_cnt.div(dx.video_cnt).round(1)
    dx['ad_vs_video_time_avg'] = dx.ad_duration_watched.div(dx.duration_watched).round(1)
    dx['agg_rec_cnt'] = grouping.size()
    print(">> starting show frequency counts")
    dx['most_frequent_show'] = grouping[['show']].agg(lambda x: pd.Series.mode(x)[0])
    print(">> starting platform frequency counts")
    dx['most_frequent_platform'] = grouping[['platform']].agg(lambda x: pd.Series.mode(x)[0])
    print(">> starting ip frequency counts")
    dx['most_frequent_ip'] = grouping[['ip']].agg(lambda x: pd.Series.mode(x)[0])
    dx.reset_index(inplace=True)

    outname = f"concat_{day}.csv.gz"
    outfile = outdir / outname
    dx.to_csv(outfile, index=False)
    print(f">> end {outname}")
    return None


def main():
    config = get_config()
    aggregates_d = Path(config['AGGREGATES_D'])
    platform = Path(config['PLATFORM'])
    concat_d = Path(config['CONCAT_D'])
    init_dir(concat_d, exist_ok=True)

    indir = aggregates_d / platform
    reader = group_files_by_day(indir)
    reader = starmap(partial(proc_day, outdir=concat_d), reader)
    for x in reader:
        pass

    return
