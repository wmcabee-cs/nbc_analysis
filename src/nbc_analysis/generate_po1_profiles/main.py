from nbc_analysis.utils.aws_utils import get_bucket
from nbc_analysis.utils.debug_utils import retval
from nbc_analysis.utils.func_utils import take_if_limit
from pathlib import Path
from itertools import starmap
import shutil

import pandas as pd
import numpy as np
import json
import gzip

from nbc_analysis.utils.log_utils import get_logger
from nbc_analysis.utils.file_utils import init_dir
from toolz import first, take, concat

from cortex_common.types import EntityEvent, StringAttributeValue, ListAttributeValue

log = get_logger(__name__)

LIST_CLEAN_FILES_F = 'list_clean_files.csv'


def list_clean_files(bucket):
    log.info(f"start list_clean_files")
    reader = ((x.key, x.size) for x in bucket.objects.filter(Prefix='clean').all())
    files = list(reader)
    df = pd.DataFrame.from_records(files, columns=['key', 'size'])
    df['week'] = df['key'].str.extract(r'/week=(\d+W\d\d)')
    df = df.sort_values('key', ascending=True)
    log.info(f"end list_clean_files")
    return df


def write_df2csv(df, outfile):
    df.to_csv(outfile, index=False)
    log.info(f"wrote {outfile},records={len(df)}")
    return df


def initialize(inbucket, work_d):
    log.info("start initialize_gen_profiles")
    init_dir(work_d, exist_ok=True, rmtree=True)

    outfile = work_d / LIST_CLEAN_FILES_F

    files = list_clean_files(inbucket)
    write_df2csv(df=files, outfile=outfile)
    log.info("end initialize_gen_profiles")


def download_files(bucket, keys, outdir, merge_limit=None):
    def download_file(key):
        name = Path(key).name
        outfile = str(outdir / name)
        log.info(f"start download_clean_file,key={key},outfile={name}")
        bucket.download_file(key, outfile)
        log.info(f"end download_clean_file,key={key}")

    reader = take_if_limit(limit=merge_limit, reader=keys, msg="MERGE_LIMIT set")
    reader = (download_file(key) for key in reader)
    for x in reader: pass


def load_files(indir):
    files = list(indir.glob('*.parquet'))
    for idx, file in enumerate(files, start=1):
        df = pd.read_parquet(file)
        log.info(f"start file,file={file.name},records={len(df)},{idx}/{len(files)}")
        yield file.name, df
        log.info(f"deleting file,file={file.name}")
        file.unlink()


def log_status(reader, input_cnt, msg):
    for idx, rec in enumerate(reader):
        if idx % 10000 == 0:
            pct = round((idx / input_cnt) * 100, 1)
            log.info(f"{msg},record={idx},records_in_file={input_cnt},pct={pct}%")
        yield rec


def mock_scores(choices):
    count = np.random.randint(5, 10)
    reader = zip(np.random.choice(choices, count, replace=False),
                 np.round(np.random.rand(count), 5))
    reader = filter(lambda x: x[0] not in {'', None}, reader)
    return sorted(reader, key=lambda x: -x[1])


def mock_profile(mpid, mpid_ts, all_genres, all_shows):
    return dict(mpid=mpid,
                genres=mock_scores(all_genres),
                shows=mock_scores(all_shows),
                event_start_unixtime_ms=mpid_ts,
                )


def mock_po1(msg, attr, items):
    try:
        items = ListAttributeValue(
            value=list(StringAttributeValue(value=value, weight=weight) for value, weight in items))

        event = EntityEvent(
            event=attr,
            entityId=str(msg['mpid']),
            entityType="nbc/Viewer",
            properties=items,
            meta={},
            eventTime=msg['event_start_unixtime_ms'],
        )
        return dict(event)
    except Exception as e:
        log.exception('problem building entity event')
        log.error(items)
        raise


def gen_profiles(week_id, file_name, df, profile_per_week_limit):
    log.info(f"start gen_profiles,file_name={file_name},records={len(df)}")

    all_genres = sorted(x for x in df.genre.unique() if x is not None)
    all_shows = sorted(x for x in df.show.unique() if x is not None)
    mpids = df.mpid
    mpids_ts = df.event_start_unixtime_ms.astype(np.int)

    reader = zip(mpids, mpids_ts)
    reader = take_if_limit(reader, limit=profile_per_week_limit, msg="PROFILE_PER_WEEK_LIMIT set")
    reader = log_status(reader, input_cnt=len(df), msg=f"week_id={week_id},file_name={file_name}")
    reader = (mock_profile(mpid=mpid,
                           mpid_ts=mpid_ts,
                           all_genres=all_genres,
                           all_shows=all_shows)
              for mpid, mpid_ts in reader)
    reader = ((msg, attr, items) for msg in reader for attr, items in msg.items() if
              attr not in {'mpid', 'event_start_unixtime_ms'})
    reader = starmap(mock_po1, reader)
    return reader


def write_dicts2json(inputs, outfile):
    reader = map(json.dumps, inputs)
    reader = map(lambda x: x + "\n", reader)

    with gzip.open(str(outfile), 'wt') as fh:
        fh.writelines(reader)
    log.info(f"wrote file={outfile}")


def upload_profiles(bucket, infile):
    name = infile.name
    key = f"nbc_profiles/{name}"
    log.info(f'start upload_profile,key={key}')
    bucket.upload_file(Filename=str(infile), Key=key)
    log.info(f'deleting profile file,infile={infile}')
    infile.unlink()
    log.info(f'end upload_profile,key={key}')


SKIP_WEEKS = {'2019W40'}


def proc_week(bucket, week_id, keys, work_d, config):
    if week_id in SKIP_WEEKS:
        return None

    merge_limit = config['MERGE_LIMIT']
    profile_per_week_limit = config['PROFILE_PER_WEEK_LIMIT']
    log.info(f"start proc_week,week_id={week_id},records={len(keys)}")

    week_d = work_d / week_id
    init_dir(week_d, exist_ok=True, rmtree=False)
    profile_f = week_d / f'nbc_profile_{week_id}.json.gz'

    # Processing
    download_files(bucket=bucket, keys=keys, outdir=week_d, merge_limit=merge_limit)
    reader = load_files(indir=week_d)
    reader = (gen_profiles(week_id=week_id, file_name=file_name, df=df, profile_per_week_limit=profile_per_week_limit)
              for file_name, df in reader)
    reader = concat(reader)
    write_dicts2json(inputs=reader, outfile=profile_f)
    retval(profile_f)
    upload_profiles(bucket=bucket, infile=profile_f)
    log.info(f"end proc_week,week_id={week_id}")
    return 'ok'


def main(config):
    bucket_key = config['GEN_PROFILE_SOURCE']
    work_d = Path(config['WORK_D'])
    infile = work_d / LIST_CLEAN_FILES_F

    log.info(f"start gen_profiles,bucket_key={bucket_key}")
    bucket = get_bucket(bucket_key)

    initialize(inbucket=bucket, work_d=work_d)

    files = pd.read_csv(infile)
    reader = iter(files.groupby('week').key)
    reader = sorted(reader, reverse=True)
    reader = (proc_week(bucket=bucket,
                        week_id=week_id,
                        keys=keys.tolist(),
                        work_d=work_d,
                        config=config)
              for week_id, keys in reader)

    # retval(first(reader))
    for x in reader: pass
    log.info(f"end gen_profiles")
