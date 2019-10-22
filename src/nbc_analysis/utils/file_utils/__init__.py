from pathlib import Path
import shutil
from os import PathLike

import pandas as pd
from nbc_analysis.utils.func_utils import take_if_limit

from nbc_analysis.utils.log_utils import get_logger

log = get_logger(__name__)


def init_dir(adir: PathLike, exist_ok=False, parents=False, rmtree=False):
    adir = Path(adir)
    if adir.is_dir():
        if rmtree:
            shutil.rmtree(adir)
    adir.mkdir(exist_ok=exist_ok, parents=parents)
    return adir


def write_parquet(name, df, outdir):
    outfile = outdir / f'{name}.parquet'
    log.info(f"start write file,{outfile},record_cnt={len(df)}")
    df.to_parquet(outfile, index=False)
    log.info(f"end write file,{outfile},record_cnt={len(df)}")
    return df


def read_parquet(name, indir, **kwargs):
    infile = indir / f'{name}.parquet'
    log.info(f"start read file,{infile}")
    df = pd.read_parquet(str(infile), **kwargs)
    log.info(f"end read file,{infile},record_cnt={len(df)}")
    return df


def read_parquet_dir(indir, limit=None, msg="file limit set"):
    reader = indir.glob('*')
    files = list(reader)
    log.info(f"start read parquet dir,file_cnt={len(files)}")
    reader = take_if_limit(files, limit=limit, msg=msg)
    df = pd.concat(map(pd.read_parquet, reader))
    log.info(f"end read parquet dir,record_cnt={len(df)}")
    return df
