from typing import Optional
from pathlib import Path
import shutil
from os import PathLike

import pandas as pd
from nbc_analysis.utils.func_utils import take_if_limit

from nbc_analysis.utils.log_utils import get_logger

log = get_logger(__name__)


def init_dir(adir: PathLike,
             exist_ok: Optional[bool] = False,
             parents: Optional[bool] = False,
             rmtree: Optional[bool] = False) -> PathLike:
    adir = Path(adir)
    if adir.is_dir():
        if rmtree:
            shutil.rmtree(adir)
    adir.mkdir(exist_ok=exist_ok, parents=parents)
    return adir


def _standardize_name(name: str) -> str:
    if name.endswith('.parquet'):
        name = name.replace('.parquet', '')
    return name


def write_parquet(name: str,
                  df: pd.DataFrame,
                  outdir: PathLike) -> pd.DataFrame:
    name = _standardize_name(name)
    outfile = outdir / f'{name}.parquet'
    log.info(f"start write file,{outfile},record_cnt={len(df)}")
    df.to_parquet(outfile, index=False)
    log.info(f"end write file,{outfile},record_cnt={len(df)}")
    return df


def read_parquet(name: str,
                 indir:
                 PathLike, **kwargs) -> pd.DataFrame:
    name = _standardize_name(name)
    infile = indir / f'{name}.parquet'
    log.info(f"start read file,{infile}")
    df = pd.read_parquet(str(infile), **kwargs)
    log.info(f"end read file,{infile},record_cnt={len(df)}")
    return df


def read_parquet_dir(indir: PathLike,
                     limit: Optional[int] = None,
                     msg: Optional[str] = "file limit set") -> pd.DataFrame:
    reader = indir.glob('*')
    files = list(reader)
    log.info(f"start read parquet dir,file_cnt={len(files)},limit={limit}")
    reader = take_if_limit(files, limit=limit, msg=msg)
    df = pd.concat(map(pd.read_parquet, reader))
    log.info(f"end read parquet dir,record_cnt={len(df)}")
    return df
