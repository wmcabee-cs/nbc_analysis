from pathlib import Path
import shutil
from os import PathLike

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
