from pathlib import Path
import shutil
from os import PathLike


def init_dir(adir: PathLike, exist_ok=False, parents=False, rmtree=False):
    adir = Path(adir)
    if adir.is_dir():
        if rmtree:
            shutil.rmtree(adir)
    adir.mkdir(exist_ok=exist_ok, parents=parents)
    return adir
