# -*- coding: utf-8 -*-
import pytest
from pathlib import Path

from nbc_analysis.main import main

__author__ = "William McAbee"
__copyright__ = "William McAbee"
__license__ = "mit"

INDIR = Path('/Users/wmcabee/_NBC/example_files')
OUTDIR = Path('/Users/wmcabee/DATA/nbc_analysis')


def test_main():
    OUTDIR.mkdir(exist_ok=True)
    return main(indir=INDIR, outdir=OUTDIR)
