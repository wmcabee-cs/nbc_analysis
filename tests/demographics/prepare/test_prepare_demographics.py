import pytest
from nbc_analysis.demographics import prepare_demographics
from nbc_analysis.utils.debug_utils import retval
from nbc_analysis.utils.toml_utils import get_config

TEST_CONFIG = """

[demographics] 
demographics_d = '{NBC_DATA_TOP}/demographics'

# input files
zip2income_input_f = '{NBC_PROJ_TOP}/datasets/ACS_17_5YR_S2503_with_ann.csv'
subnet2zip_input_f = '{NBC_PROJ_TOP}/datasets/GeoLite2-City-CSV_20191001.zip'

# output files
subnet2inc4_filename = 'subnet2inc4.parquet'
subnet2inc6_filename = 'subnet2inc6.parquet'


####################################
# For development 
####################################
#record_limit=2000 # Load only first N records of zipcode dataset. For development
"""


def _get_config():
    return get_config(config_text=TEST_CONFIG)


def test_prepare_demographics():
    config = _get_config()
    return prepare_demographics(config=config)
