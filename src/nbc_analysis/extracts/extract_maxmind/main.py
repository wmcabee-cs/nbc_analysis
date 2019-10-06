from nbc_analysis import get_config
from pathlib import Path

from zipfile import ZipFile
import pandas as pd
from io import BytesIO
from nbc_analysis.utils.debug_utils import retval

ZIP_F = 'GeoLite2-City-CSV_20191001.zip'
FILES_TO_EXTRACT = {
    'ip6': {'path': 'GeoLite2-City-CSV_20191001/GeoLite2-City-Blocks-IPv6.csv',
            'usecols': ['network', 'geoname_id', 'postal_code'],
            },
    'ip4': {'path': 'GeoLite2-City-CSV_20191001/GeoLite2-City-Blocks-IPv4.csv',
            'usecols': ['network', 'geoname_id', 'postal_code'],
            },
    'locs': {'path': 'GeoLite2-City-CSV_20191001/GeoLite2-City-Locations-en.csv',
             'usecols': ['geoname_id', 'country_iso_code', 'subdivision_1_iso_code', 'city_name', 'time_zone']
             },
}


# TODO: Change to pull raw dataset from an S3 bucket

def read_zip_file(zip, path, usecols, limit):
    return pd.read_csv(BytesIO(zip.read(path)), usecols=usecols, nrows=limit)


def read_ip_files(infile, limit=None):
    with ZipFile(infile, 'r') as zip:
        # zip.printdir()
        files = {name: read_zip_file(zip=zip, path=spec['path'], usecols=spec.get('usecols'), limit=limit)
                 for name, spec in FILES_TO_EXTRACT.items()}
        return files

        # IP4 = pd.read_csv(BytesIO(zip.read(ip4)))
        # IP6 = pd.read_csv(BytesIO(zip.read(ip6)))
        # LOCS = pd.read_csv(BytesIO(zip.read(locs)))
        # extracting all the files
        # print('Extracting all the files now...')
        # zip.extractall()


def main(config_f):
    config = get_config(config_f=config_f)
    datasets_d = Path(config['DATASETS_D'])
    infile = datasets_d / ZIP_F

    limit = None

    files = read_ip_files(infile, limit)

    locs = files['locs']
    locs = locs.query('country_iso_code == "US"')
    locs = locs.rename(columns={'subdivision_1_iso_code': 'state_iso_code'})
    files['locs'] = locs.sort_values('geoname_id')
    return files
