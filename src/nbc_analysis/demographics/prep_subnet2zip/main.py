from nbc_analysis.utils.debug_utils import retval
from nbc_analysis.utils.log_utils import get_logger
from nbc_analysis.utils.file_utils import init_dir

from zipfile import ZipFile
from io import BytesIO

from toolz import first, concatv
import pandas as pd
import numpy as np

log = get_logger(__name__)


def load_datasets(infile):
    log.info(f"start load geolite2 city datasets,infile={infile}")

    # importing required modules
    ip6_f = 'GeoLite2-City-CSV_20191001/GeoLite2-City-Blocks-IPv6.csv'
    ip4_f = 'GeoLite2-City-CSV_20191001/GeoLite2-City-Blocks-IPv4.csv'
    locs_f = 'GeoLite2-City-CSV_20191001/GeoLite2-City-Locations-en.csv'
    # opening the zip file in READ mode
    with ZipFile(infile, 'r') as zip:
        # printing all the contents of the zip file
        ip4 = pd.read_csv(BytesIO(zip.read(ip4_f)))
        ip6 = pd.read_csv(BytesIO(zip.read(ip6_f)))
        locs = pd.read_csv(BytesIO(zip.read(locs_f)))
    # filter to only US only
    keep_fields = ['geoname_id', 'country_iso_code', 'country_name', 'subdivision_1_iso_code', 'subdivision_1_name',
                   'city_name', 'time_zone']
    locs = locs[locs.country_iso_code == 'US'][keep_fields].copy()
    locs.rename(columns={'country_name': 'country',
                         'subdivision_1_iso_code': 'state_iso_code', 'subdivision_1_name': 'state',
                         'city_name': 'city'}, inplace=True)

    keep_fields = ['network', 'geoname_id', 'postal_code', 'latitude', 'longitude']
    ip6 = ip6[ip6.geoname_id.isin(locs.geoname_id)][keep_fields].copy()
    ip4 = ip4[ip4.geoname_id.isin(locs.geoname_id)][keep_fields].copy()

    assert ip6.network.is_unique, "primary key violation, ip6"
    assert ip4.network.is_unique, "primary key violation, ip4"
    assert locs.geoname_id.is_unique, "primary key violation, locs"

    # return datasets
    datasets = dict(ip6=ip6, ip4=ip4, locs=locs)
    log.info("end load geolite2 city datasets")
    return datasets


def main(cfg):
    log.info(f"start prepare subnet2zip files")
    infile = cfg['subnet2zip_input_f']

    # load datasets
    datasets = load_datasets(infile)
    locs = datasets['locs']
    ip6 = datasets['ip6']
    ip4 = datasets['ip4']

    # adorn ip6
    ip6 = ip6[ip6.geoname_id != 6252001]
    ip6 = ip6.merge(locs, on='geoname_id', how='left')
    log.info(f"end ip6 merge")

    # adorn ip4
    ip4 = ip4[ip4.geoname_id != 6252001]
    ip4 = ip4.merge(locs, on='geoname_id', how='left')
    log.info(f"end ip4 merge")

    log.info(f"end prepare subnet2zip files,ip6={len(ip6)},ip4={len(ip4)}")
    return ip4, ip6
