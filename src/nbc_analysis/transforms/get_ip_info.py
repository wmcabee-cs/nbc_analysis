import geoip2.database
from geoip2.errors import AddressNotFoundError

from nbc_analysis import get_config
from pathlib import Path
from collections import namedtuple
from nbc_analysis.utils.debug_utils import retval, StopEarlyException

IPInfo = namedtuple('IPInfo',
                    'ip geoname_id postal_code time_zone city state_iso_code state country_iso_code country_name')


def init_ip_db(config_f):
    config = get_config(config_f=config_f)
    infile = Path(config['GEOLITE2_DB'])
    return geoip2.database.Reader(infile)


def main(ips_db, ips):
    def func(ip):
        try:
            response = None
            try:
                response = ips_db.city(ip)
            except AddressNotFoundError as e:
                pass

            if response is None or response.city.geoname_id is None:
                return IPInfo(ip=ip,
                              geoname_id=0,
                              postal_code=None, time_zone=None,
                              city=None,
                              state_iso_code=None,
                              state=None,
                              country_iso_code=None,
                              country_name=None,
                              )

            subdivisions = response.subdivisions
            if len(subdivisions) > 0:
                state_iso_code = subdivisions[0].iso_code
                state = subdivisions[0].name
            else:
                state_iso_code = None
                state = None

            return IPInfo(ip=ip,
                          geoname_id=response.city.geoname_id,
                          postal_code=response.postal.code,
                          time_zone=response.location.time_zone,
                          city=response.city.name,
                          state_iso_code=state_iso_code,
                          state=state,
                          country_iso_code=response.country.iso_code,
                          country_name=response.country.name,

                          )
        except StopEarlyException as e:
            raise e
        except Exception as e:
            print(f">> ERROR: Problem parsing IP {ip}, {e}")
            raise

    return map(func, ips)
