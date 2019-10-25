from sqlalchemy import Table, Column, Integer, String, Float, DateTime
from sqlalchemy import MetaData

from nbc_analysis.utils.db_utils import init_db

SMALL_STRING = 10
MEDIUM_STRING = 20
LONG_STRING = 50
LONGER_STRING = 100
HUGE_STRING = 200

from nbc_analysis.utils.log_utils import get_logger

log = get_logger(__name__)


def get_metadata():
    metadata = MetaData()
    Table('DIM_HOUR_IN_WEEK', metadata,
          Column('hour_in_week_key', Integer, primary_key=True, nullable=False),
          Column('hour', Integer, nullable=False),
          Column('time_period', String(MEDIUM_STRING), nullable=False),
          Column('dayofweek_hiw', Integer, nullable=False),
          Column('day_name_hiw', String(SMALL_STRING), nullable=False),
          Column('week_day_hiw', String(SMALL_STRING), nullable=False),
          )

    Table('DIM_DAY', metadata,
          Column('day_key', Integer, primary_key=True, nullable=False),
          Column('ordinal', Integer, nullable=False),
          Column('day', Integer, nullable=False),
          Column('month', Integer, nullable=False),
          Column('year', Integer, nullable=False),
          Column('dayofweek', Integer, nullable=False),
          Column('day_name', String(SMALL_STRING), nullable=False),
          Column('week_day', String(SMALL_STRING), nullable=False),
          Column('month_name', String(SMALL_STRING), nullable=False),
          Column('days_in_month', Integer, nullable=False),
          Column('quarter', Integer, nullable=False),
          Column('dayofyear', Integer, nullable=False),
          Column('week', Integer, nullable=False),
          Column('weekday', Integer, nullable=False),
          Column('weekofyear', Integer, nullable=False),
          Column('week_id', String(SMALL_STRING), nullable=False),
          Column('is_leap_year', Integer, nullable=False),
          Column('is_week_start', Integer, nullable=False),
          Column('is_week_end', Integer, nullable=False),
          Column('is_month_start', Integer, nullable=False),
          Column('is_month_end', Integer, nullable=False),
          Column('is_quarter_start', Integer, nullable=False),
          Column('is_quarter_end', Integer, nullable=False),
          Column('is_year_start', Integer, nullable=False),
          Column('is_year_end', Integer, nullable=False),
          Column('start_time', String, nullable=False),
          Column('end_time', String, nullable=False),
          Column('start_uts_ms', Integer, nullable=False),
          Column('end_uts_ms', Integer, nullable=False),
          )

    Table('DIM_DAY_LOC', metadata,
          Column('day_key_loc', Integer, primary_key=True, nullable=False),
          Column('ordinal_loc', Integer, nullable=False),
          Column('day_loc', Integer, nullable=False),
          Column('month_loc', Integer, nullable=False),
          Column('year_loc', Integer, nullable=False),
          Column('dayofweek_loc', Integer, nullable=False),
          Column('day_name_loc', String(SMALL_STRING), nullable=False),
          Column('week_day_loc', String(SMALL_STRING), nullable=False),
          Column('month_name_loc', String(SMALL_STRING), nullable=False),
          Column('days_in_month_loc', Integer, nullable=False),
          Column('quarter_loc', Integer, nullable=False),
          Column('dayofyear_loc', Integer, nullable=False),
          Column('week_loc', Integer, nullable=False),
          Column('weekday_loc', Integer, nullable=False),
          Column('weekofyear_loc', Integer, nullable=False),
          Column('week_id_loc', String(SMALL_STRING), nullable=False),
          Column('is_leap_year_loc', Integer, nullable=False),
          Column('is_week_start_loc', Integer, nullable=False),
          Column('is_week_end_loc', Integer, nullable=False),
          Column('is_month_start_loc', Integer, nullable=False),
          Column('is_month_end_loc', Integer, nullable=False),
          Column('is_quarter_start_loc', Integer, nullable=False),
          Column('is_quarter_end_loc', Integer, nullable=False),
          Column('is_year_start_loc', Integer, nullable=False),
          Column('is_year_end_loc', Integer, nullable=False),
          Column('start_time_loc', String, nullable=False),
          Column('end_time_loc', String, nullable=False),
          Column('start_uts_ms_loc', Integer, nullable=False),
          Column('end_uts_ms_loc', Integer, nullable=False),
          )

    Table('DIM_PLATFORM', metadata,
          Column('platform_key', Integer, primary_key=True, nullable=False),
          Column('platform', String(SMALL_STRING), nullable=False),
          Column('data_connection_type', String(SMALL_STRING), nullable=False),
          Column('_last_upd_dt', Integer, nullable=False),
          )

    Table('DIM_END_TYPE', metadata,
          Column('end_type_key', Integer, primary_key=True, nullable=False),
          Column('video_end_type', String(SMALL_STRING), nullable=False),
          Column('resume', String(SMALL_STRING), nullable=False),
          Column('_last_upd_dt', Integer, nullable=False),
          )
    Table('DIM_EVENT_TYPE', metadata,
          Column('event_type_key', Integer, primary_key=True),
          Column('event_name', String(MEDIUM_STRING), nullable=False),
          Column('event_type', String(MEDIUM_STRING), nullable=False),
          Column('_last_upd_dt', Integer, nullable=False),
          )
    Table('DIM_PROFILE', metadata,
          Column('profile_key', Integer, primary_key=True, nullable=False),
          Column('mvpd', String(LONG_STRING), nullable=False),
          Column('nbc_profile', String(MEDIUM_STRING), nullable=False),
          Column('_last_upd_dt', Integer, nullable=False),
          )

    Table('DIM_VIDEO', metadata,
          Column('video_key', Integer, primary_key=True, nullable=False),
          Column('video_id', String(LONG_STRING), nullable=False),
          Column('video_type', String(MEDIUM_STRING), nullable=False),
          Column('show', String(LONGER_STRING), nullable=False),
          Column('season', String(SMALL_STRING), nullable=False),
          Column('episode_number', String(SMALL_STRING), nullable=False),
          Column('episode_title', String(HUGE_STRING), nullable=False),
          Column('genre', String(LONG_STRING), nullable=False),
          Column('video_duration', Float, nullable=True),
          Column('_last_upd_dt', Integer, nullable=False),
          )
    Table('F_VIDEO_END', metadata,
          Column('day_utc_key', Integer, nullable=False),
          Column('day_key_loc', Integer, nullable=True),
          Column('video_key', Integer, nullable=False),
          Column('platform_key', Integer, nullable=False),
          Column('profile_key', Integer, nullable=False),
          Column('end_type_key', Integer, nullable=False),
          Column('event_type_key', Integer, nullable=False),
          Column('network_key', Integer, nullable=True),
          Column('hour_of_week_key', Integer, nullable=True),
          Column('mpid', Integer, nullable=False),
          Column('event_start_dt', String(LONG_STRING), nullable=False),
          Column('event_start_local_dt', String(LONGER_STRING), nullable=True),
          Column('video_duration_watched', Float, nullable=True),
          Column('resume_time', Float, nullable=True),
          Column('_file', String(LONG_STRING), nullable=False),
          Column('_file_idx', Integer, nullable=False),
          Column('_batch_id', String(LONG_STRING), nullable=False),
          Column('_viewer_partition', Integer, nullable=False),
          Column('_last_upd_dt', Integer, nullable=False),
          )

    # TODO: Clean up null values in dim network
    Table('DIM_NETWORK', metadata,
          Column('network_key', Integer, primary_key=True, nullable=False),
          Column('ip_type', String(SMALL_STRING), nullable=False),
          Column('network', String(LONG_STRING), nullable=False),
          Column('geoname_id', Float, nullable=True),
          Column('postal_code', String(SMALL_STRING), nullable=True),
          Column('latitude', Float, nullable=True),
          Column('longitude', Float, nullable=True),
          Column('country_iso_code', String(SMALL_STRING), nullable=False),
          Column('country', String(MEDIUM_STRING), nullable=False),
          Column('state_iso_code', String(SMALL_STRING), nullable=False),
          Column('state', String(LONG_STRING), nullable=False),
          Column('city', String(LONGER_STRING), nullable=True),
          Column('time_zone', String(LONG_STRING), nullable=True),
          Column('geo_id', String(MEDIUM_STRING), nullable=True),
          Column('occup_housing_units', Float, nullable=True),
          Column('median_household_income', Float, nullable=True),
          Column('median_household_costs', Float, nullable=True),
          )

    return metadata


def initialize_db(cfg):
    metadata = get_metadata()
    engine = init_db(cfg=cfg, metadata=metadata, replace=True)
    return engine
