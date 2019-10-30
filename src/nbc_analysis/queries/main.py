from nbc_analysis.utils.debug_utils import retval
from nbc_analysis.utils.db_utils import get_db
from nbc_analysis.utils.file_utils import init_dir
from pathlib import Path
from toolz import groupby, pluck

import pandas as pd


def get_shows(engine, category, mpid):
    sql = f"""
    select {category}, count(1) cnt
from F_VIDEO_END f
join DIM_VIDEO vid on f.video_key = vid.video_key
where 1=1
and f.mpid = :mpid
group by {category}
order by cnt desc

limit 10
    """

    df = pd.read_sql(sql=sql, con=engine, params={'mpid': mpid})
    df['likelihood'] = df.cnt.div(df.cnt.sum()).round(5)
    df = df.drop(columns='cnt')
    obj = df.to_dict(orient='records')
    obj = list(pluck([category, 'likelihood'], obj))
    return obj


def flatten_dict(obj, category):
    reader = obj.items()
    reader = ((tp, cat, v) for (tp, cat), v in reader)
    obj = {k: {k2: v2[0][2]
               for k2, v2 in groupby(1, v).items()}
           for k, v in groupby(0, reader).items()}
    return obj


def get_show_by_time_period(engine, category, mpid):
    sql = f"""
        select time_period, {category} , count(1) cnt
    from F_VIDEO_END f
    join DIM_VIDEO vid on f.video_key = vid.video_key
    join DIM_HOUR_IN_WEEK how on f.hour_in_week_key = how.hour_in_week_key
    where 1=1
    and f.mpid = :mpid
    group by time_period, {category}
    order by cnt desc
        """

    df = pd.read_sql(sql=sql, con=engine, params={'mpid': mpid})
    dx = (pd.crosstab(df.time_period, df[category],
                      values=df.cnt, aggfunc=sum, normalize='index')
          .round(5)
          .fillna(''))
    dx = dx.mask(dx < .001).stack()  # .to_frame('likelihood')
    obj = dx.to_dict()
    obj = flatten_dict(obj, category=category)
    return obj


def get_category_info(engine, mpid, categories):
    sql = f"""
        select {categories} , 
        sum(1) video_end_event_cnt, 
        sum(video_duration_watched) video_duration_watched
    from F_VIDEO_END f
    join DIM_NETWORK net on f.network_key = net.network_key
    join DIM_PLATFORM net on f.platform_key = net.platform_key
    where 1=1
    and f.mpid = :mpid

    group by {categories}
    order by video_end_event_cnt desc
    limit  10
        """

    df = pd.read_sql(sql=sql, con=engine, params={'mpid': mpid})  # .sort_values('mpid')
    obj = df.to_dict(orient='records')
    return obj


def main(config, mpid):
    cfg = config['database']
    engine = get_db(cfg=cfg)

    ip_categories = 'postal_code, state, city, median_household_income, median_household_costs'
    platform_categories = 'platform, data_connection_type'
    msg = dict(
        mpid=mpid,
        ip_info=get_category_info(engine, mpid=mpid, categories=ip_categories),
        platform_info=get_category_info(engine, mpid=mpid, categories=platform_categories),
        shows=get_shows(engine, category='show', mpid=mpid),
        genres=get_shows(engine, category='genre', mpid=mpid),
        shows_by_timeperiod=get_show_by_time_period(engine, category='show', mpid=mpid),
        genres_by_timeperiod=get_show_by_time_period(engine, category='genre', mpid=mpid),
    )
    return msg, engine


def query_detail(config, mpid):
    cfg = config['database']
    engine = get_db(cfg=cfg)

    sql = """
    select 
	mpid,
		f.event_start_dt, 
		evt.event_name,
		endt.video_end_type, 
		plat.platform, plat.data_connection_type,
		prof.mvpd, prof.nbc_profile,
	    f.video_duration_watched,
		f.resume_time,
		endt.resume,
		f.day_utc_key,
	    vid.video_id, vid.video_type, vid.show, vid.season, vid.episode_number, vid.episode_title, vid.genre,
		net.network, postal_code, latitude, longitude, country, state, city, occup_housing_units, median_household_income, median_household_costs, time_zone, 
		f.event_start_local_dt,
	    how.hour, time_period, how.hour_in_week_key,
		f.day_key_loc, day_loc.day_name_loc, day_loc.week_day_loc, day_loc.month_name_loc, day_loc.week_id_loc,
	    f._file,
		f._file_idx
    from F_VIDEO_END f
    join DIM_VIDEO vid on f.video_key = vid.video_key
    join DIM_HOUR_IN_WEEK how on f.hour_in_week_key = how.hour_in_week_key
	join DIM_DAY day_utc on f.day_utc_key = day_utc.day_key
	join DIM_DAY_LOC day_loc on f.day_key_loc = day_loc.day_key_loc
	join DIM_EVENT_TYPE evt on f.event_type_key = evt.event_type_key
	join DIM_NETWORK net on f.network_key = net.network_key
	join DIM_PLATFORM plat on  f.platform_key = plat.platform_key
	join DIM_PROFILE prof on f.profile_key = prof.profile_key
	join DIM_END_TYPE endt on f.end_type_key = endt.end_type_key
    where 1=1
    and f.mpid = :mpid
	order by f._last_upd_dt desc
    """
    df = pd.read_sql(sql=sql, con=engine, params={'mpid': mpid})
    return df, engine
