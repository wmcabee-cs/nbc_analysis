from nbc_analysis.utils.date_utils import get_now_zulu, get_today, check_day_key
from nbc_analysis.utils.file_utils import init_dir
from nbc_analysis.utils.config_utils import get_config, write_config_yaml
from pathlib import Path
from nbc_analysis.utils.debug_utils import retval
import arrow
import pandas as pd
import re

RUN_SPEC = {'START_DAY': None, 'WEEKS_BACK': 5, }


def get_run_id(run_id):
    if run_id is not None:
        return run_id
    return f"r_{get_now_zulu()}"


def get_run_d(config, runs_d, run_id):
    if runs_d is None:
        runs_d = config['RUNS_D']
    runs_d = Path(runs_d)
    outdir = runs_d / run_id
    return outdir


def get_start_day(start_day, calendar):
    if start_day is None:
        start_day = get_today()
    if not check_day_key(start_day):
        raise ValueError(f">> Error,expecting day format 'YYYYMMDD',start_day='{start_day}'")

    start_day = int(start_day)
    start_day = calendar.loc[start_day]
    return start_day


def get_days_in_run(*, weeks=1, start_day, calendar):
    # Calculate end of period range
    end_ordinal = start_day.ordinal - (7 * (int(weeks) - 1))
    offset = calendar[calendar.ordinal == end_ordinal].iloc[0]
    mask = ((calendar.start_uts_ms <= offset.start_uts_ms)
            & (calendar.is_week_start == 1)
            & (calendar.ordinal > offset.ordinal - 7)
            )
    end_day = calendar[mask].iloc[0]
    df = calendar.loc[end_day.name: start_day.name]
    days_in_run = df.reset_index()[['day_key', 'week_id', 'ordinal']]
    return days_in_run


def get_calendar(config):
    calendar_d = Path(config['CALENDAR_D'])
    calendar_f = calendar_d / 'cal_days.parquet'
    if not calendar_f.is_file():
        raise Exception(f"calendar files missing {calendar_f}. Run create calendar first.")
    calendar = pd.read_parquet(calendar_f)
    calendar = calendar.set_index('day_key').sort_index()
    return calendar


def get_weeks_in_run(days_in_run):
    df = days_in_run.groupby('week_id').day_key.agg(['size', 'min', 'max'])
    df = df.reset_index()
    return df


def get_runs_d(config, runs_d):
    if runs_d is None:
        runs_d = config['RUNS_D']
    runs_d = Path(runs_d)
    return runs_d


def main(*, config, start_day=None, weeks=1, run_id=None, runs_d=None, exist_ok=False, rmtree=False):
    print(f">> start run init,run_id={run_id},start_day={start_day},weeks={weeks},runs_d={runs_d}")

    # load calendar
    config = get_config(config=config)
    calendar = get_calendar(config)

    run_id = get_run_id(run_id)
    runs_d = get_runs_d(config=config, runs_d=runs_d)
    init_dir(runs_d, exist_ok=True)
    run_d = get_run_d(config, runs_d=runs_d, run_id=run_id)
    start_day = get_start_day(start_day, calendar)

    # initialize directory for run
    print(f">> initializing run directory,run_d='{run_d},exist_ok={exist_ok},rmtree={rmtree}")
    init_dir(run_d, exist_ok=exist_ok, rmtree=exist_ok)

    # Calculated days in run
    days = get_days_in_run(weeks=weeks, start_day=start_day, calendar=calendar)
    outfile = run_d / 'days.csv'
    days.to_csv(outfile, index=False)
    print(f">> wrote {outfile},records={len(days)}")

    # Calculate weeks in run
    weeks = get_weeks_in_run(days_in_run=days)
    outfile = run_d / 'weeks.csv'
    weeks.to_csv(outfile, index=False)
    print(f">> wrote {outfile},records={len(weeks)}")

    # write configuration information to run directory
    outfile = run_d / "run_config.yaml"
    run_config = {'RUN_ID': run_id}
    write_config_yaml(outfile=outfile, config=run_config)
    print(f">> wrote {outfile}")
    print(">> end run init")

    return run_d
