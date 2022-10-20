import datetime
from calendar import TUESDAY

import pandas as pd
from dateutil.relativedelta import relativedelta, TU


def get_last_tuesday_of_last_month():
    today = datetime.datetime.today()
    last_tuesday = get_last_tuesday_of_the_month(today.year, today.month - 1)
    return pd.to_datetime(last_tuesday.date())


def get_last_tuesday_of_this_month():
    today = datetime.datetime.today()
    last_tuesday = get_last_tuesday_of_the_month(today.year, today.month)
    return pd.to_datetime(last_tuesday.date())


def get_last_tuesday_of_the_month(year, month):
    return pd.to_datetime((datetime.datetime(year, month, 1) + relativedelta(day=31, weekday=TU(-1))),
                          format=DATE_FORMAT)


def get_last_tuesday():
    today = datetime.date.today()
    return get_previous_tuesday(today)


def get_previous_tuesday(day):
    offset = (day.weekday() - TUESDAY) % 7
    return day - datetime.timedelta(days=offset)


def get_tuesday_of_last_week_before_date(day):
    if day.weekday() <= TUESDAY:
        result = get_previous_tuesday(day - datetime.timedelta(days=1))
    else:
        result = get_previous_tuesday(get_previous_tuesday(day) - datetime.timedelta(days=1))
    return result


def get_tuesday_of_last_week():
    return get_tuesday_of_last_week_before_date(datetime.date.today())


DATE_FORMAT = '%d/%m/%Y'
