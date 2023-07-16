import pandas as pd
import calendar
import datetime
import pickle
import pytz


def findDaysInBetween(weeks) -> int:
    """
    # find days between current date and last friday
    :param weeks: # of weeks you want to look back
    :return: # of days in between
    """
    eastern = pytz.timezone('US/Eastern')
    today_weekday = datetime.datetime.now(eastern).weekday()
    time_distance = {0: 3, 1: 4, 2: 5, 3: 6, 4: 7, 5: 1, 6: 2}
    days = (weeks - 1) * 7 + time_distance[today_weekday]
    return days


def findFridays(weeks) -> str:
    # get current date
    eastern = pytz.timezone('US/Eastern')
    today_date = datetime.datetime.now(eastern)
    days_in_between = datetime.timedelta(days=findDaysInBetween(weeks))
#    while today_date.weekday() != calendar.FRIDAY:
    today_date -= days_in_between
    friday = today_date.strftime('%Y%m%d')
    return friday


def retractDays(org_dt: str, days: int) -> object:
    org_dt_obj = datetime.datetime.strptime(org_dt, '%Y%m%d')
    retracted_obj = org_dt_obj - datetime.timedelta(days=days)
    return_dt = retracted_obj.strftime('%Y%m%d')
    return return_dt

def findTodayDate() -> str:
  eastern = pytz.timezone('US/Eastern')
  today_date = datetime.datetime.now(eastern)
  return today_date.strftime('%m/%d/%Y')
  