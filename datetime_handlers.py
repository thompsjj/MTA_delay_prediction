from time import mktime, strptime, tzset
from pytz import timezone
from datetime import datetime, timedelta
from calendar import timegm
import os


def set_ref_to_datetime(hour, minute, reference_timept):

    cur = reference_timept.split('-')
    cur_yr = int(cur[0])
    cur_mon = int(cur[1])
    cur_mday = int(cur[2])
    cur_wday = int(cur[3]) #0 is monday
    cur_yday = int(cur[4])   
    cur_hour = int(hour)
    cur_min = int(minute)
    cur_sec = int(cur[7])

    return datetime(year=cur_yr, month=cur_mon, day=cur_mday, hour=cur_hour,\
     minute=cur_min, second=cur_sec)


def timestamp_from_timept(timept, timezone):

    #hacky
    os.environ['TZ'] = timezone
    tzset()
    datetime_obj = datetime.strptime(timept+'-01', '%Y-%m-%d-%H-%M-%S')

    return mktime(datetime_obj.timetuple())

def datetime_from_timept(timept):

    cur = timept.split('-')
    cur_yr = int(cur[0])
    cur_mon = int(cur[1])
    cur_mday = int(cur[2])
    cur_wday = int(cur[3]) #0 is monday
    cur_yday = int(cur[4])   
    cur_hour = int(cur[5])
    cur_min = int(cur[6])
    cur_sec = int(cur[7])

    return datetime(year=cur_yr, month=cur_mon, day=cur_mday, hour=cur_hour,\
     minute=cur_min, second=cur_sec)

def timestamp_from_refdt(hour, minute, reference_dt, timezone):
    os.environ['TZ'] = timezone
    tzset()
    cur_yr = int(reference_dt.year)
    cur_mon = int(reference_dt.month)
    cur_mday = int(reference_dt.day)
    cur_hour = int(hour)
    cur_min = int(minute)
    cur_sec = int(0)

    datetime_obj = datetime(ear=cur_yr, month=cur_mon, day=cur_mday, \
    hour=cur_hour, minute=cur_min)

    return mktime(datetime_obj.timetuple())


def trip_day_from_timestamp(timestamp):

    daydict = {0:'WKD',1:'WKD',2:'WKD',3:'WKD',4:'WKD',5:'SAT',6:'SUN'}
    return daydict[datetime.fromtimestamp(timestamp).weekday()]


def datetime_diff_to_timedelta(end_pt_timestruct, start_pt_datetime):

    return datetime(end_pt_timestruct.tm_year,\
                             end_pt_timestruct.tm_mon, \
                             end_pt_timestruct.tm_mday,\
                             end_pt_timestruct.tm_hour, \
                             end_pt_timestruct.tm_min,\
                             end_pt_timestruct.tm_sec)- \
                             start_pt_datetime

def timestruct_from_timestring(string):

    '''infers 24H time from a time string'''
    print 'timestruct from string'
    return strptime(string, "%H:%M:%S")


def timedelta_from_timestring(string):

    '''infers a timedelta from a timestring'''
    l = string.split(':')
    try:
        return timedelta(hours=int(l[0]),minutes=int(l[1]), seconds=int(l[2]))

    except Exception, e:
        print e


def infer_datetime_from_timestruct(inftimestruct, currenttimestruct):
    print 'infer datetime'
    cur_yr = currenttimestruct.tm_year
    cur_mon = currenttimestruct.tm_mon
    cur_mday = currenttimestruct.tm_mday
    cur_wday = currenttimestruct.tm_wday #0 is monday
    cur_yday = currenttimestruct.tm_yday

    inf_hour = inftimestruct.tm_hour
    inf_min = inftimestruct.tm_min
    inf_sec = inftimestruct.tm_sec

    return datetime(cur_yr, cur_mon, cur_mday, inf_hour,\
     inf_min, inf_sec, cur_wday, cur_yday)