from datetime import datetime
from time import strptime

def datetime_diff_to_timedelta(end_pt_timestruct, start_pt_datetime):
        # This is an interface function necessary for pSQL management
    return datetime(end_pt_timestruct.tm_year,\
                             end_pt_timestruct.tm_mon, \
                             end_pt_timestruct.tm_mday,\
                             end_pt_timestruct.tm_hour, \
                             end_pt_timestruct.tm_min,\
                             end_pt_timestruct.tm_sec)- \
                             start_pt_datetime

def timestruct_from_timestring(self, string):
    '''infers 24H time from a time string'''
    return strptime(string, "%H:%M:%S")

def infer_datetime_from_time(self, timestruct, currentdatetime, timezone):
    