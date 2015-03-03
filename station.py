# -*- coding: utf-8 -*-
"""
Created on Tue Feb 24 08:23:34 2015

@author: Jared
"""

import numpy as np
from trip import trip
from datetime import datetime
from collections import defaultdict


class station(object):
    from sql_lib import get_next_MTA_train_from_schema
    #station object owns a stack of trips
    
    def __init__(self, station_id):
        self.station_id = station_id
        self.neighbor_stations = []
        self.collecting = False
        self.in_collection = False
        self.trip_stack = []
        
    def datetime_diff_to_timedelta(end_pt_timestruct, start_pt_datetime):
        # This is an interface function necessary for pSQL management
        return datetime(end_pt_timestruct.tm_year,\
                             end_pt_timestruct.tm_mon, \
                             end_pt_timestruct.tm_mday,\
                             end_pt_timestruct.tm_hour, \
                             end_pt_timestruct.tm_min,\
                             end_pt_timestruct.tm_sec)- \
                             start_pt_datetime
    

    