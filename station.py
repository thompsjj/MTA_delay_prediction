# -*- coding: utf-8 -*-
"""
Created on Tue Feb 24 08:23:34 2015

@author: Jared
"""

import numpy as np
from collections import defaultdict
from uniquelist import uniquelist
from datetime_handlers import timedelta_from_timestring, \
timestamp_from_timept, set_ref_to_datetime, datetime_from_timept
from copy import copy
import sys, os

class Station(object):

    def __init__(self, station_id):
        self.station_id = station_id
        self.neighbor_stations = uniquelist()
        self.collecting = False
        self.in_collection = False
        self.schedule = defaultdict(list)
        self.schedule_set = False
        self.reference_date = None

    def set_schedule(self, ext_schedule, reference_date):
        self.reference_date = reference_date
        self.schedule = ext_schedule
        self.schedule_set = True

    def __repr__(self):
        return "<Station:%s>" % self.station_id



class MTAStation(Station):
    def __init__(self, station_id):
        super(MTAStation, self).__init__(station_id)
        self.station_id = station_id
        self.neighbor_stations = uniquelist()
        self.collecting = False
        self.in_collection = False
        self.schedule = defaultdict(list)
        self.schedule_set = False
        self.conf_schedule = defaultdict(lambda : defaultdict(lambda : defaultdict(list)))


    #this is MTA 26H specific and should be in a descendent class
    def _pass_schedule_to_timedelta(self, ext_schedule):

        for day, sched in ext_schedule.iteritems():
            for t, time in enumerate(sched):
                self.schedule[day].append(timedelta_from_timestring(time))

    def set_schedule(self, ext_schedule, reference_date):
        self.reference_date = reference_date
        self._pass_schedule_to_timedelta(ext_schedule)
        self._calculate_delay_conformal_mapping()
        self.schedule_set = True


    def _calculate_delay_conformal_mapping(self):

        hourtypes = ['00','01','02','03','04','05','06','07','08',\
        '09','10','11','12','13','14','15','16','17','18','19','20',\
        '21','22','23']

        sample_points = ['01','06','11','16','21','26','31','36','41',\
        '46','51','56']

        for day in ['WKD','SAT','SUN']:
            for hour in hourtypes:
                for minute in sample_points:   

                    rel_vector = []

                    for i, arr in enumerate(self.schedule[day]):

                        # set clock on sample date to relative hour, pt
                        # produce timedelta between reference date 
                        # midnight and sample time

                        present = \
                        set_ref_to_datetime(hour, minute, self.reference_date)

                        ref_timestamp = \
                        datetime_from_timept(self.reference_date)

                        rel_vector.append((arr+present)-ref_timestamp)

                    self.conf_schedule[day][hour][minute] = rel_vector



