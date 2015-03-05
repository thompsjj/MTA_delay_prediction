# -*- coding: utf-8 -*-
"""
Created on Tue Feb 24 08:23:34 2015

@author: Jared
"""

import numpy as np
from collections import defaultdict
from uniquelist import uniquelist
from datetime_handlers import timedelta_from_timestring
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
        #self.schedule = None




    def all_trains_today(self):


        pass


    def current(self, now_timestamp, within_minutes):

        '''This function returns a list of all trains that should be 
        stopping very soon at this station(at the query point)'''
        pass


    def set_schedule(self, ext_schedule):
       self._pass_schedule_to_timedelta(ext_schedule)
       self.schedule_set = True

    #this is MTA 26H specific and should be in a descendent class
    def _pass_schedule_to_timedelta(self, ext_schedule):
        for day, sched in ext_schedule.iteritems():
            for t, time in enumerate(sched):
                self.schedule[day].append(timedelta_from_timestring(time))

    def __repr__(self):
        return "<Station:%s>" % self.station_id