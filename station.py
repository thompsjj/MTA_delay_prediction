# -*- coding: utf-8 -*-
"""
Created on Tue Feb 24 08:23:34 2015

@author: Jared
"""

import numpy as np
from collections import defaultdict
from uniquelist import uniquelist


class Station(object):

    def __init__(self, station_id):
        self.station_id = station_id
        self.neighbor_stations = uniquelist()
        self.collecting = False
        self.in_collection = False
        self.schedule = None
        self.schedule_set = False

    def all_trains(self):

        pass



    def all_trains_today(self):


        pass


    def current_trains(self):
        '''This function returns a list of all trains that should be 
        stopping very soon at this station(at the query point)'''
        pass


    def set_schedule(self, schedule):

        self.schedule = schedule
        for day, schedule in self.schedule.iteritems():
            schedule = sorted(schedule) 

        self.schedule_set = True


    def __repr__(self):
        return "<Station:%s>" % self.station_id