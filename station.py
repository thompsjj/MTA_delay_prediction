# -*- coding: utf-8 -*-
"""
Created on Tue Feb 24 08:23:34 2015

@author: Jared
"""

import numpy as np
from collections import defaultdict
from uniquelist import uniquelist
from datetime_handlers import timedelta_from_timestring, \
timestamp_from_timept
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

    def set_schedule(self, ext_schedule):
       self._pass_schedule_to_timedelta(ext_schedule)
       self._calculate_delay_conformal_mapping()
       self.schedule_set = True


    def __repr__(self):
        return "<Station:%s>" % self.station_id


class MTAStation(Station):

    def __init__(self, station_id):
        super(Station, self).__init__()
        self.station_id = station_id
        self.neighbor_stations = uniquelist()
        self.collecting = False
        self.in_collection = False
        self.schedule = defaultdict(list)
        self.schedule_set = False


    #this is MTA 26H specific and should be in a descendent class
    def _pass_schedule_to_timedelta(self, ext_schedule):

        for day, sched in ext_schedule.iteritems():
            for t, time in enumerate(sched):
                self.schedule[day].append(timedelta_from_timestring(time))


    def _calculate_delay_conformal_mapping(self):

        hourtypes = ['00','01','02','03','04','05','06','07','08',\
        '09','10','11','12','13','14','15','16','17','18','19','20',\
        '21','22','23']

        sample_points = ['01','06','11','16','21','26','31','36','41',\
        '46','51','56']


        #testing here


        self.conf_mapping = defaultdict(lambda : defaultdict(list))
        for hour in hourtypes:
            for point in sample_points:   
                timept = '2001-01-01-%s-%s' % (hour, point)
                sample_timestamp = timestamp_from_timept(timept, 'US/Eastern')

                rel_vector = []

                for i, arr in enumerate(self.schedule['WKD']):
                    rel_vector.append(arr-sample_timestamp)
                    #for tmpt in self.schedule[day]:
                self.conf_mapping[hour][point] = rel_vector


        print self.conf_mapping['08']['46']



    '''for day in ['WKD','SAT','SUN']:
            for hour in hourtypes:
                for point in sample_points:
                    timept = '1900-01-01-%s-%s' % (hour, point)
                    sample_timestamp = timestamp_from_timept(timept)

                    # For each point in the schedule for that day, calculate
                    # the timedelta 
                    # difference between it and the train based on the 1900 
                    # basis


                    for tmpt in self.schedule[day]:



                    # this makes a vector of 1Xtimestamps for each train.


                    # the schedule is then represented by a marked matrix of 
                    # timestamps 




                    # calculate the relationship between two trains based on the
                    # time point stamp and each other get a time delta



                    # now calculate difference matrix between each train and 
                    # timestamp



                    # the actual delays are now vector dot products 

                    # build difference vector between the present timestamp and 
                    # every train in the schedule'''
















