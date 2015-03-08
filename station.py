# -*- coding: utf-8 -*-
"""
Created on Tue Feb 24 08:23:34 2015

@author: Jared
"""

import numpy as np
from collections import defaultdict
from uniquelist import uniquelist
from itertools import cycle
from datetime_handlers import timedelta_from_timestring, \
timestamp_from_timept, set_ref_to_datetime, datetime_from_timept \
timestamp_from_refdt
from copy import copy
import sys, os

from datetime import date
from dateutil.rrule import rrule, DAILY


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
        self.conf_schedule = \
        defaultdict(lambda : defaultdict(lambda : defaultdict()))
        self.historical_schedule = \
                defaultdict(lambda : defaultdict(lambda : defaultdict()))      



        self.days = ['MON','TUE','WED', 'THU', 'FRI', 'SAT','SUN']

        self.hourtypes = ['00','01','02','03','04','05','06','07','08',\
        '09','10','11','12','13','14','15','16','17','18','19','20',\
        '21','22','23']

        self.sample_points = ['01','06','11','16','21','26','31','36','41',\
        '46','51','56']


    #this is MTA 26H specific and should be in a descendent class
    def _pass_schedule_to_timedelta(self, ext_schedule):

        for day, sched in ext_schedule.iteritems():

            for t, time in enumerate(sched):

                self.schedule[day].append(timedelta_from_timestring(time))

            self.schedule[day] = np.asarray(sorted(self.schedule[day]))



    def set_schedule(self, ext_schedule, reference_date):
        self.reference_date = reference_date
        self._pass_schedule_to_timedelta(ext_schedule)
        self._calculate_conformal_schedule()
        self.schedule_set = True


    def _calculate_conformal_schedule(self):
    # using queries, compute delays and map to stations. Thus we have a station-
    # delay dataset where delays are calculated 12 times an hour using delay-
    # frequency paradigm the station owns a histogram of delays
    # this is as opposed to actual arrival - schedule paradigm which would be
    # computed from a dynamic database.
        def next_day(day, days):
            return days[(days.index(day)+1)%len(days)]

        for day in self.days:
            schedule_today = self.schedule[day]
            for hour in self.hourtypes:
                for minute in self.sample_points:

                    # take a zip between actual time and difference between
                    # this and the current time point

                    current_sample_pt = \
                    timedelta_from_timestring(hour+':'+minute+':00')

                    # calculate point greater than zero but closest to 0
                    check_pt = \
                    [z for z in zip((schedule_today-current_sample_pt),\
                     schedule_today)\
                      if z[0] > timedelta_from_timestring('00:00:00')]

                    if len(check_pt) > 0:
                        closest_train = min(check_pt, key=lambda x: x[0])[0]
                    else:
                        closest_train = 'next'

                    # the conformal time is this minumum distance to the point
                    self.conf_schedule[day][hour][minute] = closest_train


        # we resolve the 'next' flags here.
        for d, day in enumerate(self.days):
              for h, hour in enumerate(self.hourtypes):
                for m, minute in enumerate(self.sample_points):
                        if self.conf_schedule[day][hour][minute] == 'next':
                            self.conf_schedule[day][hour][minute] = \
                            self.conf_schedule[next_day(day, self.days)]\
                            [self.hourtypes[0]][self.sample_points[0]]\
                            +timedelta_from_timestring('00:05:00')

    del sample_history_from_db(cursor, self, start_date, end_date):

        for dt in rrule(DAILY, dtstart=a, until=b):
            print 'station %s accessing: %s' % \
            (self.station_id, dt.strftime("%Y-%m-%d"))

            day = self.days[dt.weekday()]
            
            for hour in self.hourtypes:
                for minute in self.sample_points:
                    current_tstamp = timestamp_from_refdt(hour, minute, dt)

                    query_mta_historical_closest_train(cursor, self.station_id,\
                        current_tstamp)



                    #self.historical_schedule[day][hour][minute]





