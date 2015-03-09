# -*- coding: utf-8 -*-
"""
Created on Tue Feb 24 08:23:34 2015

@author: Jared
"""

import numpy as np
from collections import defaultdict
from uniquelist import uniquelist
import itertools
from itertools import cycle
from datetime_handlers import timedelta_from_timestring, \
timestamp_from_timept, set_ref_to_datetime, datetime_from_timept, \
timestamp_from_refdt
from copy import copy
import sys, os

from datetime import date
from dateutil.rrule import rrule, DAILY

import time

from mta_database_handlers import query_mta_historical_closest_train_between
from multiprocessing import Pool, cpu_count
from sql_interface import sample_local_db_dict_cursor
import psycopg2, threading
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

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
    from mta_database_handlers import query_mta_historical_closest_train
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
                defaultdict(lambda : defaultdict(lambda : defaultdict(list)))      



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
                    check_pt = [z for z in zip((schedule_today-current_sample_pt), schedule_today) if z[0] > timedelta_from_timestring('00:00:00')]


                    if len(check_pt) > 0:
                        closest_train = min(check_pt, key=lambda x: x[0])[0]
                    
                    elif (hour == self.hourtypes[-1]):
                        closest_train = 'next'
                    else:
                        print '_calculate_conformal_schedule \n: \
                        a major error occurred while setting \n \
                        the schedule for station %s' % (self.station_id)
                        print "%s %s" % (hour, minute)

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



    def sample_history_from_db(self, cursor, startdate, enddate):
        from mta_database_handlers import query_mta_historical_closest_train_between
        from multiprocessing import Pool, freeze_support


        s = startdate.split('-')
        startyr = int(s[0])
        startmo = int(s[1])
        startday = int(s[2])

        e = enddate.split('-')
        endyr = int(e[0])
        endmo = int(e[1])
        endday = int(e[2])

        a = date(startyr, startmo, startday)
        b = date(endyr, endmo, endday)

        max_tstamp = timestamp_from_refdt(self.hourtypes[-1],self.sample_points[-1],b,'US/Eastern')+3600

        interval = []
        #unroll inner loop
        for hour in self.hourtypes:
            for minute in self.sample_points:
                interval.append((hour, minute))

        start_outer = time.clock()
        for dt in rrule(DAILY, dtstart=a, until=b):
            (self.station_id, dt.strftime("%Y-%m-%d"))

            day = self.days[dt.weekday()]
         
            for interv in interval:

                    #response = query_mta_historical_closest_train(cursor,'mta_historical_small', self.station_id, current_tstamp, 120)

                current_tstamp = timestamp_from_refdt(interv[0], interv[1], dt,'US/Eastern')+3600
                response = query_mta_historical_closest_train_between(cursor,'mta_historical_small', self.station_id, current_tstamp, max_tstamp)


                self.historical_schedule[day][hour][minute].append(response)

                    #print "request time: %s"  % (time.clock() - start_inner_3)

               #if response:
                #    self.historical_schedule[day][hour][minute].append(response)
                        #print "station: %s response: %s tstamp: %s"  % (self.station_id, response, current_tstamp)
                #else:
                 #   self.historical_schedule[day][hour][minute].append(0)'''


                #print "minute loop: %s" % (time.clock() - start_inner_2)

            #print "hour loop: %s" % (time.clock() - start_inner_2)

        print "full loop: %s station: %s" % ((time.clock() - start_outer), (self.station_id))

            #now the schedule is set for seconds past the timestamp

    def sample_history_from_db_threaded(self, startdate, enddate, database, \
        table_name, user, host, password):


        s = startdate.split('-')
        startyr = int(s[0])
        startmo = int(s[1])
        startday = int(s[2])

        e = enddate.split('-')
        endyr = int(e[0])
        endmo = int(e[1])
        endday = int(e[2])

        a = date(startyr, startmo, startday)
        b = date(endyr, endmo, endday)

        max_tstamp = timestamp_from_refdt(self.hourtypes[-1],self.sample_points[-1],b,'US/Eastern')+3600

        sample_tstamp = []

        for dt in rrule(DAILY, dtstart=a, until=b):
            for hour in self.hourtypes:
                for minute in self.sample_points:
                    sample_tstamp.append(timestamp_from_refdt(hour, minute, dt,'US/Eastern')+3600)
                
        def select_func(conn_or_pool, table_name, station_id, start_tstamp, stop_tstamp):
            name = threading.currentThread().getName()
            
            for i in range(SELECT_SIZE):
                try:
                    conn = conn_or_pool.getconn()
                    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                    c = conn.cursor()

                    query = "SELECT * FROM %s WHERE stop_id='%s' \
                        AND reference BETWEEN %s AND %s \
                        ORDER BY eta_sample ASC;" \
                        % (table_name, station_id, start_tstamp, finish_tstamp)

                    c.execute(query)
                    l = c.fetchall()
                    conn_or_pool.putconn(conn)

                    s = name + ": number of rows fetched: " + str(len(l))
                    print s
                except psycopg2.ProgrammingError, err:
                    print name, ": an error occurred; skipping this select"
                    print err







