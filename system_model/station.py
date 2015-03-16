
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 24 08:23:34 2015

@author: Jared
"""

from collections import defaultdict
from datetime import date
import datetime
from dateutil.rrule import rrule, DAILY
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import DictCursor
import threading
import time
import itertools
import numpy as np
import sys, os, time, errno
from uniquelist import uniquelist
from datetime_handlers import timedelta_from_timestring, \
    timestamp_from_refdt
import dill as pickle

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

        self.parent_delay_status = 1
        self.delay_bins = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict())))
        self.delay_schedule = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list))))
        self.parent_states = [0]
        self.station_id = station_id
        self.neighbor_stations = uniquelist()
        self.child_stations = uniquelist()
        self.parent_stations = uniquelist()
        self.neighbor_stations_names = uniquelist()
        self.child_stations_names = uniquelist()
        self.parent_stations_names = uniquelist()
        self.collecting = False
        self.in_collection = False
        self.schedule = defaultdict(list)
        self.schedule_set = False
        self.delays_computed = 0
        self.has_parents = 0
        self.conf_schedule = defaultdict(lambda: defaultdict(lambda: defaultdict()))
        self.historical_schedule = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        self.historical_timestamps = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        self.historical_index = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
        self.delay_state = defaultdict(
            lambda: defaultdict(lambda: defaultdict(lambda: defaultdict())))
        self.delay_state_diagram = defaultdict(
            lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list))))

        self.days = ['MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT', 'SUN']

        self.hours = ['00', '01', '02', '03', '04', '05', '06', '07', '08', \
                      '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', \
                      '21', '22', '23']

        self.sample_points = ['01', '06', '11', '16', '21', '26', '31', '36', '41', \
                              '46', '51', '56']

        self._delay_schedule = None
        self.delay_nbins = 0

    # this is MTA 26H specific and should be in a descendent class
    def _pass_schedule_to_timedelta(self, ext_schedule):
        for day, sched in ext_schedule.iteritems():
            for t, pt in enumerate(sched):
                self.schedule[day].append(timedelta_from_timestring(pt))

            self.schedule[day] = np.asarray(sorted(self.schedule[day]))


    def set_schedule(self, ext_schedule, reference_date):
        self.reference_date = reference_date

        self._pass_schedule_to_timedelta(ext_schedule)

        self._calculate_conformal_schedule()

        self.schedule_set = True


    def _calculate_conformal_schedule(self):
        """ using queries, compute delays and map to stations. Thus we have a station-
         delay dataset where delays are calculated 12 times an hour using delay-
         frequency paradigm the station owns a histogram of delays
         this is as opposed to actual arrival - schedule paradigm which would be
         computed from a dynamic database."""

        for day in self.days:
            # self.schedule should be in hours past midnight
            schedule_today = self.schedule[day]
            current_sample_index = 0
            for hour in self.hours:
                for minute in self.sample_points:

                    current_sample_pt = \
                        timedelta_from_timestring(hour + ':' + minute + ':00')

                    # print "station id: %s sampling %s %s %s %s" % (self.station_id, day, hour, minute, current_sample_pt)

                    # calculate point greater than zero but closest to 0

                    check_pt = [z for z in zip((schedule_today - current_sample_pt), schedule_today) if
                                timedelta_from_timestring('00:05:00') > z[0] >= timedelta_from_timestring('00:00:00')]

                    # print "next point in schedule"
                    closest_train = None
                    if len(check_pt) > 0:
                        closest_train = min(check_pt, key=lambda x: x[0])[0]

                    """else:
                        print '_calculate_conformal_schedule: \n \
                        no trains were found while setting the schedule for the schedule for station %s on %s %s %s'\
                              % (self.station_id, day, hour, minute)"""

                    self.conf_schedule[day][hour][minute] = closest_train
                    current_sample_index += 1

    def sample_history_from_db(self, cursor, startdate, enddate):
        from mta_database_handlers import query_mta_historical_closest_train_between


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

        max_tstamp = timestamp_from_refdt(self.hours[-1], self.sample_points[-1], b, 'US/Eastern') + 3600

        interval = []
        #unroll inner loop
        for hour in self.hours:
            for minute in self.sample_points:
                interval.append((hour, minute))

        start_outer = time.clock()
        for dt in rrule(DAILY, dtstart=a, until=b):
            (self.station_id, dt.strftime("%Y-%m-%d"))
            for interv in interval:
                day = self.days[dt.weekday()]
                hour = interv[0]
                minute = interv[1]
                current_tstamp = timestamp_from_refdt(hour, minute, dt, 'US/Eastern') + 3600
                response = query_mta_historical_closest_train_between(cursor, 'mta_historical_small', self.station_id,
                                                                      current_tstamp, max_tstamp)
                self.historical_schedule[day][hour][minute].append(response)
                self.historical_timestamps[day][hour][minute].append(current_tstamp)

        for day in self.days:
            for hour in self.hours:
                for minute in self.sample_points:
                    self.historical_schedule[day][hour][minute] = np.asarray(self.historical_schedule[day][hour][minute])

        print "full loop: %s station: %s" % ((time.clock() - start_outer), (self.station_id))


    def sample_history_from_db_threaded(self, startdate, enddate, database, \
                                        table_name, user, host, password):
        start_outer = time.clock()
        stride = 299 # less than 5 minute stride

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

        # max_tstamp = timestamp_from_refdt(self.hours[-1], self.sample_points[-1], b, 'US/Eastern') + 3600

        sample_tstamp = []
        THREAD_NAMES = []
        sample_names = []
        for dt in rrule(DAILY, dtstart=a, until=b):
            for hour in self.hours:
                for minute in self.sample_points:
                    sample_tstamp.append(timestamp_from_refdt(hour, minute, dt, 'US/Eastern'))
                    THREAD_NAMES.append(
                        "Thread: %s :: %s :%s:%s" % (self.station_id, dt.strftime("%Y-%m-%d"), hour, minute))
                    sample_names.append((dt, hour, minute))

        #####################Threading Functions Here###########################

        ## PRIMARY FUNCTION ##

        def select_func(conn_or_pool, table_name, station_id, start_tstamp, stop_tstamp):
            name = threading.currentThread().getName()

            try:


                conn = conn_or_pool.getconn()
                conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
                c = conn.cursor(cursor_factory=DictCursor)

                query = "SELECT * FROM %s WHERE stop_id='%s' \
                        AND reference BETWEEN %s AND %s \
                        ORDER BY eta_sample ASC;" \
                        % (table_name, station_id, start_tstamp, stop_tstamp)

                c.execute(query)
                l = c.fetchall()

                conn.commit()
                conn_or_pool.putconn(conn, close=True)

                """print 'select_func: %s %s' % (start_tstamp, stop_tstamp)
                print '%s' % (query)
                print 'response: %s' % l"""

                if not l:
                    print "failed query:"
                    print query

                    #set this to None
                    return None
                else:
                    print "%s time found: %s" % (name, datetime.datetime.fromtimestamp(l[0]['eta_sample']))
                    return l[0]['eta_sample']

            except psycopg2.ProgrammingError, err:
                print name, ": an error occurred; skipping this select"
                print err

        ## WRAPPER ##

        def wrapper(func, args, name, result):
            result.append((args[3], name, func(*args)))

        ## GROUPER ##

        def chunker(seq, size):
            return (seq[pos:pos + size] for pos in xrange(0, len(seq), size))

        CHUNK_SIZE = 50
        results = []

        ## CHUNK THREADS INTO FLIGHTS ##

        for c, chunk in enumerate(chunker(THREAD_NAMES, CHUNK_SIZE)):
            #print "chunk %s" % c
            min_t = 1
            max_t = CHUNK_SIZE
            threads = []
            res = []

            ## OPEN CONNECTION ##


            conn_select = ThreadedConnectionPool(min_t, max_t, database=database, user=user, host=host,
                                                 password=password, async=0)


            ## JOIN THREADS ##

            for i, name in enumerate(chunk):
                t = threading.Thread(target=wrapper, name=name, \
                                     args=( select_func, (conn_select, table_name, \
                                                          self.station_id, sample_tstamp[i + c * CHUNK_SIZE],
                                                          sample_tstamp[i + c * CHUNK_SIZE]+stride), \
                                            sample_names[i + c * CHUNK_SIZE], res))

                t.setDaemon(0)
                threads.append(t)

            for i, t in enumerate(threads):
                t.start()

            for i, t in enumerate(threads):
                t.join()
                print t.getName(), "exited OK"

            results.extend(res)
            conn_select.closeall()

        for i, result in enumerate(results):
            day = self.days[result[1][0].weekday()]
            hour = result[1][1]
            minute = result[1][2]

            self.historical_timestamps[day][hour][minute].append(result[0])
            self.historical_schedule[day][hour][minute].append(result[2])
            self.historical_index[day][hour][minute] += 1

        print "full loop: %s station: %s" % ((time.clock() - start_outer), (self.station_id))
        print self.historical_schedule


    def compute_delay_histogram(self, paradigm, startdate, enddate):

        """Calculates the delay histograms from histories and conformal schedule
        stored to the object. Uses a delay paradigm, either projective or literal
        INPUT: self, int (number of bins)  (schedules as stored in object)
        OUTPUT: bool (success) (delay histos are stored back to object)"""

        #print self.station_id

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

        for d, day in enumerate(self.days):
            for h, hour in enumerate(self.hours):
                for m, minute in enumerate(self.sample_points):
                    if self.conf_schedule[day][hour][minute]:
                        # THIS NEEDS TO BE REWRITTEN FOR THE NEW PARADIGM
                        if self.historical_timestamps[day][hour][minute] \
                                and self.historical_schedule[day][hour][minute]:

                            # The historical timestamps consider all sample points in the history

                            reference_timepoints = \
                                        self.conf_schedule[day][hour][minute].seconds+\
                                        np.asarray(self.historical_timestamps[day][hour][minute])

                            # Subtract the reference point from the historical schedule to get the naive delays

                            self.delay_schedule[day][hour][minute] = np.array(self.historical_schedule[day][hour][minute])-reference_timepoints
                            histo = np.histogram(self.delay_schedule[day][hour][minute], bins=(self.delay_nbins-1))
                            self.delay_bins[day][hour][minute] = histo[1]
                            self.delay_state[day][hour][minute] = np.digitize(self.delay_schedule[day][hour][minute], bins=(histo[1]))-1
                            self.delays_computed = 1


    def compute_delay_state_diagram(self, paradigm):
        """This function computes the delay state diagrams based on the delay
           states. This means that the function determines the JOINT state of the parents (usually 1)
           and stores the state of self in the history dependent on the parents"""

        if self.has_parents:
            for p in self.parent_stations:
                self.parent_delay_status *= p.delays_computed
            print "parent delay status: %s" % self.parent_delay_status

        if paradigm in ['l', 'lit', 'literal']:

            for d, day in enumerate(self.days):
                for h, hour in enumerate(self.hours):
                    for m, minute in enumerate(self.sample_points):
                        if self.has_parents and self.parent_delay_status:

                            # collect delay states into diagram
                            for e, value in enumerate(self.delay_state[day][hour][minute]):
                                parent_states = []
                                for p in self.parent_stations:
                                    parent_states.append(p.delay_state[day][hour][minute][e])

                                self.delay_state_diagram[day][hour][minute][tuple(parent_states)].append(value)

                        elif self.has_parents and not self.parent_delay_status:
                            # compute the delay diagram with modified parent states
                            for e, value in enumerate(self.delay_state[day][hour][minute]):
                                parent_states = []
                                for p in self.parent_stations:
                                    if p.delays_computed:
                                        parent_states.append(p.delay_state[day][hour][minute][e])
                                    else:
                                        parent_states.append(0)

                                self.delay_state_diagram[day][hour][minute][tuple(parent_states)].append(value)
                        else:
                            for e, value in enumerate(self.delay_state[day][hour][minute]):
                                self.delay_state_diagram[day][hour][minute][tuple([0])].append(value)

            for d, day in enumerate(self.days):
                for h, hour in enumerate(self.hours):
                    for m, minute in enumerate(self.sample_points):
                        for s, state in enumerate(self.enumerate_parent_states()):
                            self.delay_state_diagram[day][hour][minute][state] = \
                                np.histogram(self.delay_state_diagram[day][hour][minute][state], bins=(self.delay_nbins-1))[0]

    def enumerate_parent_states(self):
        if self.has_parents and self.parent_delay_status:
            parent_states = []
            for p in self.parent_stations:
                parent_states.append([z for z in range(p.delay_nbins)])
            return list(itertools.product(*parent_states))

        elif self.has_parents and not self.parent_delay_status:
            parent_states = []
            for p in self.parent_stations:
                if self.delays_computed:
                    parent_states.append([z for z in range(p.delay_nbins)])
                else:
                    parent_states.append([0])
            return list(itertools.product(*parent_states))
        else:
            return [(0,)]


    #def rectify_delay_state_histograms(self):



    def condense_delay_state_weekdays(self):
        weekdays = ['MON', 'TUE', 'WED', 'THU', 'FRI']
        for day in self.days():
            for hour in self.hours():
                for minute in self.minutes():
                    for st in self.enumerate_parent_states():
                        if day in weekdays:
                            self.delay_state['WKD'][hour][minute][st] += self.delay_state[day][hour][minute][st]


    def save_station_history(self, timestamp):
        print 'saving history'
        if np.any(self.historical_schedule):
            with open('%s_%s_MTA_historical_schedule.pkl' % (timestamp, self.station_id), 'wb') as outfile:
                pickle.dump(self.historical_schedule, outfile)

        if np.any(self.historical_timestamps):
            with open('%s_%s_MTA_historical_timestamps.pkl' % (timestamp, self.station_id), 'wb') as outfile:
                pickle.dump(self.historical_timestamps, outfile)

    def load_station_history(self, path):

        for dir_entry in os.listdir(path):
            dir_entry_path = os.path.join(path, dir_entry)
            if os.path.isfile(dir_entry_path):
                name = dir_entry_path

                if (name.split('_')[1] == self.station_id) and (name.split('.')[-1] == 'pkl'):
                    print "inside 1: %s" % name.split('.')

                    #print name.split('.')[0]
                    if name.split('.')[1].split('_')[-1] == 'schedule':
                        try:
                            with open(name,'rb') as f:
                                print 'schedule detected'
                                pkl_history = pickle.load(f)
                                if not np.any(pkl_history):
                                     print 'An empty history was loaded for station %s' % self.station_id
                                self.historical_schedule = pkl_history
                                print self.historical_schedule
                        except IOError as exc:
                            if exc.errno != errno.EISDIR: # Do not fail if a directory is found, just ignore it.
                                raise # Propagate other kinds of IOError.

                    if name.split('.')[1].split('_')[-1] == 'timestamps':
                        try:
                            with open(name,'rb') as f:
                                print 'timestamps detected'
                                pkl_timestamps = pickle.load(f)
                                if not np.any(pkl_timestamps):
                                    print 'An empty timestamp set was loaded for station %s' % self.station
                                self.historical_timestamps = pkl_timestamps
                                print self.historical_timestamps

                        except IOError as exc:
                            if exc.errno != errno.EISDIR: # Do not fail if a directory is found, just ignore it.
                                raise # Propagate other kinds of IOError.




    def save_station_delay_state(self, timestamp):
        if np.any(self.delay_state):
            with open('%s_%s_MTA_delay_state_diagram.pkl' % (timestamp, self.station_id), 'wb') as outfile:
                pickle.dump(self.delay_state, outfile)

            with open('%s_%s_MTA_day_points.pkl' % (timestamp, self.station_id), 'wb') as outfile:
                pickle.dump(self.days, outfile)

            with open('%s_%s_MTA_hour_points.pkl' % (timestamp, self.station_id), 'wb') as outfile:
                pickle.dump(self.hours, outfile)

            with open('%s_%s_MTA_minute_sample_points.pkl' % (timestamp, self.station_id), 'wb') as outfile:
                pickle.dump(self.sample_points, outfile)

    def save_delay_histos(self, timestamp):
        if np.any(self._delay_schedule):

            with open('%s_%s_MTA_delay_histo.pkl' % (timestamp, self.station_id), 'wb') as outfile:
                pickle.dump(self.delay_schedule, outfile)

            with open('%s_%s_MTA_day_points.pkl' % (timestamp, self.station_id), 'wb') as outfile:
                pickle.dump(self.days, outfile)

            with open('%s_%s_MTA_hour_points.pkl' % (timestamp, self.station_id), 'wb') as outfile:
                pickle.dump(self.hours, outfile)

            with open('%s_%s_MTA_minute_sample_points.pkl' % (timestamp, self.station_id), 'wb') as outfile:
                pickle.dump(self.sample_points, outfile)


