
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 24 08:23:34 2015

@author: Jared
"""

from collections import defaultdict
from datetime import date
from dateutil.rrule import rrule, DAILY
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import DictCursor
import threading
import time
import itertools
import numpy as np
import sys, os, glob, errno
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
            return days[(days.index(day) + 1) % len(days)]

        for day in self.days:
            schedule_today = self.schedule[day]
            for hour in self.hours:
                for minute in self.sample_points:

                    # take a zip between actual time and difference between
                    # this and the current time point

                    current_sample_pt = \
                        timedelta_from_timestring(hour + ':' + minute + ':00')

                    # calculate point greater than zero but closest to 0
                    check_pt = [z for z in zip((schedule_today - current_sample_pt), schedule_today) if
                                z[0] > timedelta_from_timestring('00:00:00')]

                    if len(check_pt) > 0:
                        closest_train = min(check_pt, key=lambda x: x[0])[0]

                    elif (hour == self.hours[-1]):
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
            for h, hour in enumerate(self.hours):
                for m, minute in enumerate(self.sample_points):
                    if self.conf_schedule[day][hour][minute] == 'next':
                        self.conf_schedule[day][hour][minute] = \
                            self.conf_schedule[next_day(day, self.days)] \
                                [self.hours[0]][self.sample_points[0]] \
                            + timedelta_from_timestring('00:05:00')


    event = threading.Event()

    def handler(signum, _frame):
        # global event
        # event.set()
        print('Signal handler called with signal [%s]' % signum)


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

        sample_tstamp = []
        THREAD_NAMES = []
        sample_names = []
        for dt in rrule(DAILY, dtstart=a, until=b):
            for hour in self.hours:
                for minute in self.sample_points:
                    sample_tstamp.append(timestamp_from_refdt(hour, minute, dt, 'US/Eastern') + 3600)
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
                    print 'no historical schedule time was found'
                    return 0
                else:
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

        threads = []
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
                                                          max_tstamp), \
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
            #timestamp_from_refdt(self.hours[-1],self.sample_points[-1],b,'US/Eastern')+3600
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

        if paradigm in ['l', 'lit', 'literal']:

            print 'self conf. schedule:'
            print self.conf_schedule

            # Everything should be based now on the historical timestamps.




            for d, day in enumerate(self.days):
                for h, hour in enumerate(self.hours):
                    for m, minute in enumerate(self.sample_points):
                        if self.conf_schedule[day][hour][minute]:
                            if self.historical_timestamps[day][hour][minute] \
                                    and self.historical_schedule[day][hour][minute]:

                                    self.delays_computed = 1

                                    #new code here

                                    ### conf schedule is delta timepoint ###

                                    print self.conf_schedule[day][hour][minute].seconds


                                    reference_timepoint = \
                                        self.conf_schedule[day][hour][minute].seconds+\
                                        self.historical_timestamps[day][hour][minute][0]

                                    print reference_timepoint

                                    print 'historical schedule'

                                    print 'timestamp'

                                    print self.historical_timestamps[day][hour][minute]

                                    print 'historical schedule'

                                    print self.historical_schedule[day][hour][minute]

                                    print 'conf schedule'

                                    print self.conf_schedule[day][hour][minute].seconds

                                    print '%s %s %s' % (day, hour, minute)
                                    # Subtract the reference point from the historical schedule to get the naive delays

                                    self.delay_schedule[day][hour][minute] = \
                                       np.array(self.historical_schedule[day][hour][minute])-reference_timepoint


                                    print self.delay_schedule[day][hour][minute]



                                    sys.exit(1)

                                    # Bin the naive delays. First you must compute a blind histogram.

                                    #



        else:
            print "delay paradigm not recognized."





    def collect_concurrent_states(self, parent_state_vector, self_state_vector, temp_state_vector):
        for e, element in enumerate(self_state_vector):

            # this needs to be enumerated by the parent states

            # NEEDS RECODING. WE HAVE THE PARENT STATES

            self_state = element
            print 'inside collecting concurrent states:'
            print self.station_id
            print 'parent delay vec'
            print parent_state_vector
            print 'self delay vec'
            print self_state_vector
            print np.any(parent_state_vector)

            sys.exit(0)
            if np.any(parent_state_vector):
                for e, self_state in self_state_vector:
                    parent_states = []
                    for v in parent_state_vector:
                        parent_states.append(v[e])
                    temp_state_vector[tuple(parent_states)].append(self_state)

                    print temp_state_vector
            else:
                for e, self_state in self_state_vector:
                    temp_state_vector[tuple([0])].append(self_state)

    def store_delay_states(self, day, hour, minute, temp_delay_states):
        for k, v in temp_delay_states.iteritems():

            print "inside storing delay states: %s %s" % (k,v)

            # changing now, but producing a strange vector of delay states with no difference in parent state

            time.sleep(5)

            self.delay_state[day][hour][minute][k] = \
                np.histogram(v, bins=self.delay_nbins, range=(0, self.delay_nbins),normed=False)[0]

    def store_delay_state_for_root_node(self, day, hour, minute, temp_delay_states):
        self.delay_state[day][hour][minute][0] = \
                np.histogram(temp_delay_states[0], bins=self.delay_nbins, range=(0, self.delay_nbins))[0]

    def find_timestamp(self, day, dt, hour, minute):
        ref = timestamp_from_refdt(hour, minute, dt, 'US/Eastern') + 3600
        return float(self.conf_schedule[day][hour][minute].seconds) + ref


    def compute_parent_states(self):

        """This function produces a list of joint states, enumerated by the
        bins of the n parents of this station.
        INPUT: self (station)
        OUTPUT: None """

        # usually uniform
        histogram_size = []
        if self.has_parents:
            # "parent stations not getting appended"
           # print self.parent_stations
            for st, station in enumerate(self.parent_stations):
                # need to set nbins upstream
                if station.delay_nbins:
                    histogram_size.append([b for b in xrange(station.delay_nbins)])
                   # print 'delay bins: %s' % station.delay_nbins
                else:
                    histogram_size.append([0])
           # print 'histogram size'
           # print histogram_size
            self.parent_states = [i for i in itertools.product(*histogram_size)]

           # print "I AM %s ; parent states:" % (self.station_id)
           # print self.parent_states
           # time.sleep(5)



    def fill_delay_tensor(self, day, hour, minute):
        """ This function fills the delay tensor with zeros so to avoid null value problems """

        # Precompute the number of parent states here if this node has parents.

        if self.has_parents:
            # if this station has parents

            for i, st in enumerate(self.parent_states):
                self.delay_state[day][hour][minute][st] = [0 for x in xrange(self.delay_nbins)]
        else:
            # in the case that this is a source node, there is only one parent state.

            self.delay_state[day][hour][minute][0] = [0 for x in xrange(self.delay_nbins)]


    def sample_point_occupied(self, day, hour, minute):

        print 'historical schedule:'
        print self.historical_schedule[day][hour][minute]
        time.sleep(1)

        if np.any(self.historical_schedule[day][hour][minute]):
            return True
        else:
            return False

    def calculate_delay_vector(self, c, day, hour, minute):
        print 'conf. reference time: %s' % c
        delay_vec = [v - c if v - c > 0 else 0 for v in self.historical_schedule[day][hour][minute]]
        return delay_vec

    def compute_delay_state_diagram(self, paradigm, startdate, enddate, aggregate_wkdays=False):
        """This function computes the delay state diagrams based on the delay
           histograms"""

        self.delay_state = defaultdict(
            lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict()))))

        # hacky due to non condensed histograms

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

        if paradigm in ['l', 'lit', 'literal']:

            ### THEN GO OVER EACH DAY FOR EACH STATION AND CALCULATE DELAY STATES RELATIVE TO PARENTS ###
            ### DELAY ARE ARRIVAL-EXPECTED TIM


            for d, day in enumerate(self.days):
                for h, hour in enumerate(self.hours):
                    for m, minute in enumerate(self.sample_points):
                        pass
                        # this is just a simple subtraction and binning
                        # self_delay_vec = self.calculate_delay_vector(day, hour, minute)





            ### AFTER, CALCULATE nbinned HISTOGRAM BASED ON PARENT STATE, STATION STATE

                """for dt in rrule(DAILY, dtstart=a, until=b):
                d = dt.weekday()
                day = self.days[d]
                for h, hour in enumerate(self.hours):
                    for m, minute in enumerate(self.sample_points):
                        tmstmp = self.find_timestamp(day, dt, hour, minute)
                        # there are two key problems here. The delay vector contains the entire history of that timept
                        # so this algo is recalculating at every tp ( this might be the only way to do it.)
                        # doesn't matter b/c its fast..

                        # is there a better way to do this?

                        # we need to have the list of states for every point in the tensor.


                        #### THIS IS WRONG BECAUSE IT NEEDS TO BE TUNED TO THE NUMBER OF HISTORY POINTS###
                        #### THIS IS OVERWRITING ON EVERY DAY ####
                        # self.fill_delay_tensor(day, hour, minute)



                        # Test here for occupancy of both self and parents
                        self_occupied = self.sample_point_occupied(day, hour, minute)

                        # Are parents occupied?

                        # parents_occupied = SOMETHING

                        if self_occupied and parents_occupied:


                            print "occupied: %s" % occupied

                            self_delay_vec = self.calculate_delay_vector(tmstmp, day, hour, minute)

                            print 'self delay vec:'
                            print self_delay_vec


                            parent_delay_vecs = []
                            for p in self.parent_stations:
                                print "parent: %s" % (p.station_id)
                                parent_delay_vecs.append(p.calculate_delay_vector(c, day, hour, minute))
                                print parent_delay_vecs

                            # collect concurrent states into a single vector

                            print 'parent delay vecs'
                            print parent_delay_vecs


                            sys.exit(0)

                            # temp_delay_states = defaultdict(list)
                            # self.collect_concurrent_states(parent_delay_vecs, self_delay_vec, temp_delay_states)


                            print 'storing delay states'
                            self.store_delay_states(day, hour, minute, temp_delay_states)

                        elif occupied:
                            temp_delay_states = defaultdict(list)
                            temp_delay_states[0] = self_delay_vec
                            self.store_delay_state_for_root_node(day, hour, minute, temp_delay_states)
                        else:
                            print 'time point %s %s %s ignored for station %s' % (day,hour, minute, self.station_id)"""


    def condense_delay_state_weekdays(self):
        weekdays = ['MON', 'TUE', 'WED', 'THU', 'FRI']
        for day in self.days():
            for hour in self.hours():
                for minute in self.minutes():
                    for st in self.parent_states:
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
                pickle.dump(self._delay_state, outfile)

            with open('%s_%s_MTA_day_points.pkl' % (timestamp, self.station_id), 'wb') as outfile:
                pickle.dump(self.days, outfile)

            with open('%s_%s_MTA_hour_points.pkl' % (timestamp, self.station_id), 'wb') as outfile:
                pickle.dump(self.hours, outfile)

            with open('%s_%s_MTA_minute_sample_points.pkl' % (timestamp, self.station_id), 'wb') as outfile:
                pickle.dump(self.sample_points, outfile)

    def save_delay_histos(self, timestamp):
        if np.any(self._delay_schedule):

            with open('%s_%s_MTA_delay_histo.pkl' % (timestamp, self.station_id), 'wb') as outfile:
                pickle.dump(self._delay_schedule, outfile)

            with open('%s_%s_MTA_day_points.pkl' % (timestamp, self.station_id), 'wb') as outfile:
                pickle.dump(self.days, outfile)

            with open('%s_%s_MTA_hour_points.pkl' % (timestamp, self.station_id), 'wb') as outfile:
                pickle.dump(self.hours, outfile)

            with open('%s_%s_MTA_minute_sample_points.pkl' % (timestamp, self.station_id), 'wb') as outfile:
                pickle.dump(self.sample_points, outfile)


