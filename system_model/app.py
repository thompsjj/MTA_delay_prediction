#!/usr/bin/env 
import sys, os
from schedule_tables import schedule_table, mta_route_schedule
from topology import Topology
from system import System, MTASystem
import time, datetime
import psycopg2
from psycopg2.extras import DictCursor
from sql_interface import connect_to_local_db, \
    connect_to_db, sample_local_db_dict_cursor
import dill as pickle
import json




def main(argv):
    mode = 'LOAD_SYS_STORE_HIST'

    if mode == 'STORE':
        mta_routes = mta_route_schedule()
        mta_routes.build('../google_transit/corrected_stop_times.txt', \
        '../google_transit/stops.txt')

        # construct topofile using schedule and line tables

        route_topology = Topology()

        route_topology.add_mta_route(mta_routes.get_route('1'),'1')

        reference_date = '2013-12-15-6-349-0-0-0'

        mta_system = MTASystem()
        mta_system.build(route_topology, mta_routes, reference_date, 10)

        print mta_system.station['101S'].conf_schedule['MON']['00']

        tmstmp = int(time.mktime(datetime.datetime.now().timetuple()))

        with open('%s_MTA_system.pkl' % (tmstmp), 'wb') as outfile:
            pickle.dump(mta_system, outfile)

    elif mode == "LOADALL" or mode == 'LOAD_SYS_STORE_HIST':
        with open('1426540886_MTA_system.pkl', 'rb') as infile:
            mta_system = pickle.load(infile)


    for k, v in mta_system.station['104S'].conf_schedule['MON']['00'].iteritems():
        print "%s: %s" % (k, v)

    cursor, conn = connect_to_local_db('mta_historical','postgres','postgres')

    # to connect to a remote db
    #cursor, conn = connect_to_db('mta_historical','postgres','ec2-54-67-95-112.us-west-1.compute.amazonaws.com','user')

    try:
        cursor.execute("CREATE INDEX stid ON mta_historical_small USING gin (to_tsvector('english',stop_id));")
    except Exception, e:
        print e

    try:
        cursor.execute("CREATE INDEX ON mta_historical_small USING btree (reference);")
    except Exception, e:
        print e

    cursor.close()
    conn.close()

    # map arrivals times to stations

    if mode == 'STORE' or mode == 'LOAD_SYS_STORE_HIST':
        mta_system.sample_arrival_times_from_db('2014-11-24', '2014-11-25','mta_historical','mta_historical_small', 'postgres', 'localhost', 'user')
        mta_system.save_history()
        sys.exit(1)
    elif mode == 'LOAD' or mode == 'LOAD_ALL':
        print 'attempting to load history'
        mta_system.load_history('./history2')

# Delay histograms need to be calculated first
    sys.exit(0)
    mta_system.compute_delay_histograms('l', '2014-11-24', '2014-11-25', 10)

    print 'computing state diagrams'

    mta_system.compute_delay_state_diagrams('l','2014-11-24', '2014-11-25', 10)

 #   mta_system.save_snapshot()

    mta_system.save_delay_state_file()


   # mta_system.discrete_bayesian(target_file)


    # libpgm loads and produces first predictions

   # pgm_model.predict_discrete_bayesian(target_file)


#possibly create a run function as a method of a class for the updated 
# forward pred. using the arrival-schedule paradigm 








if __name__ == '__main__':
    main(sys.argv)
