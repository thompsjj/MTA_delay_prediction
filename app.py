#!/usr/bin/env 
import sys, os
from schedule_tables import schedule_table, mta_route_schedule
from topology import Topology
from system import System, MTASystem
import psycopg2
from psycopg2.extras import DictCursor
from sql_interface import connect_to_local_db, sample_local_db_dict_cursor
import dill as pickle

import json




def main(argv):

    # load schedule table

        # load line tables
        #print "getting stations"

        #stations = overall_schedule.get_stations()

        #print stations

        #print overall_schedule.get_station('J14S')

        #print overall_schedule.get_route('J')

    mta_routes = mta_route_schedule()
    mta_routes.build('./google_transit/corrected_stop_times.txt', \
        './google_transit/stops.txt')
    # construct topofile using schedule and line tables

    route_topology = Topology()

    # the topology can be updated with as many routes and joinfiles as desired
    # here we are just testing route 1

    route_topology.add_mta_route(mta_routes.get_route('1'),'1')


    # construct a system from topofile
    reference_date = '2013-12-15-6-349-0-0-0'

    mta_system = MTASystem()
    mta_system.build(route_topology, mta_routes, reference_date)

    #mta_system.build(route_topology, None, reference_date)

  '''  for stid, station in mta_system.station.iteritems():
        print 'station: %s' % stid
        for i in station.neighbor_stations:
            print i'''


    cursor, conn = connect_to_local_db('mta_historical','postgres','postgres')


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

    #cursor, conn = sample_local_db_dict_cursor('mta_historical','postgres')


    #map arrivals times to stations
    mta_system.sample_arrival_times_from_db('2014-10-15', '2014-10-15','mta_historical','mta_historical_small', 'postgres', 'localhost', 'postgres')

    mta_system.compute_delay_histograms('2014-10-15', '2014-10-15',10)

    mta_system.compute_delay_state_diagrams('l')


    mta_system.save_snapshot()


    ##### LOAD FROM PICKLED SCHEDULES #####


    #######################################




   # mta_system.discrete_bayesian(target_file)


    # libpgm loads and produces first predictions

   # pgm_model.predict_discrete_bayesian(target_file)


#possibly create a run function as a method of a class for the updated 
# forward pred. using the arrival-schedule paradigm 








if __name__ == '__main__':
    main(sys.argv)
