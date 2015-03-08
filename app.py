#!/usr/bin/env 
import sys, os
from schedule_tables import schedule_table, mta_route_schedule
from topology import Topology
from system import System, MTASystem
import psycopg2
from sql_interface import connect_to_local_db

def main(argv):

    # load schedule table




   # overall_schedule = schedule_table()
   # overall_schedule.build('./google_transit/stop_times.txt', \
    #    './google_transit/stops.txt')

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


    #print mta_routes.get_station('J14S')


    # the topology can be updated with as many routes and joinfiles as desired
    # here we are just testing route 1

    route_topology.add_mta_route(mta_routes.get_route('1'),'1')

    # construct a system from topofile
    reference_date = '2013-12-15-6-349-0-0-0'



    mta_system = MTASystem()
    mta_system.build(route_topology, mta_routes, reference_date)

    # attach to historical db
    
    cursor, conn = connect_to_local_db('mta_historical','postgres')


    #map arrivals times to stations

    mta_system.sample_arrival_times_from_db(cursor, '2014-09-30', '2014-10-30')


    #stations own delays per schedule, or pattern of delays per sample. Both
    # objects should be available. 


    # system structure outputs dicts to libpgm

    # libpgm loads and produces first predictions




#possibly create a run function as a method of a class for the updated 
# forward pred. using the arrival-schedule paradigm 








if __name__ == '__main__':
    main(sys.argv)
