#!/usr/bin/env 
import sys, os
from schedule_tables import schedule_table, mta_route_schedule
from topology import Topology
from system import System
import psycopg2


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
    mta_routes.build('./google_transit/stop_times.txt', \
        './google_transit/stops.txt')
    # construct topofile using schedule and line tables

    route_topology = Topology()

    # the topology can be updated with as many routes and joinfiles as desired
    # here we are just testing route 1

    route_topology.add_mta_route(mta_routes.get_route('1'),'1')

    # construct a system from topofile
    
    mta_system = System()
    mta_system.build(route_topology, mta_routes)


    #print mta_system.station['101N'].schedule




    # attach a line database and build schema from system
    




    from mta_database_handlers import sample_mta_historical, \
    create_mta_etd_schema

    from sql_interface import connect_to_local_db

    cursor, conn = connect_to_local_db('mta_historical','postgres')

    create_mta_etd_schema(cursor, 'mta_historical')
    sample_mta_historical(cursor, 'mta_historical', \
     'https://datamine-history.s3.amazonaws.com/', '2014-09-17', '2014-9-17')

    # external (fundamentally temporary)
    # load system data into static database (ideally this would be a dynamically
    # collected database)

    
    


    
    # for number of days in num_days (need at least a few weeks):
        # for number of hours in a day 
            # for number of samples in an hour (cited on MTA site)
                # get the timedelta from 00:00:00 for that timestamp
                # also calculate the reference timedelta from the hour sample
                # delta(hoursample-arrivaltime)




    # using queries, compute delays and map to stations. Thus we have a station-
    # delay dataset where delays are calculated 12 times an hour using delay-
    # frequency paradigm (not sure how yet) the station owns a histogram of delays
    # this is as opposed to actual arrival - schedule paradigm which would be
    # computed from a dynamic database.

    #stations own delays per schedule, or pattern of delays per sample. Both
    # objects should be available. 


    # system structure outputs dicts to libpgm

    # libpgm loads and produces first predictions




#possibly create a run function as a method of a class for the updated 
# forward pred. using the arrival-schedule paradigm 








if __name__ == '__main__':
    main(sys.argv)
