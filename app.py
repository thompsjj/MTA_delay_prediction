#!/usr/bin/env 
import sys, os
from schedule_tables import schedule_table
from sql_interface 

def main(argv):

    # load schedule table




    overall_schedule = schedule_table()
    overall_schedule.build('./google_transit/stop_times.txt', \
        './google_transit/stops.txt')

        # load line tables
        #print "getting stations"

        #stations = overall_schedule.get_stations()

        #print stations

        #print overall_schedule.get_station('J14S')

        #print overall_schedule.get_route('J')



    # construct topofile using schedule and line tables

    route_topology = Topology()

    # the topology can be updated with as many routes and joinfiles as desired

    route_topology.add_mta(overall_schedule.get_route('J'),joinfile=None)

    # construct a system from topofile

    mta_system = System()

    mta_system.build(route_topology, overall_schedule)

    # attach a line database and build schema from system

    # load system data using the delay - frequency paradigm - MAJOR FUNCTION
    # this is as opposed to actual arrival - schedule paradigm

    # there should be a separate load from past and update function. 


    # query distributions from database and pipeline back to system structure


    # system structure outputs dicts to libpgm



    # libpgm loads and produces first predictions



    pass



#possibly create a run function as a method of a class for the updated 
# forward pred. using the arrival-schedule paradigm 








if __name__ == '__main__':
    main(sys.argv)
