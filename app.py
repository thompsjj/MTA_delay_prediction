#!/usr/bin/env 
import sys, os
from schedule_tables import schedule_table


def main(argv):

    # load schedule table

    schedule = schedule_table()


    # possibly load line tables

    # construct topofile using schedule and line tables

    # construct a system from topofile

    # attach a line database and build schema from system

    # load system data using the delay - frequency paradigm - MAJOR FUNCTION
    # this is as opposed to actual arrival - schedule paradigm

    # there should be a separate load from past and update function. 


    # query distributions from database and pipeline back to system structure


    # system structure outputs dicts to libpgm



    # libpgm loads and produces predictions



    pass



#possibly create a run function as a method of a class for the updated 
# forward pred. using the arrival-schedule paradigm 








if __name__ == '__main__':
    main(sys.argv)
