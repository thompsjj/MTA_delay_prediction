# -*- coding: utf-8 -*-
"""
Created on Mon Mar 2 17:32:02 2015

@author: Jared Thompson 

"""
from topology import Topology
from collections import defaultdict
from station import Station, MTAStation
import sys, os
import numpy as np
import time, datetime
# The system structure owns all lines, stations and trips, 
# and it owns all update methods from the top down. 

# The system structure is capable of returning a clean list (JSON or dict) of 
# nodes and data belonging to those nodes. 
# This is then pipelined to the libpgm model.

# System reads from updates cleanly but also needs a topology for 
# lines that have contact through stations. 

# In the case of MTA it may be important to join nodes conditionally on track
# relationships alone. In this way, the topology of the network is in some
# sense changing. However, the way the MTA is designed, they avoid node
# congestion in general, as lines do not change tracks and run along their own
# course. In addition, MTA has a very large number of available tracks. 



class System(object):

    def __init__(self):
        
        self.num_lines = 0
        self.num_stations = 0
        self.isBuilt = False
        self.station = defaultdict()

    def build(self, topology, schedule, reference_date, nbins):

        try:
            self._read_topology(topology)

            if schedule:
                # the n bins array can be adjusted later for variable bins
                self._populate_stations(schedule, reference_date, nbins)
                self.isBuilt = True
        except StandardError, e:
            print e


    def _read_topology(self, topology):

        if isinstance(topology, Topology):

            for v in topology.vertices:
                self.station[v] = Station(v)

            for e in topology.edges:
                self.station[e[0]].neighbor_stations.append(e[1])

    def _populate_stations(self, schedule, reference_date, nbins):

        '''This function takes a station id and sets its schedule for all
        trains and trips for every day that belong to this station'''

        for stid, stn in self.station.iteritems():

            if stid in ['126N','127N','128N']: #remove after scale up

                stn.set_schedule(schedule.table[stid]['arrivals'], reference_date)
                stn.delay_nbins = nbins

    def __repr__(self):
        print self.isBuilt



class MTASystem(System):

    def __init__(self):
        super(MTASystem, self).__init__()
        self.num_lines = 0
        self.num_stations = 0
        self.isBuilt = False
        self.station = defaultdict()
        self.num_histo_bins = None
        self.num_delay_histo_bins = None


    def sample_arrival_times_from_db(self, start_date, end_date, database, tablename, user, host, password):

        for stid, stn in self.station.iteritems():
            if stid in ['126N','127N','128N']: # remove after scale up

                # stn.sample_history_from_db(cursor, start_date, end_date)
                # stn.sample_history_from_db_parallel('mta_historical','mta_historical_small',start_date, end_date)
                stn.sample_history_from_db_threaded( start_date, end_date, database, tablename, user, host, password)   


    def compute_delay_histograms(self, paradigm, start_date, end_date, nbins):
        self.num_delay_histo_bins = nbins
        for stid, stn in self.station.iteritems():
            if stid in ['126N','127N','128N']: # remove after scale up

            # stn.sample_history_from_db(cursor, start_date, end_date)
            # stn.sample_history_from_db_parallel('mta_historical','mta_historical_small',start_date, end_date)

                stn.compute_delay_histogram(paradigm, start_date, end_date)

                print "complete station id: %s num_nonzero: %s" % (stid, np.count_nonzero(stn.delay_schedule))



    def compute_delay_state_diagrams(self, paradigm, start_date, end_date,nbins):
        self.num_delay_histo_bins = nbins
        for stid, stn in self.station.iteritems():
            if stid in ['126N','127N','128N']: # remove after scale up

                stn.compute_delay_state_diagram(paradigm)
                print stn.delay_state_diagram

    def _read_topology(self, topology):

        print 'reading topology'


        if isinstance(topology, Topology):

            for v in topology.vertices:
                self.station[v] = MTAStation(v)

            for e in topology.edges:
                self.station[e[0]].neighbor_stations_names.append(e[1])
                self.station[e[0]].child_stations_names.append(e[1])
                self.station[e[0]].child_stations.append(self.station[e[1]])

            for e in topology.edges:
                self.station[e[1]].parent_stations_names.append(e[0])
                self.station[e[1]].parent_stations.append(self.station[e[0]])
                self.station[e[1]].has_parents = True

            """for stid, station in self.station.iteritems():
                print "my id: %s" % (stid)
                if station.has_parents:
                    print "my parents: %s" % station.parent_stations_names
            sys.exit(0)"""


    def save_snapshot(self):
        tmstmp = int(time.mktime(datetime.datetime.now().timetuple()))
        for stid, stn in self.station.iteritems():
            if stid in ['126N','127N','128N']: #remove after scale up
                stn.save_delay_histos(tmstmp)


    def save_history(self):
        tmstmp = int(time.mktime(datetime.datetime.now().timetuple()))
        for stid, stn in self.station.iteritems():
            if stid in ['126N','127N','128N']: #remove after scale up
                stn.save_station_history(tmstmp)


    def condense_weekdays(self):
        for stid, stn in self.station.iteritems():
            if stid in ['126N','127N','128N']: #remove after scale up
                stn.condense_weekdays()


    def load_history(self, target_dir):
        for stid, stn in self.station.iteritems():
            if stid in ['126N','127N','128N']: #remove after scale up
                stn.load_station_history(target_dir)



    def save_delay_state_file(self):
        outputdict = {}


        ### OUTPUT VERTICES AND EDGES ###

        outputdict["V"] = []
        outputdict["E"] = []

        for stid, station in self.station.iteritems():
            if stid in ['126N','127N','128N']:
                outputdict["V"].append(stid)
                edge_set = []
                for stid_e in enumerate(station.neighbor_stations_names):
                    edge_set.append([stid, stid_e])
                outputdict["E"].append(edge_set)


        outputdict["Vdata"] = {}

        for stid, station in self.station.iteritems():
            if stid in ['126N','127N','128N']:
                outputdict["Vdata"][stid] = {}
                outputdict["Vdata"][stid]["numoutcomes"] = station._delay_nbins
                outputdict["Vdata"][stid]["vals"] = [x for x in xrange(0,station._delay_nbins)]
                outputdict["Vdata"][stid]["parents"] = station.parent_stations_names
                outputdict["Vdata"][stid]["children"] = station.child_stations_names

             ########## BUILD CPROB HERE #################

                outputdict["Vdata"][stid]["cprob"] = {}
                for d, day in enumerate(station.days):
                    for h, hour in enumerate(station.hours):
                        for m, minute in enumerate(station.sample_points):
                            for k, v in station.delay_state[day][hour][minute].iteritems():
                                index = "['%s']['%s']['%s']" % (day, hour, minute)
                                for i, e in enumerate(k):
                                    index += "['%s']" % e

                                #print 'iterating'
                               # print k
                                #print index
                                #print station.delay_states[day][hour][minute][k]
                                outputdict["Vdata"][stid]["cprob"][index] = station.delay_state[day][hour][minute][k]



      # for k, v in outputdict["Vdata"]['127N']["cprob"].iteritems():
        #    print k
         #   print v'''





