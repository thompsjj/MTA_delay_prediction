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

    def build(self, topology, schedule, reference_date):

        try:
            self._read_topology(topology)

            if schedule:
                self._populate_stations(schedule, reference_date)
                self.isBuilt = True
        except StandardError, e:
            print e


    def _read_topology(self, topology):

        if isinstance(topology, Topology):

            for v in topology.vertices:
                self.station[v] = Station(v)

            for e in topology.edges:
                self.station[e[0]].neighbor_stations.append(e[1])

    def _populate_stations(self, schedule, reference_date):

        '''This function takes a station id and sets its schedule for all
        trains and trips for every day that belong to this station'''


        for stid, stn in self.station.iteritems():

            if stid in ['126N','127N','128N']: #remove after scale up

                stn.set_schedule(schedule.table[stid]['arrivals'], reference_date)

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

                #stn.sample_history_from_db(cursor, start_date, end_date)
                #stn.sample_history_from_db_parallel('mta_historical','mta_historical_small',start_date, end_date)
                stn.sample_history_from_db_threaded( start_date, end_date, database, tablename, user, host, password)   




    def compute_delay_histograms(self, paradigm, start_date, end_date,nbins):
        self.num_delay_histo_bins = nbins
        for stid, stn in self.station.iteritems():
            if stid in ['126N','127N','128N']: # remove after scale up

            #stn.sample_history_from_db(cursor, start_date, end_date)
            #stn.sample_history_from_db_parallel('mta_historical','mta_historical_small',start_date, end_date)
                stn.compute_delay_histograms(nbins,paradigm,start_date, end_date)

                print "complete station id: %s num_nonzero: %s" % (stid, np.count_nonzero(stn._delay_schedule))



    def compute_delay_state_diagrams(self, paradigm, start_date, end_date,nbins):
        self.num_delay_histo_bins = nbins
        for stid, stn in self.station.iteritems():
            if stid in ['126N','127N','128N']: # remove after scale up

            #stn.sample_history_from_db(cursor, start_date, end_date)
            #stn.sample_history_from_db_parallel('mta_historical','mta_historical_small',start_date, end_date)
                stn.compute_delay_state_diagram(paradigm, start_date, end_date,nbins)





    def _read_topology(self, topology):

        if isinstance(topology, Topology):

            for v in topology.vertices:
                self.station[v] = MTAStation(v)

            for e in topology.edges:
                self.station[e[0]].neighbor_stations.append(e[1])
                self.station[e[0]].child_stations.append(e[1])

            for e in topology.edges:
                self.station[e[1]].parent_stations.append(e[0])



    def save_snapshot(self):
        tmstmp = int(time.mktime(datetime.datetime.now().timetuple()))
        for stid, stn in self.station.iteritems():
            if stid in ['126N','127N','128N']: #remove after scale up
                stn.save_delay_histos(tmstmp)





    def save_delay_state_file(self):
        outputdict = {}


        ### OUTPUT VERTICES AND EDGES ###

        outputdict["V"] = []
        outputdict["E"] = []

        for stid, station in mta_system.station.iteritems():
            outputdict["V"].append(stid)
            edge_set = []
            for stid_e in station.neighbor_stations:
                edge_set.append([stid,stid_e])
            outputdict["E"].append(edge_set)


        outputdict["Vdata"] = {}

        for stid, station in mta_system.station.iteritems():
             outputdict["Vdata"][stid] = {}
             outputdict["Vdata"][stid]["numoutcomes"] = self.num_delay_histo_bins
             outputdict["Vdata"][stid]["vals"] = [x for x in xrange(0,self.num_delay_histo_bins)]
             outputdict["Vdata"][stid]["parents"] = self.parent_stations
             outputdict["Vdata"][stid]["children"] = self.child_stations

             ########## BUILD CPROB HERE #################

             outputdict["Vdata"][stid]["cprob"] = {}
             for d, day in enumerate(station.days):
                for h, hour in enumerate(station.hours):
                    for m, minute in enumerate(station.sample_points):
                        for k, v in self.delay_states[day][hour][minute].iteritems():
                            index = "['%s']['%s']['%s']" % (day, hour, minute)
                            for i, e in enumerate(k):
                                index += "['%s']" % e


                            outputdict["Vdata"][stid]["cprob"][index] = delay_states[day][hour][minute][k]



        print outputdict


    def discrete_bayesian(self):

        #

        pass





