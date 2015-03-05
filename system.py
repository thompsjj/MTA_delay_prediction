# -*- coding: utf-8 -*-
"""
Created on Mon Mar 2 17:32:02 2015

@author: Jared Thompson 

"""
from topology import Topology
from collections import defaultdict
from station import Station
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

    def build(self, topology, schedule):

        try:
            self._read_topology(topology)
            self._populate_stations(schedule)
            self.isBuilt = True
        except StandardError, e:
            print e

    def _read_topology(self, topology):

        if isinstance(topology, Topology):

            for v in topology.vertices:
                self.station[v] = Station(v)

            for e in topology.edges:
                self.station[e[0]].neighbor_stations.append(e[1])

    def _populate_stations(self, schedule):

        '''This function takes a station id and sets its schedule for all
        trains and trips for every day that belong to this station'''

        for stid, Stn in self.station.iteritems():
            Stn.set_schedule(schedule.table[stid]['arrivals'])

    def __repr__(self):
        print self.isBuilt




