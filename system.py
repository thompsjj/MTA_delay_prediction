# -*- coding: utf-8 -*-
"""
Created on Mon Mar 2 17:32:02 2015

@author: Jared Thompson 

"""


# The system structure owns all lines, stations and trips, 
# and it owns all update methods from the top down. 

#The system structure is capable of returning a clean list (JSON or dict) of 
# nodes and data belonging to those nodes. 
# This is then pipelined to the libpgm model.

# System reads from updates cleanly but also needs a topology file for 
# lines that have contact through stations. 

# In the case of MTA it may be important to join nodes conditionally on track
# relationships alone. In this way, the topology of the network is in some
# sense changing. However, the way the MTA is designed, they avoid node
# congestion in general, as lines do not change tracks and run along their own
# course. In addition, MTA has a very large number of available tracks. 



class System(object):

    def __init__(self,topofile):
        self.num_lines = 0
        self.num_stations = 0
        try:
            self.topology = self.read_topology(topofile)
        except StandardError e:
            print e


    def read_topology(self, filename):
        # this constructs the topology of the network from a topofile. 
        pass

    def construct_network(self):
        if 


