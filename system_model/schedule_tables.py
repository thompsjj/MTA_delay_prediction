import sys, os
from collections import defaultdict


class schedule_table(object):
    def __init__(self):
        self.name = ''
        self.loaded = False

    def build(self, stoptimes, stops):

        '''
        Builds schedule from input file names (in google transit folder)

        INPUT: string (filename), string (filename)
        OUTPUT: None
        '''

        #print stoptimes
        #print stops

        stop_times = open(stoptimes, "r+")
        stop_info = open(stops, "r+")


        # Create stops
        self.stops = defaultdict()
        #self.routes = defaultdict(list)
        for line in stop_info:
            #print line
            l = line.split(",")
            stop_id = l[0]
            name = l[2]
            lat = l[4]
            lon = l[5]
            route = stop_id[0]
            entry = {'id':stop_id, 'name' : name, 'lat': lat, 'lon': lon}
            self.stops[stop_id] = entry
            #self.routes[route].append(entry)
            #print self.stops[stop_id]

        # Create arrivals
        self.ids = []
        self.arrivals = defaultdict(list)
        for line in stop_times:
            l = line.split(",")
            time = l[1]
            stop_id = l[3]
            if stop_id not in self.ids:
                self.ids.append(stop_id)
            self.arrivals[stop_id].append(time)

        # Create table
        self.table = defaultdict()
        for i, stop_id in enumerate(self.ids):
            entry = {'name' : self.stops[stop_id]['name'],\
             'lat': self.stops[stop_id]['lat'], \
             'lon': self.stops[stop_id]['lon'],\
             'arrivals': self.arrivals[stop_id]}
            self.table[stop_id] = entry

    def get_station(self, req):

        '''
        Returns the name and gis of a chosen station id

        INPUT: string (station_id)
        OUTPUT: dict (name, GIS)
        '''
        return {'id': req, 'name': self.table[req]['name'],\
                'lat': self.table[req]['lat'], \
                'lon': self.table[req]['lon'], \
                 'arrivals': self.table[req]}


    def get_stations(self):
        '''
        Returns a list of all stations present in the schedule

        INPUT: None
        OUTPUT: List (id, name)
        '''
        stations = []
        for i, req in enumerate(self.ids):
            stations.append({'id':req, 'name':self.table[req]['name']})

        return stations


class mta_route_schedule(schedule_table):
    def __init__(self):
        super(schedule_table, self).__init__()
        self.loaded = False

    def build(self, stoptimes, stops):

        '''
        Builds schedule from input file names (in google transit folder)

        INPUT: string (filename), string (filename)
        OUTPUT: None
        '''

        #print stoptimes
        #print stops

        print stoptimes
        print stops


        stop_times = open(stoptimes, "r+")
        stop_info = open(stops, "r+")


        # Create stops
        self.stops = defaultdict()
        self.routes = defaultdict(list)
        for line in stop_info:
            #print line
            l = line.split(",")
            stop_id = l[0]
            name = l[2]
            lat = l[4]
            lon = l[5]
            route = stop_id[0]

            entry = {'id':stop_id, 'name' : name, 'lat': lat, 'lon': lon}
            self.stops[stop_id] = entry
            self.routes[route].append(entry)


        # Create arrivals
        self.ids = []
        self.arrivals = defaultdict(lambda :defaultdict(list))
        for line in stop_times:
            l = line.split(",")
            day = l[6].strip()
            arrival_time = l[2].split(' ')[1]
            stop_id = l[4]

            if stop_id not in self.ids:
                self.ids.append(stop_id)
            self.arrivals[stop_id][day].append(arrival_time)

        # Create table
        self.table = defaultdict()
        for i, stop_id in enumerate(self.ids):
            entry = {'name' : self.stops[stop_id]['name'],\
             'lat': self.stops[stop_id]['lat'], \
             'lon': self.stops[stop_id]['lon'],\
             'arrivals': self.arrivals[stop_id]}
            self.table[stop_id] = entry

    def get_route(self, req):
        return self.routes[req]

    def __repr__(self):
        return get_stations()


