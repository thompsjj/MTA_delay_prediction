from collections import defaultdict
import sys, os

class nd_node(object):
    ''' directionless (non-directed) node'''
    def __init__(self, name):
        self.name = name
        self.neighbors = []


class Topology(object):

    def __init__(self):
        self.num_nodes = 0
        self.vertices = []
        self.edges = []
        self.nodes = defaultdict(dict)
        self.routes = defaultdict(lambda : defaultdict(list))

    def add_mta_route(self, route, routename):
        '''This function is specific to mta outputs. It reads a list of 
        unordered mta stations and builds a topology from it. It then
        goes over the station name and links neighboring stations (for later)'''

        # MTA data is directional and only runs north to south

        def sort_N(z):
            if len(z['id']) > 3:
                if z['id'][3] == 'N':
                    return z

        def sort_S(z):
            if len(z['id']) > 3:
                if z['id'][3] == 'S':
                    return z

        NB_route = [sort_N(z) for z in sorted(route, key=lambda k: k['id'],\
         reverse=True) if sort_N(z)]
        SB_route = [sort_S(z) for z in sorted(route, key=lambda k: k['id']) \
        if sort_S(z)]

        self.routes[routename]['nb'] = NB_route
        self.routes[routename]['sb'] = SB_route

        #calculate joins here for later

        self._compile_network_within_routes()



    def _compile_network_within_routes(self):
        for routename, route in self.routes.iteritems():
            for direction, stations in route.iteritems():

                self._route_edges(stations, self.edges)

                for station in stations:
                    if station not in self.vertices:
                        self.vertices.append(station['id'])

                

    def _route_edges(self, stations, output_list):
        if len(stations)==2:
            return output_list.append([stations[0]['id'],stations[1]['id']])
        else:
            output_list.append([stations[0]['id'],stations[1]['id']])
            return self._route_edges(stations[1:], output_list)





