

class schedule_table(object):
    def __init__(self, name):
        self.name = name
        self.loaded = False

    def build(self, stoptimes, stops):

        '''
        Builds schedule from input file names (in google transit folder)

        INPUT: string (filename), string (filename)
        OUTPUT: None
        '''

        stop_times = open(stoptimes, "r+")
        stop_info = open(stops, "r+")


        # Create stops
        stops = {}
        for line in stop_info:
            # print line
            l = line.split(",")
            stop_id = l[0]
            name = l[2]
            lat = l[4]
            lon = l[5]
            stop = {'name' : name, 'lat': lat, 'lon': lon}
            self.stops[stop_id] = stop
            # print type(stops)

            # Create arrivals
        self.ids = []
        self.arrivals = {}
        for line in stop_times:
            l = line.split(",")
            time = l[1]
            stop_id = l[3]
            if stop_id not in self.ids:
                self.ids.append(stop_id)

            self.arrivals.setdefault(stop_id,[]).append(time)


        self.table = {}
        for stop_id in self.ids:
            self.table [stop_id] = \
            {'name' : self.stops[stop_id]['name'],\
             'lat': self.stops[stop_id]['lat'], \
             'lon': self.stops[stop_id]['lon'],\
             'arrivals': self.arrivals[stop_id]}


     def get_station(self, req):

        '''
        Returns the name and gis of a chosen station id

        INPUT: string (station_id)
        OUTPUT: dict (name, GIS)
        '''

        return {'name': self.data[req]['name'],\
                'lat': self.data[req]['lat'], \
                'lon': self.data[req]['lon']}


     def get_stations(self):

       '''
        Returns a list of all stations present in the schedule

        INPUT: None
        OUTPUT: List (id, name)
        '''
        stations = []
        for i, req in enumerate(self.ids):
            stations.append({'id':st_id, 'name':self.data[req]['name']})

        return stations

    def get_trains(self):

       '''
        Returns a list of all trains that are stopping right now (or soon)

        INPUT: None
        OUTPUT: List (id, name)
        '''

        pass

