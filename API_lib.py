from google.transit import gtfs_realtime_pb2
import nyct_subway_pb2
import urllib2
import sys, os
import datetime
import pytz
from pytz import timezone
utc = pytz.utc
eastern = timezone('US/Eastern')


test_url = 'https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-46'


#a class should be implemented here



def parse_MTA_API_to_JSON(target_url):
    feed = gtfs_realtime_pb2.FeedMessage()
    response = urllib2.urlopen(target_url)
    feed.ParseFromString(response.read())
    return json.loads(json.dumps(protobuf_to_json.pb2json(feed), separators=(',',':')))

####highly unfinished####


def parse_MTA_historical():
    stop_ids = []
    for i, entity in enumerate(feed_json['entity']):
        if 'trip_update' in entity:
            print "element: %s" % i
            #print len(entity['trip_update']['stop_time_update'])
            #print entity['trip_update']
            print "train_id: %s" % entity['trip_update']['trip']['nyct_trip_descriptor']['train_id']
            print "trip_id: %s" % entity['trip_update']['trip']['trip_id']   

            soonest_stop = entity['trip_update']['stop_time_update'][0]
            if 'arrival' in soonest_stop:
                print soonest_stop['stop_id']
                print "arrival time: %s" % datetime.datetime.fromtimestamp(int(soonest_stop['arrival']['time'])).strftime('%Y-%m-%d %H:%M:%S')
                print '\n\n'