from google.transit import gtfs_realtime_pb2
from google.protobuf import message
import nyct_subway_pb2
import urllib2
import sys, os
import datetime
import pytz
import json
import protobuf_to_json
from datetime_handlers import trip_day_from_timestamp
test_url = 'https://datamine-history.s3.amazonaws.com/gtfs-2014-09-17-09-46'


#a class should be implemented here


def parse_mta_api_to_json(target_url):
    try:
        feed = gtfs_realtime_pb2.FeedMessage()
        response = urllib2.urlopen(target_url)
        feed.ParseFromString(response.read())
        return json.loads(json.dumps(protobuf_to_json.pb2json(feed),\
         separators=(',',':')))
    except (StandardError, message.DecodeError), e:
        print "Cannot access JSON at time interval"
        print e
        return None

####highly unfinished####


def parse_mta_historical(json_feed, reference_timestamp=None):
    stop_list = []
    if json_feed and reference_timestamp!=None:
        for i, entity in enumerate(json_feed['entity']):
            if 'trip_update' in entity:
            # print "element: %s" % i
            # print len(entity['trip_update']['stop_time_update'])
            # print entity['trip_update']
            # print "train_id: %s" % entity['trip_update']['trip']['nyct_trip_descriptor']['train_id']
            # print "trip_id: %s" % entity['trip_update']['trip']['trip_id']
                if 'stop_time_update' in entity['trip_update']:

                    soonest_stop = entity['trip_update']['stop_time_update'][0]
                    if 'arrival' in soonest_stop:

                        soonest = soonest_stop['stop_id']

                        #print "stop_id: %s arrival time: %s" \
                       #% (soonest, int(soonest_stop['arrival']['time']))

                        entry = {'stop_id':soonest, 'trip_id': \
                        entity['trip_update']['trip']['trip_id'],\
                        'trip_type':trip_day_from_timestamp(reference_timestamp),\
                        'eta_sample':int(soonest_stop['arrival']['time']),\
                        'reference':int(reference_timestamp)}

                        #print entry

                        stop_list.append(entry)

        return stop_list
