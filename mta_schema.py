
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 24 08:02:02 2015

@author: Jared J. Thompson
"""
import sql_interface

def create_MTA_ETD_schema(cur, table_name):
    if table_exists(cur, table_name):
        print "table %s exists already." % table_name
        return None
    else:
        if cur.closed==False:
            cur.execute("CREATE TABLE %s( station_id varchar(20), destination_id varchar(20),\
            etd_sample real, delay real, sample_time timestamp);" % table_name)
        else:
            print 'cursor is closed.'

def create_MTA_schedule_schema(cur, table_name):
    if table_exists(cur, table_name):
        print "table %s exists already." % table_name
        return None
    else:
        if cur.closed==False:
            cur.execute("CREATE TABLE %s( trip_name varchar(30), trip_type varchar(30),\
            station_id varchar(30), destination_id varchar(30),\
            line_name_id varchar(40), departure_time interval);" % table_name)
        else:
            print 'cursor is closed.'
            
def update_MTA_schedule_schema(cur, table_name, input_v):
    if table_exists(cur, table_name):
        if cur.closed==False:            
            cur.execute("INSERT INTO %s( trip_name, trip_type, station_id, destination_id,\
            line_name_id, departure_time) VALUES ('%s', '%s', '%s', '%s', '%s', '%s');" % (table_name, input_v['trip_name'], input_v['trip_type'],\
                        input_v['station_id'],input_v['destination_id'],input_v['line_name_id'],input_v['departure_time']))
        else:
            print 'cursor is closed.'