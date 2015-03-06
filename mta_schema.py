
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 24 08:02:02 2015

@author: Jared J. Thompson
"""


def create_mta_eta_schema(cur, table_name):

    if table_exists(cur, table_name):
        print "table %s exists already." % table_name
        return None
    else:
        if cur.closed==False:
            cur.execute("CREATE TABLE %s( stop_id varchar(20), \
                trip_type varchar(5) trip_id varchar(20), \
                eta_sample timestamp, reference timestamp);" % table_name)
        else:
            print 'cursor is closed.'

def drop_mta_eta_schema(cur, table_name):
    if table_exists(cur, table_name):
        if cur.closed==False:
            cur.execute("DROP TABLE %s;" % table_name)
        else:
            print 'cursor is closed.'
    else:
        print 'cursor is closed.'


def check_mta_eta_schema(cur, table_name, nrows):
    if table_exists(cur, table_name):
        if cur.closed==False:
            cur.execute("SELECT * FROM %s LIMIT %s;" % (table_name, nrows))
        else:
            print 'cursor is closed.'
    else:
        print 'cursor is closed.'





def update_mta_eta_schema(cur, table_name, input_v):

    if table_exists(cur, table_name):
        if cur.closed==False:

            cur.execute("INSERT INTO %s \
                ( stop_id, trip_type, trip_id, eta_sample, reference) \
                VALUES ('%s', '%s', '%s', '%s', '%s');" % \
            (table_name, input_v['stop_id'], input_v['trip_type'],\
            input_v['trip_id'],input_v['eta_sample'], \
            input_v['reference']))
        else:
            print 'cursor is closed.'


def get_next_mta_trains_from_schema(cur, table_name, current_time, \
station_id, destination_id,trip_type, range_t):
    '''
    INPUT: cursor, string, interval
    OUTPUT: interval
    '''
    #given the current time, construct the time into an interval
    if table_exists(cur, table_name):
        if cur.closed==False:     
            cur.execute('''SELECT * FROM %s WHERE station_id='%s' \
            AND destination_id='%s' AND trip_type='%s' AND \
            departure_time BETWEEN '%s' and '%s' LIMIT 10''' % (table_name,\
            station_id, destination_id, trip_type, range_t[0],range_t[1]))
            return cur.fetchall()
        else:
            print 'cursor is closed.'
            
def get_first_mta_train_from_schema(cur, table_name, station_id,\
    destination_id,trip_type):
    '''
    INPUT: cursor, string, interval
    OUTPUT: interval
    '''
    if table_exists(cur, table_name):
        if cur.closed==False:     
            cur.execute('''
            WITH times AS(SELECT trip_name, departure_time FROM %s\
                WHERE station_id='%s'\
                AND destination_id='%s'\
                AND trip_type='%s')

            SELECT trip_name, departure_time FROM times \
            GROUP BY trip_name, departure_time\
            HAVING departure_time = (SELECT MIN(departure_time) FROM times);
            
            ''' % (table_name, station_id, destination_id, trip_type))
            return cur.fetchall()
        else:
            print 'cursor is closed.'



'''def create_mta_schedule_schema(cur, table_name):
    if table_exists(cur, table_name):
        print "table %s exists already." % table_name
        return None
    else:
        if cur.closed==False:
            cur.execute("CREATE TABLE %s( trip_name varchar(30), \
            trip_type varchar(30), station_id varchar(30), \
            destination_id varchar(30), line_name_id varchar(40), \
            departure_time interval);" % table_name)
        else:
            print 'cursor is closed.'
            
def update_mta_schedule_schema(cur, table_name, input_v):
    if table_exists(cur, table_name):
        if cur.closed==False:            
            cur.execute("INSERT INTO %s( trip_name, trip_type, station_id, \
                destination_id, line_name_id, departure_time) VALUES ('%s', \
                '%s', '%s', '%s', '%s', '%s');" % \
            (table_name, input_v['trip_name'], input_v['trip_type'],\
            input_v['station_id'],input_v['destination_id'], \
            input_v['line_name_id'],input_v['departure_time']))
        else:
            print 'cursor is closed.'
'''