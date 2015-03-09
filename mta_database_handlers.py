from sql_interface import *
import mta_schema
from time import strftime, strptime
import datetime
import psycopg2
import numpy as np

def sample_mta_historical(cur, table_name, point_at, startdate, enddate):

    from datetime import date
    from dateutil.rrule import rrule, DAILY
    from datetime_handlers import timestamp_from_timept, trip_day_from_timestamp
    from MTA_API_lib import parse_mta_api_to_json, parse_mta_historical

    hourtypes = ['00','01','02','03','04','05','06','07','08','09','10','11',\
    '12','13','14','15','16','17','18','19','20','21','22','23']

    sample_points = ['01','06','11','16','21','26',\
    '31','36','41','46','51','56']

    if table_exists(cur, table_name):

        s = startdate.split('-')
        startyr = int(s[0])
        startmo = int(s[1])
        startday = int(s[2])

        e = enddate.split('-')
        endyr = int(e[0])
        endmo = int(e[1])
        endday = int(e[2])

        a = date(startyr, startmo, startday)
        b = date(endyr, endmo, endday)

        for dt in rrule(DAILY, dtstart=a, until=b):
            print 'accessing: %s' % dt.strftime("%Y-%m-%d")
            #curr_date = strftime("%Y-%m-%d", date.timetuple())
            for hour in hourtypes:
                for point in sample_points:
                    timept = '-%s-%s' % (hour, point)
                    fulltime = dt.strftime("%Y-%m-%d")+timept
                    target_url = point_at+'gtfs-'+fulltime

                    print 'accessing: %s' % target_url

                    #create reference timestamp
                    reference_timestamp = timestamp_from_timept(fulltime,\
                     'US/Eastern')

                    history = parse_mta_historical(\
                        parse_mta_api_to_json(target_url),reference_timestamp)

                    if np.any(history):
                        for i, entry in enumerate(history):
                            update_mta_eta_schema(cur, table_name, entry)
                    else:
                        fname = '%s_failed_samples.txt' % table_name
                        with open(fname, 'a+') as errf:
                            errf.write("%s\n" % fulltime)
        errf.close()

###############################################################################

def query_mta_historical_closest_train(cur, table_name, station_id, sample_tstamp, padding):
    if table_exists(cur, table_name):
        if cur.closed==False:
            #return the trains at the station closest to timestamp. 

            query = "SELECT * FROM %s WHERE reference BETWEEN %s AND %s AND stop_id='%s';" % (table_name, sample_tstamp, sample_tstamp+padding, station_id)

           #print query

            cur.execute(query)
            response = cur.fetchone()
            if not response:
                return []
            return  response['eta_sample']
        else:
            print 'cursor is closed.'
    else:
        print 'Query MTA historical: Table is closed'

###############################################################################

def create_mta_eta_schema(cur, table_name):

    if table_exists(cur, table_name):
        print "table %s exists already." % table_name
        return None
    else:
        if cur.closed==False:
            cur.execute("CREATE TABLE %s( stop_id varchar(20), \
                trip_type varchar(5), trip_id varchar(20), \
                eta_sample bigint, reference bigint);" % table_name)
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
            return  cur.fetchall()
        else:
            print 'cursor is closed.'
    else:
        print 'cursor is closed.'

def size_mta_eta_schema(cur, table_name):
    if table_exists(cur, table_name):
        if cur.closed==False:
            cur.execute("SELECT \
                pg_size_pretty(pg_database_size(current_database())) \
                AS human_size;")
            return  cur.fetchall()
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


def create_mta_schedule_schema(cur, table_name):

    if table_exists(cur, table_name):
        print "table %s exists already." % table_name
        return None
    else:
        if cur.closed==False:
            cur.execute("CREATE TABLE %s( trip_name varchar(30), \
            trip_type varchar(30), station_id varchar(30), \
            destination_id varchar(30),\
            line_name_id varchar(40), departure_time interval);" % table_name)
        else:
            print 'cursor is closed.'
            
def update_mta_schedule_schema(cur, table_name, input_v):

    if table_exists(cur, table_name):
        if cur.closed==False:            
            cur.execute("INSERT INTO %s( trip_name, trip_type, station_id, \
                destination_id,line_name_id, departure_time) VALUES \
            ('%s', '%s', '%s', '%s', '%s', '%s');" \
            % (table_name, input_v['trip_name'], input_v['trip_type'],\
                input_v['station_id'],input_v['destination_id'],
                input_v['line_name_id'],input_v['departure_time']))
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