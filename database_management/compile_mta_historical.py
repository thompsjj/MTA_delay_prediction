import os, sys

def main(argv):

    from mta_database_handlers import sample_mta_historical, \
    create_mta_eta_schema, drop_mta_eta_schema, check_mta_eta_schema,\
    size_mta_eta_schema

    from sql_interface import connect_to_local_db
    
    try:
        cursor, conn = connect_to_local_db('mta_historical','postgres')
    except:
        print "a serious error has occurred with connection."
        sys.exit(0)

    drop_mta_eta_schema(cursor, 'mta_historical_small')


    create_mta_eta_schema(cursor, 'mta_historical_small')
    sample_mta_historical(cursor, 'mta_historical_small', \
     'https://datamine-history.s3.amazonaws.com/', '2014-11-23', '2015-01-30')

    print check_mta_eta_schema(cursor, 'mta_historical_small', 10)
    print size_mta_eta_schema(cursor, 'mta_historical_small')

    cursor.close()
    conn.close()

if __name__ == '__main__':
    main(sys.argv)
