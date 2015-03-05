# -*- coding: utf-8 -*-
"""
Created on Tue Feb 24 08:02:02 2015

@author: Jared J. Thompson
"""

import psycopg2

def connect_to_db(name, user, host, password ):
    try:
        conn = psycopg2.connect(database=name, user=user, host=host, password=password)
        conn.autocommit=True
    except:
        print "Unable to connect to the database"

    cursor = conn.cursor()
    cursor.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'""")
    for table in cursor.fetchall():
        print(table)

    return cursor, conn

def connect_to_local_db(name, user, password='user'):
    try:
        conn = psycopg2.connect(database=name, user=user, host='localhost', password=password)
        conn.autocommit=True
    except:
        print "Unable to connect to the database"
    

    cursor = conn.cursor()
    cursor.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'""")
    for table in cursor.fetchall():
        print(table)

    return cursor, conn

def table_exists(cur, table_str):
    exists = False
    if cur.closed==True:
        print "cursor is closed."
        return False
    try:
        cur.execute("SELECT EXISTS(SELECT relname FROM pg_class WHERE relname='" + table_str + "')")
        exists = cur.fetchone()[0]
    except psycopg2.Error as e:
        print e
    return exists
    
def drop_table(cur, table_name):
    if table_exists(cur, table_name):
        if cur.closed==False:            
            cur.execute("DROP TABLE %s" % (table_name))
        else:
            print 'cursor is closed.'