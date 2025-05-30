#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cursor = openconnection.cursor()
    Ratings = "CREATE TABLE IF NOT EXISTS {DB} (UserID INT, movieID INT, Rating FLOAT)".format(DB=ratingstablename)
    cursor.execute(Ratings)
    openconnection.commit()
    with open(ratingsfilepath, "r") as file:
        for row in file:
            [userId, movieId, rating, timestamp] = row.split("::")
            cursor.execute('INSERT INTO %s VALUES (%s,%s,%s);' % (ratingstablename, userId, movieId, rating))
    openconnection.commit()

def rangePartition(ratingstablename, numberofpartitions, openconnection):
    conn = openconnection
    cursor = conn.cursor()
    dataBaseInstance = 'range_part'
    creation_query = "CREATE TABLE IF NOT EXISTS range_meta(partno INT, from_rat FLOAT, to_rat float)"
    cursor.execute(creation_query)
    for i in range(0, numberofpartitions):
        f = i * float(5 / numberofpartitions)
        t = (i + 1) * float(5 / numberofpartitions)
        database_name = dataBaseInstance + str(i)
        rangePartition = "CREATE TABLE IF NOT EXISTS {db} (UserID INT, movieID INT, Rating FLOAT)".format(db=database_name)
        cursor.execute(rangePartition)
        conn.commit()
        if (i == 0):
            insertRangeQuery = "INSERT INTO {db} select * from {r}  where {r}.rating BETWEEN {f} AND {t}  ".format(db=database_name,r=ratingstablename,f=f, t=t)
        else:
            insertRangeQuery = "INSERT INTO {db} select * from {r}  where {r}.rating > {f} AND {t} >= {r}.rating ".format(db=database_name,r=ratingstablename,f=f, t=t)
        cursor.execute(insertRangeQuery)
        conn.commit()
        insertQuery = "INSERT INTO range_meta VALUES ({partno},{f},{t})".format(partno=i, f=f, t=t)
        cursor.execute(insertQuery)
        conn.commit()

def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    cursor = openconnection.cursor()
    dataBaseInstance = 'rrobin_part'
    create_query = "CREATE TABLE IF NOT EXISTS rrobin_meta(partno INT, index INT)"
    cursor.execute(create_query)
    openconnection.commit()
    sql_create_query = "CREATE TABLE IF NOT EXISTS rrobin_temp (UserID INT, MovieID INT, Rating FLOAT, idx INT)"
    cursor.execute(sql_create_query)
    openconnection.commit()
    sql_insert_query = "INSERT INTO rrobin_temp (SELECT {DB}.UserID, {DB}.MovieID, {DB}.Rating , (ROW_NUMBER() OVER() -1) % {n} as idx from {DB})".format(n=str(numberofpartitions), DB=ratingstablename)
    cursor.execute(sql_insert_query)
    openconnection.commit()
    for i in range(0, numberofpartitions):
        create_round_robin_query = "CREATE TABLE IF NOT EXISTS {DB} (UserID INT, MovieID INT, Rating FLOAT)".format(DB=dataBaseInstance + str(i))
        cursor.execute(create_round_robin_query)
        openconnection.commit()
        insert_round_robin_query = "INSERT INTO {DB} select userid,movieid,rating from rrobin_temp where idx = {idx}".format(DB=dataBaseInstance + str(i), idx=str(i))
        cursor.execute(insert_round_robin_query)
        openconnection.commit()
    sql_meta_insert_query = "INSERT INTO rrobin_meta SELECT {N} AS partno, count(*) % {N} from {DB}".format(DB=ratingstablename, N=numberofpartitions)
    cursor.execute(sql_meta_insert_query)
    openconnection.commit()
    deleteTables('rrobin_temp', openconnection)
    openconnection.commit()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cursor = openconnection.cursor()
    cursor.execute( "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'rrobin_part%';")
    numberofpartitions = int(cursor.fetchone()[0])
    cursor.execute("SELECT COUNT (*) FROM " + ratingstablename + ";")
    maxRows = int(cursor.fetchone()[0])
    n = (maxRows) % numberofpartitions
    cursor.execute("INSERT INTO rrobin_part" + str(n) + " (UserID,MovieID,Rating) VALUES (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    cursor.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    conn = openconnection
    cursor = conn.cursor()
    select_query = "SELECT MIN(r.partno) FROM range_meta as r where r.from_rat <= {rat} and r.to_rat >= {rat} ".format(rat=rating)
    cursor.execute(select_query)
    conn.commit()
    part_number = cursor.fetchone()
    part_number_0= part_number[0]
    rate_insert_query = "Insert into {db} values ({u},{it},{r})".format(db=ratingstablename, u=userid, it=itemid, r=rating)
    cursor.execute(rate_insert_query)
    conn.commit()
    rate_insert_query = "Insert into range_part{i} values ({u},{it},{r})".format(i=part_number_0, u=userid, it=itemid, r=rating)
    cursor.execute(rate_insert_query)
    conn.commit()


def createDB(dbname='dds_assignment'):
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    finally:
        if cursor:
            cursor.close()
