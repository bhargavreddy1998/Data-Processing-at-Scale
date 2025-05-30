import psycopg2
import os
import sys
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    result = []
    cursor = openconnection.cursor()
    query = '''SELECT partitionnum FROM rangeratingsmetadata WHERE maxrating>={0} AND minrating<={1};'''.format(ratingMinValue, ratingMaxValue)
    cursor.execute(query)
    partitions = cursor.fetchall()
    partitions = [partition[0] for partition in partitions]
    selectQuery = '''SELECT * FROM rangeratingspart{0} WHERE rating>={1} and rating<={2};'''
    for partition in partitions:
        cursor.execute(selectQuery.format(partition, ratingMinValue, ratingMaxValue))
        sqlresult = cursor.fetchall()
        for res in sqlresult:
            res = list(res)
            res.insert(0,'RangeRatingsPart{}'.format(partition))
            result.append(res)
    roundRobinCountQuery = '''SELECT partitionnum FROM roundrobinratingsmetadata;'''
    cursor.execute(roundRobinCountQuery)
    roundRobinParts = cursor.fetchall()[0][0]
    roundRobinSelectQuery = '''SELECT * FROM roundrobinratingspart{0} WHERE rating>={1} and rating<={2};'''
    for i in xrange(0,roundRobinParts):
        cursor.execute(roundRobinSelectQuery.format(i, ratingMinValue, ratingMaxValue))
        sqlresult = cursor.fetchall()
        for res in sqlresult:
            res = list(res)
            res.insert(0, 'RoundRobinRatingsPart{}'.format(i))
            result.append(res)

    writeToFile('RangeQueryOut.txt', result)

def PointQuery(ratingsTableName, ratingValue, openconnection):
    result = []
    cursor = openconnection.cursor()
    selectQuery = '''SELECT partitionnum FROM rangeratingsmetadata WHERE maxrating>={0} AND minrating<={0};'''.format(ratingValue)
    cursor.execute(selectQuery)
    partitions = cursor.fetchall()
    partitions = [partition[0] for partition in partitions]
    rangeSelectQuery = '''SELECT * FROM rangeratingspart{0} WHERE rating={1};'''
    for partition in partitions:
        cursor.execute(rangeSelectQuery.format(partition, ratingValue))
        sqlresult = cursor.fetchall()
        for res in sqlresult:
            res = list(res)
            res.insert(0, 'RangeRatingsPart{}'.format(partition))
            result.append(res)
    roundRobinCountQuery = '''SELECT partitionnum FROM roundrobinratingsmetadata;'''
    cursor.execute(roundRobinCountQuery)
    roundRobinParts = cursor.fetchall()[0][0]
    roundRobinSelectQuery = '''SELECT * FROM roundrobinratingspart{0} WHERE rating={1};'''
    for i in xrange(0, roundRobinParts):
        cursor.execute(roundRobinSelectQuery.format(i, ratingValue))
        sqlresult = cursor.fetchall()
        for res in sqlresult:
            res = list(res)
            res.insert(0, 'RoundRobinRatingsPart{}'.format(i))
            result.append(res)
    writeToFile('PointQueryOut.txt', result)

def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()