#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'rangeratingspart%';")
        range_partition_tables = cursor.fetchall()
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'roundrobinratingspart%';")
        rr_partition_tables = cursor.fetchall()
        result_rows = []
        for table_tuple in range_partition_tables:
            table_name = table_tuple[0]
            partition_num = table_name.replace('rangeratingspart', '')
            partition_name = 'RangeRatingsPart{}'.format(partition_num)
            query = "SELECT '{0}', userid, movieid, rating FROM {1} WHERE rating >= {2} AND rating <= {3};".format(partition_name, table_name, ratingMinValue, ratingMaxValue)
            cursor.execute(query)
            rows = cursor.fetchall()
            result_rows.extend(rows)
        for table_tuple in rr_partition_tables:
            table_name = table_tuple[0]
            partition_num = table_name.replace('roundrobinratingspart', '')
            partition_name = 'RoundRobinRatingsPart{}'.format(partition_num)
            query = "SELECT '{0}', userid, movieid, rating FROM {1} WHERE rating >= {2} AND rating <= {3};".format(partition_name, table_name, ratingMinValue, ratingMaxValue)
            cursor.execute(query)
            rows = cursor.fetchall()
            result_rows.extend(rows)
        writeToFile('RangeQueryOut1.txt', result_rows)
    except Exception as e:
        print("Error in RangeQuery:", e)



def PointQuery(ratingsTableName, ratingValue, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'rangeratingspart%';")
        range_partition_tables = cursor.fetchall()
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'roundrobinratingspart%';")
        rr_partition_tables = cursor.fetchall()
        result_rows = []
        for table_tuple in range_partition_tables:
            table_name = table_tuple[0]
            partition_num = table_name.replace('rangeratingspart', '')
            partition_name = 'RangeRatingsPart{}'.format(partition_num)
            query = "SELECT '{0}', userid, movieid, rating FROM {1} WHERE rating = {2};".format(partition_name, table_name, ratingValue)
            cursor.execute(query)
            rows = cursor.fetchall()
            result_rows.extend(rows)
        for table_tuple in rr_partition_tables:
            table_name = table_tuple[0]
            partition_num = table_name.replace('roundrobinratingspart', '')
            partition_name = 'RoundRobinRatingsPart{}'.format(partition_num)
            query = "SELECT '{0}', userid, movieid, rating FROM {1} WHERE rating = {2};".format(partition_name, table_name, ratingValue)
            cursor.execute(query)
            rows = cursor.fetchall()
            result_rows.extend(rows)
        writeToFile('PointQueryOut1.txt', result_rows)
    except Exception as e:
        print("Error in PointQuery:", e)


def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
