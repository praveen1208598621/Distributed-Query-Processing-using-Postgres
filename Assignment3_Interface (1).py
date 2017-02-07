#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'table1'
SECOND_TABLE_NAME = 'table2'
SORT_COLUMN_NAME_FIRST_TABLE = 'column1'
SORT_COLUMN_NAME_SECOND_TABLE = 'column2'
JOIN_COLUMN_NAME_FIRST_TABLE = 'column1'
JOIN_COLUMN_NAME_SECOND_TABLE = 'column2'
##########################################################################################################

range_partition_index = 0
max_rating = 5.0
threadname = "thread"
#threadLock = threading.Lock()
numPartitions = 5
PARTITION_NAME = "range_part"
JOIN_PARTITION_NAME = "join_range_part"

def rangepartition(min_val, max_val, partitionName,tablename, numberofpartitions, SortingColumnName, openconnection):
    if not isinstance(numberofpartitions, int) and numberofpartitions <= 1:
        print "Invalid no of partitions entered"
        return

    cur = openconnection.cursor()

    minSortingCol = "SELECT MIN("+SortingColumnName+") FROM "+tablename

    cur.execute(minSortingCol)
    min_val1 = cur.fetchone()[0]
    if min_val == min_val1:
        ins = "CREATE TABLE IF NOT EXISTS "+partitionName+" AS SELECT * FROM "+tablename+" WHERE "+SortingColumnName+" >= "+str(min_val)+" AND "+SortingColumnName+"<=" \
              +str(max_val) +" ORDER BY "+SortingColumnName
    else:
        ins = "CREATE TABLE IF NOT EXISTS "+partitionName+" AS SELECT * FROM "+tablename+" WHERE "+SortingColumnName+" >"+str(min_val)+" AND "+SortingColumnName+"<=" \
              +str(max_val) +" ORDER BY "+SortingColumnName

    print ins
    cur.execute(ins)
    openconnection.commit()
    print "Range partition of table is successful"

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    cur = openconnection.cursor()
    global numPartitions
    count = numPartitions
    maxSortingCol = "SELECT MAX(" + SortingColumnName + ") FROM " + InputTable

    cur.execute(maxSortingCol)
    max_val = cur.fetchone()[0]
    # print "Max value on sorting col is " + str(max_val)
    minSortingCol = "SELECT MIN(" + SortingColumnName + ") FROM " + InputTable

    cur.execute(minSortingCol)
    min_val = cur.fetchone()[0]
    # print "Min val on sorting col is " + str(min_val)
    size = float(max_val - min_val) / numPartitions
    partitionNames = []
    threads = [0, 0, 0, 0, 0]
    i=0
    try:
        while count:
            name = PARTITION_NAME + str(i+1)

            if i == 0:
                t = threading.Thread(target=rangepartition, args=(min_val, (min_val+size), name, InputTable, numPartitions, SortingColumnName, openconnection,))
            elif i != (numPartitions-1):
                t = threading.Thread(target=rangepartition, args=((min_val + i*size), ((min_val + (i+1)*size)), name, InputTable, numPartitions, SortingColumnName, openconnection,))
            else:
                t = threading.Thread(target=rangepartition, args=((min_val + i * size), max_val, name, InputTable, numPartitions, SortingColumnName, openconnection,))
            t.start()
            threads[i] = t
            partitionNames.append(name)
            i += 1
            count -= 1
        #join all threads
        for t in threads:
            t.join()

        #create output table
        cmd1 = "CREATE TABLE IF NOT EXISTS "+OutputTable+" AS SELECT * FROM "+InputTable
        print cmd1
        cur.execute(cmd1)

        cmd2 = "TRUNCATE TABLE "+OutputTable
        print cmd2
        cur.execute(cmd2)

        for l in partitionNames:
            cmd3 = "INSERT INTO "+OutputTable+" SELECT * FROM "+l
            print cmd3
            cur.execute(cmd3)

        openconnection.commit()

    except Exception as error:
        print error
    pass #Remove this once you are done with implementation


def Join(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    cur = openconnection.cursor()
    cmd = "INSERT INTO " + OutputTable + " SELECT * FROM " + InputTable1 + " , " + InputTable2 + " WHERE " + InputTable1 + "." + Table1JoinColumn + " = " + InputTable2 + "." + Table2JoinColumn
    print cmd
    cur.execute(cmd)
    openconnection.commit()
    print "Join function is success"


def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    global numPartitions
    cur = openconnection.cursor()

    count = numPartitions

    maxSortingCol = "SELECT MAX(" + Table1JoinColumn + ") FROM " + InputTable1
    minSortingCol = "SELECT MIN(" + Table1JoinColumn + ") FROM " + InputTable1

    cur.execute(maxSortingCol)
    max_val1 = cur.fetchone()[0]
    cur.execute(minSortingCol)
    min_val1 = cur.fetchone()[0]
    print "Max val1 is :"+str(max_val1)
    print "Min val1 is :" + str(min_val1)


    maxSortingCol2 = "SELECT MAX(" + Table2JoinColumn + ") FROM " + InputTable2
    minSortingCol2 = "SELECT MIN(" + Table2JoinColumn + ") FROM " + InputTable2

    cur.execute(maxSortingCol2)
    max_val2 = cur.fetchone()[0]
    cur.execute(minSortingCol2)
    min_val2 = cur.fetchone()[0]
    print "Max val2 is :" + str(max_val2)
    print "Min val2 is :" + str(min_val2)

    if max_val1 > max_val2:
        max_val = max_val1
    else:
        max_val = max_val2

    if min_val2 > min_val1:
        min_val = min_val1
    else:
        min_val = min_val2

    size = float(max_val - min_val) / numPartitions
    partitionNames1 = []
    partitionNames2 = []
    threads1 = [0, 0, 0, 0, 0]
    threads2 = [0, 0, 0, 0, 0]
    i = 0
    try:
        while count:
            name = JOIN_PARTITION_NAME + "1" + str(i + 1)
            if i == 0:
                t = threading.Thread(target=rangepartition, args=(min_val, (min_val + size), name, InputTable1, numPartitions, Table1JoinColumn, openconnection,))
            elif i != (numPartitions-1):
                t = threading.Thread(target=rangepartition, args=((min_val + i * size), ((min_val + (i + 1) * size)), name, InputTable1, numPartitions, Table1JoinColumn, openconnection,))
            else:
                t = threading.Thread(target=rangepartition, args=((min_val + i * size), max_val, name, InputTable1, numPartitions, Table1JoinColumn, openconnection,))
            t.start()
            threads1[i] = t
            partitionNames1.append(name)
            i += 1
            count -= 1
        for t in threads1:
            t.join()

        i=0
        count = numPartitions
        while count:
            name = JOIN_PARTITION_NAME + "2" + str(i + 1)
            if i == 0:
                t = threading.Thread(target=rangepartition, args=(min_val, (min_val + size), name, InputTable2, numPartitions, Table2JoinColumn, openconnection,))
            elif i != (numPartitions-1):
                t = threading.Thread(target=rangepartition, args=((min_val + i * size), ((min_val + (i + 1) * size)), name, InputTable2, numPartitions, Table2JoinColumn, openconnection,))
            else:
                t = threading.Thread(target=rangepartition, args=((min_val + i * size), max_val, name, InputTable2, numPartitions, Table2JoinColumn, openconnection,))
            t.start()
            threads2[i] = t
            partitionNames2.append(name)
            i += 1
            count -= 1
        for t in threads2:
            t.join()

        threads3 = [0, 0, 0, 0, 0]
        D1 = []

        #create output table
        cmd1 = "SELECT column_name  FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name ='" + InputTable1 + "'"
        print cmd1
        cur.execute(cmd1)
        col1 = cur.fetchall()
        for col in col1:
            D1.append(InputTable1 +"_"+col[0])
        cmd2 = "SELECT column_name  FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name ='" + InputTable2 + "'"
        print cmd2
        cur.execute(cmd2)
        col2 = cur.fetchall()
        for col in col2:
            D1.append(InputTable2 + "_" +col[0])

        colList = ""
        for col in D1:
            colList += col+","
        colList = colList[:-1]

        cmd = "CREATE TABLE IF NOT EXISTS " + OutputTable + "("+ colList +") AS SELECT * FROM " + InputTable1 + " , " + InputTable2 + " WHERE 5 > 10"
        print cmd
        cur.execute(cmd)

        #perform join parallelly
        if len(partitionNames2) == len(partitionNames1):
            for k in range(0, len(partitionNames1)):
                t = threading.Thread(target=Join, args=(partitionNames1[k], partitionNames2[k], Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection,))
                t.start()
                threads3[k] = t
            for t in threads3:
                t.join()
            print "Parallel join successfull"
        else:
            print "No of partitions to join are not equal"

        openconnection.commit()
    except Exception as error:
        print error

    pass # Remove this once you are done with implementation


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
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
    con.commit()
    con.close()

# Donot change this function
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
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    try:
        # Creating Database ddsassignment2
        print "Creating Database named as ddsassignment2"
        createDB()

        # Getting connection to the database
        print "Getting connection from the ddsassignment2 database"
        con = getOpenConnection()

        # Calling ParallelSort
        print "Performing Parallel Sort"
        ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con)

        # Calling ParallelJoin
        print "Performing Parallel Join"
        ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con)

        # Saving parallelSortOutputTable and parallelJoinOutputTable on two files
        saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con)
        saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con)

        # Deleting parallelSortOutputTable and parallelJoinOutputTable
        deleteTables('parallelSortOutputTable', con)
        deleteTables('parallelJoinOutputTable', con)

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
