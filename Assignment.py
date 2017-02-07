#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
import fileinput

range_partition_index = 0
rr_partition_index = 0
max_rating = 5.0
min_rating = 0.0
DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

def processfile(filepath):
    for line in fileinput.input(filepath, inplace=True):
        x, y, z, rest = line.split('::', 3)
        print ','.join((x, y, z))
    print "Process file is success"

def loadratings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    try:
        file_handle = open(ratingsfilepath, 'r')
        cur.execute("CREATE TABLE " + ratingstablename + "(UserID integer NOT NULL, dummy1 char, MovieID integer NOT NULL, dummy2 char, Rating decimal, dummy3 char, timestamp integer)")
        #cmd = "COPY "+ratingstablename+" FROM '"+ratingsfilepath+"' DELIMITER ':'"
        cur.copy_from(file_handle, ratingstablename, ':')
        # cur.execute(cmd)
        cur.execute("ALTER TABLE "+ratingstablename+ " DROP COLUMN dummy1")
        cur.execute("ALTER TABLE " + ratingstablename + " DROP COLUMN dummy2")
        cur.execute("ALTER TABLE " + ratingstablename + " DROP COLUMN dummy3")
        cur.execute("ALTER TABLE " + ratingstablename + " DROP COLUMN timestamp")
        # openconnection.commit()
        print "Loading into table is successful"
        file_handle.close()
    except Exception as error:
        print error;


def update_metadata(partitionname, minval, maxval, openconnection):
    #print "Inside update_metadata funtion :"
    #print "************************************"
    cmd = "CREATE TABLE IF NOT EXISTS meta_data (pname varchar(255), startvalue float, endval float)"
    #print cmd
    cur = openconnection.cursor()
    cur.execute(cmd)
    query = "INSERT INTO meta_data VALUES('" + str(partitionname) + "' ,'"+str(minval)+ "' , '"+str(maxval)+"')"
    print query
    cur.execute(query)
    pass

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    if not isinstance(numberofpartitions, int) and numberofpartitions <= 1:
        print "Invalid no of partitions entered"
        return

    cur = openconnection.cursor()
    global range_partition_index
    cur.execute("SELECT MAX(rating) FROM "+ratingstablename)
    range = cur.fetchone()[0]
    # print "count is "
    # print range
    size = max_rating/numberofpartitions
    i=0
    while i <numberofpartitions:
        query = "CREATE TABLE range_part"+str(range_partition_index)+\
                " (UserID integer NOT NULL, MovieID integer NOT NULL, Rating decimal)"

        # print "partition creation query is "
        print query
        cur.execute(query)
        range_partition_index += 1
        if i == 0:
            ins = "SELECT * FROM "+ratingstablename+" WHERE rating>="+str(round(i*size, 2))+" AND rating<="\
              +str(round((i+1)*size, 2))
        else:
            ins = "SELECT * FROM " + ratingstablename + " WHERE rating>" + str(round(i * size, 2)) + " AND rating<=" \
                  + str(round((i + 1) * size, 2))

        #print "insertion into partition cmd is "
        #print ins
        temp = "INSERT INTO range_part"+str(range_partition_index-1)+" "+ins
        print temp
        cur.execute(temp)
        update_metadata("range_part" + str(range_partition_index-1), str(round(i * size, 2)), str(round((i + 1) * size, 2)), openconnection)
        openconnection.commit()
        i=i+1
    print "Range partition of table is successful"
    pass


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    if not isinstance(numberofpartitions, int) and numberofpartitions <= 1:
        print "Enter valid no of partitions"
        return

    i = 0
    cur = openconnection.cursor()
    cur.execute("SELECT COUNT(*) FROM " + ratingstablename)
    range1 = cur.fetchone()[0]
    # print "count is "
    # print range
    while i < numberofpartitions:
        query = "CREATE TABLE rrobin_part" + str(i) + \
            " (UserID integer NOT NULL, MovieID integer NOT NULL, Rating decimal)"
        print query
        cmd = "INSERT INTO rrobin_part" + str(i) + \
              " select t.userid, t.movieid, t.rating from(select *, row_number() OVER() as row from "\
              + ratingstablename + ")t where t.row%" + str(numberofpartitions) + "=" + str(i)
        print cmd
        cur.execute(query)
        cur.execute(cmd)
        update_metadata("rrobin_part" + str(i), str(0), str(0), openconnection)
        # roundrobin_partition_index += 1
        i = i+1
    query = "CREATE TABLE IF NOT EXISTS meta_data2 (last_rr_index integer, partition_cnt integer)"
    if range1 > 0:
        query2 = "INSERT INTO meta_data2 VALUES ('" + str((range1 % numberofpartitions)-1) + "' , '"+str(numberofpartitions)+"')"
    else:
        query2 = "INSERT INTO meta_data2 VALUES ('" + str(0) + "' , '" + str(numberofpartitions) + "')"
    cur.execute(query)
    cur.execute(query2)
    print "Round robin partition of table is successful"
    pass


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    rating = float(rating)
    if rating > max_rating or rating < min_rating:
        print "The rating values lie in between 0 and 5 (inclusive) and the given rating is invalid"
        return
    cur = openconnection.cursor()
    cmd = "SELECT * FROM meta_data2"
    cur.execute(cmd)
    row = cur.fetchone()
    last_index = row[0]
    numberofpartitions = row[1]
    new_index = (last_index + 1)% numberofpartitions
    #print "Last index ", last_index
    #print "no of partitions ", numberofpartitions
    cmd1 = "INSERT INTO rrobin_part" + str(new_index) + " VALUES('" + str(userid) + "','" + str(itemid) + "','" + str(rating) + \
          "')"
    cmd2 = "UPDATE meta_data2 SET last_rr_index="+str(new_index)
    print cmd1
    print cmd2
    #print "last Index now is ", last_index
    cur.execute(cmd1)
    cur.execute(cmd2)
    pass


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    rating = float(rating)
    if rating > max_rating and rating < min_rating:
        print "The rating values lie in between 0 and 5 (inclusive)"
        return
    cur = openconnection.cursor()
    cmd = "SELECT * from meta_data"
    cur.execute(cmd)
    rows = cur.fetchall()
    #print "Hi after fetching of rows"
    for row in rows:
        if row[1] == 0.0 and row[2] == 0.0:  #row1 is min and row2 is max
            continue
        if row[1] == 0.0 and rating == 0.0:
            query = "INSERT INTO "+str(row[0])+" VALUES ('"+str(userid)+"', '"+str(itemid)+"', '"+str(rating)+"')"
            print query
            cur.execute(query)
        else:
            if rating<=row[2] and rating>row[1]:
                query = "INSERT INTO "+str(row[0])+" VALUES ('"+str(userid)+"', '"+str(itemid)+"', '"+str(rating)+"')"
                print query
                cur.execute(query)
            else:
                continue
    pass

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cmd = "SELECT pname from meta_data"
    print cmd
    try:
        cur.execute(cmd)
    except:
        print "Meta_data table doesn't exist"
        return
    rows = cur.fetchall()
    for row in rows:
        #print "row is ", row[0]
        query = "DROP TABLE IF EXISTS "+str(row[0])
        print query
        cur.execute(query)
    cur.execute("DROP TABLE IF EXISTS meta_data")
    cur.execute("DROP TABLE IF EXISTS meta_data2")
    pass

def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
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


# Middleware
def before_db_creation_middleware():
    # Use it if you want to
    pass


def after_db_creation_middleware(databasename):
    # Use it if you want to
    pass


def before_test_script_starts_middleware(openconnection, databasename):
    # Use it if you want to
    pass


def after_test_script_ends_middleware(openconnection, databasename):
    # Use it if you want to
    pass


if __name__ == '__main__':
    try:

        # Use this function to do any set up before creating the DB, if any
        before_db_creation_middleware()

        create_db(DATABASE_NAME)

        # Use this function to do any set up after creating the DB, if any
        after_db_creation_middleware(DATABASE_NAME)

        with getopenconnection() as con:
            # Use this function to do any set up before I starting calling your functions to test, if you want to
            before_test_script_starts_middleware(con, DATABASE_NAME)

            # Here is where I will start calling your functions to test them. For example,
            loadratings("Ratings", "ratings.dat", con)
            #rangepartition("Ratings", 4, con)
            #rangeinsert("Ratings",'2016','2016','3',con)
            #rangeinsert("Ratings", '2016', '2017', '5', con)
            #rangeinsert("Ratings", '2016', '2018', '0', con)
            #rangeinsert("Ratings", '2016', '2019', '1.5', con)
            #roundrobinpartition("Ratings", 5, con)
            #roundrobininsert("Ratings", '1208598621', '008', '5', con)
            #roundrobininsert("Ratings", '1208598621', '009', '5', con)
            #roundrobininsert("Ratings", '1208598621', '010', '5', con)
            #deletepartitionsandexit(con)
            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
            after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail
