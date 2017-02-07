#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
import traceback

rangename = "RangeRatingsPart"
rrname = "RoundRobinRatingsPart"
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    #Implement RangeQuery Here.
    file = open("RangeQueryOut.txt","w")
    try:
        cur = openconnection.cursor()
        partitionslist = []
        cmd = "SELECT * from rangeratingsmetadata"
        cur.execute(cmd)
        rows = cur.fetchall()
        for row in rows:
            if ratingMinValue == 0 and row[1] == 0:
            	if row[0] not in partitionslist:
                	partitionslist.append(row[0])
                # print "a"
                # print row
                # print (partitionslist)
            elif ratingMinValue > row[1] and ratingMinValue <= row[2]:
                if row[0] not in partitionslist:
                    partitionslist.append(row[0])
                    # print "b"
                    # print row
                    # print (partitionslist)
            elif ratingMaxValue > row[1] and ratingMaxValue <= row[2]:
                if row[0] not in partitionslist:
                    partitionslist.append(row[0])
                    # print "b"
                    # print row
                    # print (partitionslist)
            elif ratingMinValue <= row[1] and  ratingMaxValue>=row[2]:
            	if row[0] not in partitionslist:
                    partitionslist.append(row[0])
                    # print "c"
                    # print row
                    # print (partitionslist)
        # print (partitionslist)

        for table in partitionslist:
            cmd2 = "select * from "+rangename+str((table))+" where Rating >= "+str(ratingMinValue)+" and Rating <= "+str(ratingMaxValue)
            print cmd2
            cur.execute(cmd2)
            result = cur.fetchall()
            for row in result:
                file.write("%s%s,%s,%s,%s\n" %(rangename,table,row[0],row[1],row[2]))
                # print row


        #Get the number of round robin partitions from round robin meta data table
        #

        cmd3 = "select partitionnum from roundrobinratingsmetadata"
        cur.execute(cmd3)
        temp = cur.fetchone()[0]
        # print temp
        for i in range(0,temp):
        	cmd4 = "select * from "+rrname+str(i)+" where Rating >= "+str(ratingMinValue)+" and Rating <="+str(ratingMaxValue)
        	print cmd4
        	cur.execute(cmd4)
        	res = cur.fetchall()
        	for row in res:
        		file.write("%s%s,%s,%s,%s\n" %(rrname,i,row[0],row[1],row[2]))
    except Exception as e:
        print e
        print traceback.print_exc()



def PointQuery(ratingsTableName, ratingValue, openconnection):
    #Implement PointQuery Here.
    pass # Remove this once you are done with implementation
    file = open("PointQueryOut.txt","w")
    try:
        cur = openconnection.cursor()
        partitionslist = []
        cmd = "SELECT * from rangeratingsmetadata"
        cur.execute(cmd)
        rows = cur.fetchall()
        for row in rows:
            if ratingValue == 0:
                partitionslist.append(row[0])
            	break
            elif ratingValue > row[1] and ratingValue <= row[2]:
                if row[0] not in partitionslist:
                    partitionslist.append(row[0])
        
            
        # print (partitionslist)

        for table in partitionslist:
            cmd2 = "select * from "+rangename+str((table))+" where Rating = "+str(ratingValue)
            print cmd2
            cur.execute(cmd2)
            result = cur.fetchall()
            for row in result:
                file.write("%s%s,%s,%s,%s\n" %(rangename,table,row[0],row[1],row[2]))
                # print row
        
        cmd3 = "select partitionnum from roundrobinratingsmetadata "
        cur.execute(cmd3)
        temp = cur.fetchone()[0]
        #print temp
        for i in range(0,temp):
        	cmd4 = "select * from "+rrname+str(i)+" where Rating = "+str(ratingValue)
        	print cmd4
        	cur.execute(cmd4)
        	res = cur.fetchall()
        	for row in res:
        		file.write("%s%s,%s,%s,%s\n" %(rrname,i,row[0],row[1],row[2]))
    except Exception as e:
        print e
        print traceback.print_exc()
