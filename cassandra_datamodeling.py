# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv

# Creating list of filepaths to process original event csv data files
# checking your current working directory
print(os.getcwd())

# Get your current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    
# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
    print(file_path_list)
    
# initiating an empty list of rows that will be generated from each file
full_data_rows_list = [] 
print(full_data_rows_list)
    
# for every filepath in the file path list 
for f in file_path_list:

# reading csv file 
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
        
 # extracting each data row one by one and append it        
        for line in csvreader:
            #print(line)
            full_data_rows_list.append(line) 
            
# uncomment the code below if you would like to get total number of rows 
print(len(full_data_rows_list))
# uncomment the code below if you would like to check to see what the list of event data rows will look like
print(full_data_rows_list)

# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

# This should make a connection to a Cassandra instance your local machine 
# (127.0.0.1)

from cassandra.cluster import Cluster
cluster = Cluster()
try:
    cluster = Cluster(['127.0.0.1'])
    # To establish connection and begin executing queries, need a session
    session = cluster.connect()
    print(session)
except Exception as e:
    print(e)
    
# TO-DO: Create a Keyspace 
try:
    keyspc= session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity_prj_02_keyspc
    WITH REPLICATION =
    {'class' : 'SimpleStrategy','replication_factor': 1}""")
    print("Success in KEYSPACE!!! " + str(keyspc))
except Exception as e:
    print(e)

drop_query="DROP TABLE IF EXISTS music_app_history"
try:
    session.execute(drop_query)
    print(drop_query)
except Exception as e:
    print(e)

create_query="CREATE TABLE IF NOT EXISTS music_app_history"
create_query=create_query + "(sessionId int ,artist text, song_title text, itemInSession int, length_of_song float, PRIMARY KEY((sessionid, itemInSession), artist))"
try:
    session.execute(create_query)
    print(create_query)
except Exception as e:
    print(e)
    
 file = 'event_datafile_new.csv'
with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
## TO-DO: Assign the INSERT statements into the `query` variable
        query = "INSERT INTO music_app_history (sessionId,artist, song_title, itemInSession, length_of_song) VALUES "
        query = query + "(%s, %s, %s, %s, %s)"
        #print(query)
        ## TO-DO: Assign which column element should be assigned for each column in the INSERT statement.
        ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
        session.execute(query, (int(line[8]), str(line[0]), str(line[9]), int(line[3]), float(line[5])))

        
verify_data="SELECT * from music_app_history "
verify_data = verify_data + "where  sessionId = 338 and itemInSession = 4"
try:
    rows = session.execute(verify_data)
except Exception as e:
    print(e)
for row in rows:
    print(row.sessionid, row.artist, row.song_title, row.iteminsession, row.length_of_song,)
    
## TO-DO: Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)\
## for userid = 10, sessionid = 182

drop_query="DROP TABLE IF EXISTS song_details"
try:
    session.execute(drop_query)
    print(drop_query)
except Exception as e:
    print(e)

create_query="""CREATE TABLE IF NOT EXISTS song_details"""
create_query=create_query + """(userid int, sessionId int ,artist text,  song_title text, 
                                 first_name text, last_name text, itemInSession int, length_of_song float,
                                 PRIMARY KEY((userid, sessionId), itemInSession ))
                               """
try:
    session.execute(create_query)
    print(create_query)
except Exception as e:
    print(e)


file = 'event_datafile_new.csv'
with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
## TO-DO: Assign the INSERT statements into the `query` variable
        query = "INSERT INTO song_details (userid, sessionId ,artist ,  song_title , first_name , last_name, itemInSession, length_of_song) VALUES "
        query = query + "(%s, %s, %s, %s, %s, %s, %s, %s)"
        #print(query)
        ## TO-DO: Assign which column element should be assigned for each column in the INSERT statement.
        ## For e.g., to INSERT artist_name and user first_name, you would change the code below to `line[0], line[1]`
        session.execute(query, (int(line[10]),int(line[8]), str(line[0]), str(line[9]), str(line[1]), str(line[4]), int(line[3]), float(line[5])))
print("Success Insert")

                    
verify_data="SELECT * from song_details "
verify_data = verify_data + "where  userid = 10 and sessionid = 182 order by iteminsession desc "
try:
    rows = session.execute(verify_data)
except Exception as e:
    print(e)
for row in rows:
    print(row.sessionid, row.artist, row.song_title, row.iteminsession, row.length_of_song,)
