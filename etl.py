import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def process_song_file(cur, filepath):
    """The function to read and load the song and artist data 
       from the song file using the cursor obect and filepath 
    """
    df = pd.read_json(filepath, lines=True)

     
    # Declare the variable to hold the columns from the JSON file and read in dataframe to produce the list
     
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].flatten()
    cur.execute(song_table_insert, song_data)
    
    # Populate the artist record using data frame and transform the same in list
    

    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].flatten().tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """This function process the data from log file 
       and load users and open log file 
    """
    df = pd.read_json(filepath, orient='records', typ='frame' ,lines=True)

    # filter by NextSong action you can use square brackets to select one column of the df DataFrame 
    is_song = df["page"] == 'NextSong'
    df = df[is_song]

    # convert timestamp column to datetime. Print out ts column as Pandas Series
    t  = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    
    time_data = (t.dt.time,t.dt.hour,t.dt.day,t.dt.weekofyear,t.dt.month,t.dt.year,t.dt.weekday)
    column_labels = ("start_time","hour","day","week","month", "year","weekday")
    time_df = pd.DataFrame(dict(zip(column_labels,time_data)),columns=column_labels).reset_index(drop=True)  
    # Iterate throw the dataframe time_df """
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # Load user table and accesing the columns using pandas DataFrame with columns as shown below """
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # Insert user records 
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # Get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # Insert songplay record 
        songplay_data = (pd.to_datetime(row.ts, unit='ms'),row.userId,row.level,songid,artistid,row.sessionId,row.location,row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """ The Function to read the files from path"""
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # Get total number of files found 
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # Iterate over files and process """
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """For python main function, we have to define a function and 
       then use if __name__ == __main__ condition to execute this function. 
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    # Process data 
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()

    

if __name__ == "__main__":
    main()
