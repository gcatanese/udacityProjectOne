import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Description: This function reads the file in the filepath (data/song_data)
    to get song/artist data and populate the database tables

    Arguments:
        cur: the cursor object.
        filepath: song data file path.

    Returns:
        None
    """

    print("Process SONG data") 
     
    # open song file
    df = pd.read_json(filepath, typ='series')

    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values.tolist()
    cur.execute(artist_table_insert, artist_data)

    # insert song record
    song_data = df[['song_id','title', 'artist_id', 'year', 'duration']].values.tolist()
    cur.execute(song_table_insert, song_data)

def process_log_file(cur, filepath):
    """
    Description: This function reads the file in the filepath (data/log_data)
    to get user/time data and populate the database tables

    Arguments:
        cur: the cursor object.
        filepath: log data file path.

    Returns:
        None
    """

    """Process and insert LOG data"""
    
    print("Process LOG data") 
    
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    time_data = [[df['ts'], t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday]]
    column_labels = ("timestamp", "hour", "day", "week of year", "month", "year", "weekday")
    time_df = pd.DataFrame.from_dict({'ts': df['ts'], 'hour': t.dt.hour, 'day': t.dt.day, 'week': t.dt.week, 'month': t.dt.month,
                                      'year': t.dt.year, 'weekday': t.dt.weekday})

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates(subset=['userId', 'level'])

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row['ts'], row['userId'], row['level'], songid, artistid, row['sessionId'],
                         row['location'], row['userAgent'])
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Description: Generic method to perform a call to the
    methods implementing the logic

    Arguments:
        cur: the cursor object.
        conn: the connection object.
        filepath: file path.
        func: method performing the logic.

    Returns:
        None
"""

    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """Main: runs the pipeline"""
    
    print("Main starts...")
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    
    print("Pipeline is completed") 
   
    conn.close()


if __name__ == "__main__":
    main()
