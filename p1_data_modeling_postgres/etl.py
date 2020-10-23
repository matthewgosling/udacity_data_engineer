import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):

    """ Process_song_file takes the cursor and the list of song files as arguments and processes the data for the Songs and the Artists table.
    
    It reads the song json files and stores them in a dataframe. Then it extracts the Songs and Artist related columns from the dataframe and executes SQL statements to insert data into the Songs and the Artists tables.  
    """    
    
    # open song file

    df = pd.read_json(filepath, lines=True)

    # insert song record

    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)

    # insert artist record

    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):

    """  Process_log_file takes the cursor and the list of log files as arguments and processes the data for Time, User and Songplays tables. 
    
    It captures all json formatted log files and stores them in a dataframe. It formats the time information and stores it in another dataframe and triggers an SQL statement to insert the dataframe into Time table. 
    
    It captures all user related columns from the main dataframe and stores them in a temporary dataframe. Then it triggers an SQL statement to store the user dataframe into the Users table. 
    
    It triggers an SQL statement to capture the song_id, Artist_id and length from the Songs and the Artists tables. It triggers an SQL statement to insert the necessary columns to Songsplay table. 
    """
        
    # open log file

    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action

    df = df.loc[df['page'] == 'NextSong']

    # convert timestamp column to datetime

    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    # isocalendar is now the new format in pandas

    time_data = [df.ts.values, t.dt.hour.values, t.dt.day.values, t.dt.isocalendar().week.values, t.dt.month.values, t.dt.year.values, t.dt.weekday.values]

    # old version uses weekofyear. Deprecated in pandas since version 1.1.0
    #    time_data = [df.ts.values, t.dt.hour.values, t.dt.day.values, t.dt.weekofyear.values, t.dt.month.values, t.dt.year.values, t.dt.weekday.values]

    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table

    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records

    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records

    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables

        cur.execute(song_select, (row.song, row.artist, row.length))
        result = cur.fetchone()
        if result:
            songid, artistid = result
        else:
            songid, artistid = None, None

        # insert songplay record

        songplay_data = [row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):

    """ Process_data function uses the cursor and connection to collect all the files and call functions to process the Song and the Log files.
   
   It collects all the files in the available directory and stores them in a list which it passes along with cursor to call the process_song_file and process_log_file functions. On completion of the functions, it prints how many files are processed.
    """
    
    # get all files matching extension from directory

    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
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
    """ Main function which calls the process_data function for processing the Songs and Logs files. 
    
    It passes the cursor, connection, filepaths and the function names as arguments.
    """
    
    conn = psycopg2.connect('host=127.0.0.1 dbname=sparkifydb user=student password=student')
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == '__main__':
    main()
