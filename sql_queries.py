# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
songplay_id serial, 
start_time bigint not null, 
user_id int not null, 
level text, 
song_id text,
artist_id text, 
session_id int, 
location text, 
user_agent text, 
CONSTRAINT pk_songplays PRIMARY KEY (songplay_id));
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
user_id int, 
first_name text not null, 
last_name text not null, 
gender text, 
level text, 
CONSTRAINT pk_users PRIMARY KEY (user_id, level));
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (song_id text, 
title text not null, 
artist_id text not null, 
year int, 
duration numeric, 
CONSTRAINT pk_songs PRIMARY KEY (song_id));
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (artist_id text, 
name text not null, 
location text, 
latitude numeric, 
longitude numeric, 
CONSTRAINT pk_artists PRIMARY KEY (artist_id));
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (start_time bigint, 
hour int not null, 
day int not null, 
week int not null, 
month int not null, 
year int not null, 
weekday int not null);
""")

# FOREIGN KEYS
song_fk_artists = ("""
ALTER TABLE songs ADD CONSTRAINT fk_artists FOREIGN KEY (artist_id) REFERENCES artists (artist_id);
""")

songplays_fk_artists = ("""
ALTER TABLE songplays ADD CONSTRAINT fk_artists FOREIGN KEY (artist_id) REFERENCES artists (artist_id);
""")

songplays_fk_songs = ("""
ALTER TABLE songplays ADD CONSTRAINT fk_songs FOREIGN KEY (song_id) REFERENCES songs (song_id);
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT ON CONSTRAINT pk_users DO NOTHING
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT ON CONSTRAINT pk_songs DO NOTHING
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT ON CONSTRAINT pk_artists DO NOTHING
""")


time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday) VALUES (%s, %s, %s, %s, %s, %s, %s)
""")

# FIND SONGS

song_select = ("""
SELECT songs.song_id, artists.artist_id FROM songs JOIN artists ON songs.artist_id = artists.artist_id WHERE title = %s AND name = %s AND duration = %s
""")

# QUERY LISTS

create_table_queries = [artist_table_create, song_table_create, user_table_create, time_table_create, songplay_table_create, song_fk_artists, songplays_fk_artists, songplays_fk_songs]
drop_table_queries = [songplay_table_drop, time_table_drop, user_table_drop, song_table_drop, artist_table_drop]
