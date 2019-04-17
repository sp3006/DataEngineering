# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id SERIAL PRIMARY KEY, start_time time NOT NULL, user_id text NOT NULL, level text, song_id text, artist_id text, session_id integer NOT NULL, location text, user_agent text)
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id text PRIMARY KEY, firstName text NOT NULL, lastName text NOT NULL, gender text, level text)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(song_id text PRIMARY KEY, title text, artist_id text, year smallint, duration float)""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(artist_id text PRIMARY KEY, name text, location varchar(5000), lattitude float, longitude float)""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(start_time time PRIMARY KEY, hour smallint, day smallint, week smallint, month smallint, year smallint, weekday smallint)
""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays(start_time , user_id , level , song_id , artist_id , session_id , location , user_agent) VALUES(%s, %s, %s, %s, %s, %s, %s, %s)""")

user_table_insert = ("""INSERT INTO users (user_id, firstName , lastName , gender, level)VALUES(%s, %s, %s, %s, %s) ON CONFLICT (user_id) DO UPDATE SET level=EXCLUDED.level """)

song_table_insert = ("""INSERT INTO songs(song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (song_id) DO UPDATE SET song_id = EXCLUDED.song_id""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, lattitude, longitude)VALUES(%s, %s, %s, %s, %s) ON CONFLICT (artist_id) DO UPDATE SET artist_id = EXCLUDED.artist_id""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) VALUES(%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (start_time) DO UPDATE SET start_time = EXCLUDED.start_time""")

# FIND SONGS

song_select = ("""
SELECT s.song_id, a.artist_id FROM songs as s
LEFT JOIN artists as a 
ON a.artist_id = s.artist_id
WHERE s.title = (%s) AND a.name = (%s) AND s.duration=(%s)
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
