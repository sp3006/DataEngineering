# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id int, start_time varchar(10), user_id int, level int, song_id int, artist_id int, session_id int, location varchar(10), user_agent varchar(50))
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id int, first_name varchar(30), last_name varchar(30), gender varchar(20), level varchar(30))
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(song_id int, title varchar(60), artist_id int, year int, duration varchar(10))
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(artist_id int, name varchar(30), location varchar(30), lattitude float, longitude float)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(start_time int, hour int, day int, week int, month int, year int, weekday int)
""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songsplay VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""INSERT INTO users VALUES(%s, %s, %s, %s, %s)
""")

song_table_insert = ("""INSERT INTO songs VALUES(%s, %s, %s, %s, %s)
""")

artist_table_insert = ("""INSERT INTO artists VALUES(%s, %s, %s, %s, %s)
""")


time_table_insert = ("""INSERT INTO users VALUES(%s, %s, %s, %s, %s, %s, %s)
""")

# FIND SONGS

song_select = ("""select * from songs
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
