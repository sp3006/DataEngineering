# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id varchar(20), start_time varchar(10), user_id varchar(50), level varchar(50), song_id varchar(50), artist_id varchar(50), session_id varchar(50), location varchar(50), user_agent varchar(50))
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (userId varchar(50), firstName varchar(50), lastName varchar(50), gender varchar(50), level varchar(50))
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(song_id varchar(50), title varchar(50), artist_id varchar(50), year varchar(50), duration varchar(50))""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(artist_id varchar(50), name varchar(50), location varchar(30), lattitude float, longitude float)""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(start_time varchar(50), hour varchar(50), day varchar(50), week varchar(50), month varchar(50), year varchar(50), weekday varchar(50))
""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songsplay VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)""")

user_table_insert = ("""INSERT INTO users VALUES(%s, %s, %s, %s, %s)""")

song_table_insert = ("""INSERT INTO songs VALUES(%s, %s, %s, %s, %s)""")

artist_table_insert = ("""INSERT INTO artists VALUES(%s, %s, %s, %s, %s)""")

time_table_insert = ("""INSERT INTO users VALUES(%s, %s, %s, %s, %s, %s, %s)""")

# FIND SONGS

song_select = ("""select * from songs
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
