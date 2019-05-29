import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events
       (artist_id       varchar,
        auth            varchar,
        first_name      varchar,
        gender          varchar,
        session_item    integer,
        last_name       varchar,
        length          float,
        level           varchar,
        location        varchar,
        method          varchar,
        page            varchar,
        registration    varchar,
        session_id      bigint,
        song            varchar,
        status          integer,
        ts              bigint,
        user_agent      varchar,
        user_id         integer
       ) ;
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs bigint,
    artist_id varchar,
    artist_lattitude decimal(10,8),
    artist_longitude decimal(11,8),    
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration float,
    year integer
);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (songplay_id BIGINT identity PRIMARY KEY, start_time timestamp not null, user_id integer not null, level VARCHAR, song_id VARCHAR(20) not null, artist_id VARCHAR not null, session_id integer, location VARCHAR, user_agent VARCHAR)
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id integer PRIMARY KEY, first_name VARCHAR, last_name VARCHAR, gender VARCHAR, level VARCHAR)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(song_id VARCHAR PRIMARY KEY, title VARCHAR, artist_id VARCHAR NOT null, year smallint, duration float)""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(artist_id VARCHAR PRIMARY KEY, name VARCHAR, location VARCHAR, lattitude decimal(10,8), longitude decimal(11,8))""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(start_time timestamp PRIMARY KEY, hour integer, day integer, week integer, month integer, year integer, weekday integer)
""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events FROM {} \
                          iam_role  {} \
                          region 'us-west-2' \
                          FORMAT AS json {} timeformat 'epochmillisecs';\
""").format(config.get("S3","LOG_DATA"),config.get("IAM_ROLE","ARN"),config.get("S3","LOG_JSONPATH"))

staging_songs_copy = ("""COPY staging_songs FROM {}
                        iam_role  {} \
                        compupdate off region 'us-west-2' \
                        FORMAT AS json 'auto';\
                          """).format(config.get("S3","SONG_DATA"), config.get("IAM_ROLE","ARN"))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays(start_time,user_id,level, song_id,artist_id, session_id, location, user_agent)
                            SELECT DISTINCT 
                                    TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second'  as start_time
								   ,CASE WHEN a.user_id is null THEN 99 ELSE a.user_id END as user_id
								   ,a.level as level
								   ,b.song_id as song_id
								   ,a.artist_id as artist_id
								   ,a.session_id as session_id
								   ,a.location as location
								   ,a.user_agent as user_agent
							  FROM
								   public.staging_events a
							   JOIN
								   public.staging_songs b
							   ON
								   a.title=b.artist_name
                               and a.song=b.title
                               where a.page='NextPage'
                               """)

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT CASE WHEN a.user_id is null THEN 99 ELSE a.user_id END as user_id , a.first_name, a.last_name, a.gender, a.level FROM
public.staging_events a WHERE a.page ='NextPage' """)

song_table_insert = ("""INSERT INTO songs(song_id, title, artist_id, year, duration) SELECT b.song_id, b.title, b.artist_id,b.year, b.duration FROM public.staging_songs b""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, lattitude, longitude) SELECT b.artist_id,b.artist_name, b.artist_location, b.artist_lattitude, b.artist_longitude FROM public.staging_songs b""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
       SELECT  TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' as Start_time,
       CAST(DATE_PART(hour , (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')) as integer) as hour,
       CAST(DATE_PART(day , (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')) AS integer)as day,
       CAST(DATE_PART(week , (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')) AS integer)as week,
       CAST(DATE_PART(month , (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')) AS integer)as month,
       CAST(DATE_PART(year , (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')) AS integer)as year,
       CAST(DATE_PART(dow , (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second')) AS integer)as weekday
       FROM songplays;""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,time_table_insert]
