import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP SCHEMAS
fact_schema_drop= ("DROP SCHEMA IF EXISTS fact_tables CASCADE")
dimension_schema_drop= ("DROP SCHEMA IF EXISTS dimension_tables CASCADE")
staging_schema_drop= ("DROP SCHEMA IF EXISTS staging_tables CASCADE")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


# CREATE SCHEMAS STAGING
staging_schema= ("CREATE SCHEMA IF NOT EXISTS staging_tables")
# CREATE TABLES
staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events 
(
    artist          VARCHAR(255),
    auth            VARCHAR(255),
    first_name      VARCHAR(255),
    gender          VARCHAR(255),
    item_in_session INTEGER,  
    last_name       VARCHAR(255),
    length          DECIMAL,
    level           VARCHAR(255),
    location        VARCHAR(255),
    method          VARCHAR(255),
    page            VARCHAR(255),
    registration    DECIMAL,
    session_id      INTEGER,
    song            VARCHAR(255),
    status          INTEGER,
    ts              BIGINT,
    user_agent      VARCHAR(255),
    user_id         INTEGER
)
""")

staging_songs_table_create = (""" CREATE TABLE IF NOT EXISTS staging_songs 
( 
    num_songs        INTEGER,
    artist_id        VARCHAR(255),
    artist_latitude  DECIMAL,
    artist_longitude DECIMAL,
    artist_location  VARCHAR(255),
    artist_name      VARCHAR(255),
    song_id          VARCHAR(255),
    title            VARCHAR(255),
    duration         DECIMAL,
    year             INTEGER   
)
""")


# CREATE SCHEMAS FACT
fact_schema= ("CREATE SCHEMA IF NOT EXISTS fact_tables")
# CREATE TABLES
songplay_table_create = (""" CREATE TABLE songplays
(
    songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time  TIMESTAMP NOT NULL,
    user_id     INTEGER NOT NULL,
    level       VARCHAR(255),
    song_id     VARCHAR(255),
    artist_id   VARCHAR(255),
    session_id  INTEGER, 
    location    VARCHAR(255),
    user_agent  VARCHAR(255),
    duration    DECIMAL
    
)
""")


# CREATE SCHEMAS DIMENSION
dimension_schema= ("CREATE SCHEMA IF NOT EXISTS dimension_tables")
# CREATE TABLES
user_table_create = (""" CREATE TABLE users
(
    user_id     INTEGER NOT NULL PRIMARY KEY,
    first_name  VARCHAR(255),
    last_name   VARCHAR(255),
    gender      VARCHAR(255),
    level       VARCHAR(255)
)
""")

song_table_create = (""" CREATE TABLE songs
(
    song_id     VARCHAR(255) NOT NULL PRIMARY KEY,
    title       VARCHAR(255),
    artist_id   VARCHAR(255) NOT NULL,
    year        INTEGER,
    duration    DECIMAL 
)
""")

artist_table_create = (""" CREATE TABLE artists
(
    artist_id   VARCHAR(255) NOT NULL PRIMARY KEY,
    name        VARCHAR(255),
    location    VARCHAR(255),
    latitude   DECIMAL,
    longitude   DECIMAL
)
""")

time_table_create = (""" CREATE TABLE time
(
    start_time  TIMESTAMP NOT NULL PRIMARY KEY,
    hour        INTEGER,
    day         INTEGER,
    week        INTEGER,
    month       INTEGER,
    year        INTEGER,
    weekday     INTEGER
)
""")


# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM {}
    IAM_ROLE {}
    JSON {}
    ;""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])


staging_songs_copy = ("""
    COPY staging_songs
    FROM {}
    IAM_ROLE {}
    JSON 'auto'
    ;""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])


# FINAL TABLES

songplay_table_insert = (""" INSERT INTO songplays (start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time, a.user_id, a.level, b.song_id, b.artist_id, a.session_id,
    a.location, a.user_agent
    FROM staging_events a
    LEFT JOIN staging_songs b ON a.song = b.title AND a.artist = b.artist_name AND a.length = b.duration
    WHERE a.page = 'NextSong'
""")

user_table_insert = (""" INSERT INTO users (user_id,first_name,last_name,gender,level)
 SELECT DISTINCT a.user_id, a.first_name, a.last_name, a.gender, a.level
    FROM staging_events a
    WHERE a.page = 'NextSong'
""")

song_table_insert = (""" INSERT INTO songs (song_id,title,artist_id,year,duration)
    SELECT DISTINCT b.song_id, b.title, b.artist_id, b.year, b.duration
    FROM staging_songs b
""")

artist_table_insert = (""" INSERT INTO artists (artist_id,name,location,latitude,longitude)
    SELECT DISTINCT b.artist_id, b.artist_name, b.artist_location, b.artist_latitude, b.artist_longitude
    FROM staging_songs b
""")

time_table_insert = (""" INSERT INTO time (start_time,hour,day,week,month,year,weekday)
    SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time, EXTRACT(hour from start_time), 
    EXTRACT(day from start_time), EXTRACT(week from start_time), EXTRACT(month from start_time), EXTRACT(year from start_time),
    EXTRACT(weekday from start_time)
    FROM staging_events a
    WHERE a.page = 'NextSong'
""")

# QUERY LISTS

create_schemas_queries =[fact_schema, dimension_schema, staging_schema]
drop_schemas_queries = [fact_schema_drop, dimension_schema_drop, staging_schema_drop]
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy,staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
