import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events \
                                (\
                                    artist VARCHAR, \
                                    auth VARCHAR, \
                                    firstName VARCHAR, \
                                    gender VARCHAR, \
                                    itemSession INT, \
                                    lastName VARCHAR, \
                                    length FLOAT, \
                                    level VARCHAR, \
                                    location VARCHAR, \
                                    method VARCHAR, \
                                    page VARCHAR, \
                                    registration VARCHAR, \
                                    sessionId INT, \
                                    song VARCHAR, \
                                    status INT, \
                                    ts timestamp, \
                                    userAgent VARCHAR, \
                                    userId INT \
                                )""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs \
                                (\
                                    song_id VARCHAR PRIMARY KEY, \
                                    artist_id VARCHAR, \
                                    artist_latitude FLOAT, \
                                    artist_longitude FLOAT, \
                                    artist_location VARCHAR, \
                                    artist_name VARCHAR,\
                                    duration FLOAT, \
                                    num_songs INT, \
                                    title VARCHAR, \
                                    year INT  \
                                )""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays \
                            (\
                                songplay_id INT IDENTITY(1, 1) PRIMARY KEY, \
                                start_time TIMESTAMP NOT NULL, \
                                user_id INT NOT NULL, \
                                level VARCHAR, \
                                song_id VARCHAR, \
                                artist_id VARCHAR, \
                                session_id INT, \
                                location VARCHAR, \
                                user_agent VARCHAR\
                            )""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users \
                        (\
                            user_id INT NOT NULL PRIMARY KEY, \
                            level VARCHAR, \
                            first_name VARCHAR, \
                            last_name VARCHAR, \
                            gender VARCHAR\
                        )""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs \
                        (\
                            song_id VARCHAR NOT NULL PRIMARY KEY, \
                            title VARCHAR NOT NULL, \
                            artist_id VARCHAR, \
                            year INT, \
                            duration FLOAT\
                        )""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists \
                            (\
                                artist_id VARCHAR NOT NULL PRIMARY KEY, \
                                name VARCHAR, \
                                latitude FLOAT, \
                                longitude FLOAT, \
                                location VARCHAR\
                            )""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time \
                        (\
                            start_time TIMESTAMP NOT NULL PRIMARY KEY, \
                            hour INT, \
                            day INT, \
                            week INT, \
                            month INT, \
                            year INT, \
                            weekday INT\
                        )""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events from {} \
                        credentials 'aws_iam_role={}' \
                        json {} \
                        timeformat as 'epochmillisecs' \
                        region 'us-west-2'; \
                        """
                        ).format(
                        config.get("S3","LOG_DATA"), \
                        config.get("IAM_ROLE","ARN"), \
                        config.get("S3","LOG_JSONPATH"))

staging_songs_copy = ("""copy staging_songs from {} \
                        credentials 'aws_iam_role={}' \
                        json 'auto' \
                        region 'us-west-2'; \
                        """).format(
                        config.get("S3","SONG_DATA"), \
                        config.get("IAM_ROLE","ARN"))

# FINAL TABLES

songplay_table_insert = ("INSERT INTO songplays \
                            (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)\
                            SELECT \
                            e.ts, e.userId, e.level, s.song_id, s.artist_id, e.sessionId, e.location, e.userAgent \
                            FROM staging_events AS e \
                            JOIN staging_songs AS s ON s.title = e.song;")


user_table_insert = ("INSERT INTO users \
                        (user_id, first_name, last_name, gender, level) \
                        SELECT DISTINCT \
                        userId, firstName, lastName, gender, level \
                        FROM  staging_events \
                        WHERE userId IS NOT NULL;")

song_table_insert = ("INSERT INTO songs \
                        (song_id, title, artist_id, year, duration) \
                        SELECT DISTINCT \
                        song_id, title, artist_id, year, duration \
                        FROM staging_songs;")

artist_table_insert = ("INSERT INTO artists \
                            (artist_id, name, location, latitude, longitude) \
                            SELECT DISTINCT \
                            artist_id, artist_name, artist_location, artist_latitude, artist_longitude \
                            FROM  staging_songs;")


time_table_insert = ("INSERT INTO time \
                        (start_time, hour, day, week, month, year, weekday) \
                        SELECT DISTINCT \
                        ts, \
                        EXTRACT(hour FROM ts), \
                        EXTRACT(day FROM ts), \
                        EXTRACT(week FROM ts), \
                        EXTRACT(month FROM ts), \
                        EXTRACT(year FROM ts), \
                        EXTRACT(weekday FROM ts) \
                        FROM staging_events;")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

insert_print = ['songplays', 'users', 'songs', 'artists', 'time']
copy_print = ['staging_events', 'staging_songs']
