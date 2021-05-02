import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events_stg;"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs_stg;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS events_stg(artist TEXT,      
                                                         auth TEXT,
                                                         first_name TEXT,
                                                         gender TEXT,
                                                         item_in_session INTEGER,
                                                         last_name TEXT,
                                                         length REAL,
                                                         level TEXT,  
                                                         location TEXT,
                                                         method TEXT,
                                                         page TEXT,
                                                         registration BIGINT,
                                                         session_id INTEGER,
                                                         song TEXT,
                                                         status INTEGER,
                                                         ts BIGINT,
                                                         user_agent TEXT,
                                                         user_id INTEGER);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS songs_stg(song_id TEXT,
                                                        num_songs INTEGER,
                                                        title TEXT,
                                                        artist_name TEXT,
                                                        artist_latitude REAL,
                                                        year INTEGER,
                                                        duration REAL,
                                                        artist_id TEXT,
                                                        artist_longitude REAL,
                                                        artist_location TEXT);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay(songplay_id INTEGER IDENTITY(1,1) PRIMARY KEY,
                                                        start_time TIMESTAMP NOT NULL REFERENCES time(start_time) sortkey,
                                                        user_id INTEGER NOT NULL REFERENCES users(user_id),
                                                        level TEXT ,
                                                        song_id TEXT NOT NULL REFERENCES songs(song_id) distkey,
                                                        artist_id TEXT NOT NULL REFERENCES artists(artist_id),
                                                        session_id INTEGER ,
                                                        location TEXT ,
                                                        user_agent TEXT );
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(user_id INTEGER PRIMARY KEY sortkey,
                                              first_name TEXT ,
                                              last_name TEXT ,
                                              gender TEXT ,
                                              level TEXT ) diststyle all;
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(song_id TEXT PRIMARY KEY sortkey distkey,
                                              title TEXT ,
                                              artist_id TEXT ,
                                              year INTEGER ,
                                              duration REAL );
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(artist_id TEXT PRIMARY KEY sortkey,
                                                  name TEXT ,
                                                  location TEXT ,
                                                  latitude REAL ,
                                                  longitude REAL );
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(start_time TIMESTAMP PRIMARY KEY sortkey,
                                              hour INTEGER ,
                                              day INTEGER ,
                                              week INTEGER ,
                                              month INTEGER ,
                                              year INTEGER ,
                                              weekday INTEGER ) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""COPY {} FROM {}
                          IAM_ROLE '{}'
                          JSON {} REGION 'us-west-2';
                          """).format('events_stg',
                                      config["S3"]["LOG_DATA"],
                                      config["IAM_ROLE"]["ARN"],
                                      config["S3"]["LOG_JSONPATH"])

staging_songs_copy = ("""COPY {} FROM {} 
                         IAM_ROLE '{}'
                         JSON 'auto' REGION 'us-west-2';
                         """).format('songs_stg',
                                     config["S3"]["SONG_DATA"],
                                     config["IAM_ROLE"]["ARN"])

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay 
                         (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                         SELECT (TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second') AS start_time,
                                e.user_id,
                                e.level,
                                s.song_id,
                                s.artist_id,
                                e.session_id,
                                e.location,
                                e.user_agent       
                         FROM songs_stg s RIGHT JOIN events_stg e 
                                          ON (s.artist_name = e.artist AND 
                                              s.title = e.song)
                        WHERE e.page = 'NextSong' AND
                              e.ts IS NOT NULL AND
                              s.song_id IS NOT NULL AND
                              s.artist_id IS NOT NULL;""")

user_table_insert = ("""INSERT INTO users
                     (user_id, first_name, last_name, gender, level)
                     SELECT DISTINCT(user_id),
                            first_name,
                            last_name,
                            gender,
                            level
                     FROM events_stg
                     WHERE user_id IS NOT NULL;""")

song_table_insert = ("""INSERT INTO songs
                     (song_id, title, artist_id, year, duration)
                     SELECT DISTINCT(song_id),
                            title,
                            artist_id,
                            year,
                            duration
                    FROM songs_stg
                    WHERE song_id IS NOT NULL;""")

artist_table_insert = ("""INSERT INTO artists
                       (artist_id, name, location, latitude, longitude)
                       SELECT DISTINCT(artist_id),
                              artist_name AS name,
                              artist_location AS location,
                              artist_latitude AS latitude,
                              artist_longitude AS longitude
                      FROM songs_stg
                      WHERE artist_id IS NOT NULL;""")

time_table_insert = ("""INSERT INTO time
                     (start_time, hour, day, week, month, year, weekday)
                     WITH tmp_tbl AS (SELECT (TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second') as ts FROM events_stg)
                     SELECT DISTINCT(ts) AS start_time,
                            EXTRACT(HOUR FROM ts) AS hour,
                            EXTRACT(DAY FROM ts) AS day,
                            EXTRACT(WEEK FROM ts) AS  week,
                            EXTRACT(MONTH FROM ts) AS month,
                            EXTRACT(YEAR FROM ts) AS year,
                            EXTRACT(DOW FROM ts) AS weekday
                    FROM tmp_tbl;""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop, staging_songs_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
