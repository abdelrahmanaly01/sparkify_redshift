import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events;"
staging_songs_table_drop = "drop table if exists staging_songs;"
songplay_table_drop = "drop table if exists songplays;"
song_table_drop = "drop table if exists songs;"
user_table_drop = "drop table if exists users;"
artist_table_drop = "drop table if exists artists;"
time_table_drop = "drop table if exists time;"

# CREATE TABLES

staging_events_table_create= ("""
create table staging_events
(
artist text,
auth text ,
firstName text ,
gender text ,
itemInSession integer ,
lastName text ,
length float,
level text ,
location text ,
method text,
page text ,
registration text ,
sessionId integer ,
song text,
status text ,
ts bigint ,
userAgent text ,
userId integer 
)
""")

staging_songs_table_create = ("""
create table staging_songs (
num_songs integer ,
artist_id text ,
artist_latitude float,
artist_longitude float,
artist_location text,
artist_name text ,
song_id text,
title text ,
duration float ,
year integer
)
""")

songplay_table_create = ("""
create table songplays (
songplay_id integer identity(0,1), 
start_time bigint not null      sortkey, 
user_id int not null, 
level text not null,
song_id text distkey, 
artist_id text , 
session_id int, 
location text, 
user_agent text,
primary key (songplay_id),
CONSTRAINT fk_user_id
    FOREIGN KEY(user_id)
    REFERENCES users(user_id),
CONSTRAINT fk_song_id
    FOREIGN KEY(song_id)
    REFERENCES songs(song_id),
CONSTRAINT fk_start_time
    FOREIGN KEY(start_time)
    REFERENCES time(start_time),
CONSTRAINT fk_artist_id
    FOREIGN KEY(artist_id)
    REFERENCES artists(artist_id)
);
""")

user_table_create = ("""
create table users (
user_id int sortkey    distkey, 
first_name text, 
last_name text, 
gender text, 
level text,
primary key (user_id)
);
""")

song_table_create = ("""
create table songs (
song_id text          sortkey,
title text not null,
artist_id text not null,
year int not null      distkey,
duration float not null,
primary key (song_id)
) ;
""")

artist_table_create = ("""
create table artists (
artist_id text,
name text not null,
location text,
latitude float,
longitude float,
primary key (artist_id)
)           
diststyle all;
""")

time_table_create = ("""
create table time (start_time bigint  sortkey,
hour int, 
day int, 
week int,
month int, 
year int, 
weekday int,
primary key(start_time)
) 
diststyle all;
""")
# STAGING TABLES

staging_events_copy = ("""
copy staging_events 
from 's3://udacity-dend/log_data' 
iam_role {}
region 'us-west-2'
json 's3://udacity-dend/log_json_path.json';
""").format(config['IAM_ROLE']['ARN'])

staging_songs_copy = ("""
copy staging_songs 
from 's3://udacity-dend/song_data' 
iam_role {}
region 'us-west-2'
json 'auto';
""").format(config['IAM_ROLE']['ARN'])




# FINAL TABLES


songplay_table_insert = ("""
insert into
  songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
  )
select distinct
  se.ts,
  se.userId,
  se.level,
  ss.song_id,
  ss.artist_id,
  se.sessionId,
  se.location,
  se.userAgent
from
  staging_events se
  join staging_songs ss on se.song = ss.title 
""")

user_table_insert = ("""
insert into users (user_id , first_name, last_name, gender, level)
select userId, firstName,lastName , gender, level
from 
(select userId,
firstName,
lastName,
gender, 
level, 
row_number () OVER (partition by userId order by userId desc) as rnk
from staging_events)
WHERE rnk = 1 and userID is not null
""")

song_table_insert = ("""
insert into songs (song_id , title , artist_id , year , duration)
select song_id, title,artist_id, year,duration 
from staging_songs
""")

artist_table_insert = ("""
insert into artists (artist_id, name, location, latitude, longitude)
SELECT  artist_id, 
     artist_name, 
     artist_location,
     artist_latitude, 
     artist_longitude
FROM 
(
  SELECT 
     artist_id, 
     artist_name, 
     artist_location,
     artist_latitude, 
     artist_longitude,
     row_number () OVER (partition by artist_id order by artist_id) as rnk
  FROM staging_songs
) WHERE rnk = 1 and artist_id is not null
""")


time_table_insert = ("""
insert into time (start_time, hour, day, week, month, year, weekday)
select distinct ts, 
DATE_PART('hour',timestamp 'epoch' + ts/1000 * interval '1 second') 
, DATE_PART('day',timestamp 'epoch' + ts/1000 * interval '1 second') 
, DATE_PART('week',timestamp 'epoch' + ts/1000 * interval '1 second') 
,DATE_PART('month',timestamp 'epoch' + ts/1000 * interval '1 second') 
,DATE_PART('year',timestamp 'epoch' + ts/1000 * interval '1 second') 
,DATE_PART('dow',timestamp 'epoch' + ts/1000 * interval '1 second') 
from staging_events
where ts is not null

""")

# QUERY LISTS

create_table_queries = [
    staging_events_table_create, 
    staging_songs_table_create,
    user_table_create,
    song_table_create, 
    artist_table_create,
    time_table_create,
    songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
