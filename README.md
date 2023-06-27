# sparkify ETL and DWH using  AWS Redshift
================
## summary :
================
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer,  I am tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to. I'll be able to test my database and ETL pipeline by running queries given to me by the analytics team from Sparkify and compare my results with their expected results.

==============================
## how to use the scripts :
==============================
1- first you will need to run the create_tables.py using the following command this will create the staging tables and the needed model tables (starschema)
       >> python create_table.py
       
2- run the etl.py to load the data from JSON files to staginhg tables and from staging tables to the Redshift dWh tables 
        >> python etl.py
        
=======================
## files Utilization:
=======================
**sql_queries.py:** contain the COPY, CREATE, DROP, INSERT and SELECT statements used in creating the 
staging and DWH tables also contains queries used for loading the data to Redshift.

**create_tables.py:** contains the functions which utilize the _sql_queries.py_ file qeuries to create the tables.

**etl.py:** loads the data from S3 to the tables in AWS Redshfit database using the _sql_queries.py_ queries.

![alt text](https://drive.google.com/file/d/1jf2-uv8li8DS6VVf1sZEEJY5cXIKStc_/view?usp=sharing)


===============
## challenges: 
===============
1- AWS Redshift Doesn't have the **ON CONFLICT** clause so i struggled a lot until i found this work around to
make sure the inserted columns are unique 

>> insert into artists (artist_id, name, location, latitude, longitude)
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
    
2- for the time table i found that Redshift also doesn't have the to_timestamp(bigint) so i couldn't convert the ts column to timestamp to extract the month, day and hour data from it but after some good searching i found this

>> DATE_PART('day',timestamp 'epoch' + ts/1000 * interval '1 second') 

**here is a test query and results**

![alt text](https://drive.google.com/file/d/1F9LmS2D6nZp0UF7NKQfLasSdPdYejnq2/view?usp=sharing )
![alt text](https://drive.google.com/file/d/12RKZfx-bTAxRv2ZBgptJHYTrsWVDvl18/view?usp=sharing)


