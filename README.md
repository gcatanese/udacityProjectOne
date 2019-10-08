Discuss the purpose of this database in the context of the startup, Sparkify, and their analytical goals.
State and justify your database schema design and ETL pipeline.
[Optional] Provide example queries and results for song play analysis.

Insert data using the COPY command to bulk insert log files instead of using INSERT on one row at a time
Add data quality checks
Create a dashboard for analytic queries on your new database

The README file includes a summary of the project, how to run the Python scripts, and an explanation of the files in the repository. Comments are used effectively and each function has a docstring.

https://www.python.org/dev/peps/pep-0008/


# Project: Data Modeling with Postgres


## Intro

The goal of the database is to allow Sparkify teams to analyse user behaviour (what they play and when). Other interesting metrics
are the devices (User Agent) and subscription type (Paid vs Free).

The data read from JSON includes duplicates and has missing data, this has been considered while designing the data pipeline.


## Data Model


The tables represent a star schema where 'songplays' is the Fact table and 'songs', 'artists', 'users' and 'time'
are the Dimension tables.

Each table has a Primary Key with the exception of 'time': there might be multiple songs played at the same time.
Table 'user' has a composite primary key (user-level) as there are users who have both subscription (first free, then upgraded to paid?)

Foreign Keys have also been defined (to improve data integrity): in order to support this I had to change the order of
the SQL insert in the data pipeline (etl.py).
Foreign key between 'songplays' and 'users' has been omitted: the data in 'songplays' includes orphan records (no matching user-level record).

## Pipeline

Data duplication is handled at DB level: when a record exists already the INSERT SQL doesnt fail (ON CONFLICT do nothing).
This could also be done in the data pipeline (for example user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates(subset=['userId', 'level']))

Missing data (songs and artists of songplays) is allowed (defined a None) but should be considered when writing the analytic queries


## Files

- sql_queries.py: data model definition
- create_table.py: (re)create sparkify DB and data model
- etl.py: runs the pipeline

## Run project

### Prerequisites

1. Postgres must run on localhost, default port
2. create STUDENT role:
"CREATE ROLE student WITH
  LOGIN
  SUPERUSER
  INHERIT
  CREATEDB
  CREATEROLE
  NOREPLICATION;
"
3. Create STUDENTDB database
CREATE DATABASE studentdb
    WITH 
    OWNER = student
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.utf8'
    LC_CTYPE = 'en_US.utf8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;
  
### Execution  

1. python create_table.py
2. python etl.py


## Sample Queries

Find below (available also in Workspace test.ipynb) the analytic queries which Sparkify team can run to collect meaningful insights on the user behaviour: 

### Busiest Months
SELECT year,month, count(*) FROM time group by year, month order by year, month limit 10;

### Distribution per day
SELECT weekday, count(*) FROM time group by weekday order by weekday;

### Distribution weekday VS weekend

### Distribution weekday VS weekend
select 'weekday' as when, sum(total) from (SELECT weekday, count(*) as total FROM time group by weekday order by weekday) as t where weekday in (0,1,2,3,4) \
union \
select 'weekend' as when, sum(total) from (SELECT weekday, count(*) as total FROM time group by weekday order by weekday) as t where weekday in (5,6)

### Most popular songs
SELECT songs.title, count(*) as total FROM songs JOIN songplays ON songs.song_id = songplays.song_id group by songs.title order by total limit 10;

### Most popular artists
SELECT artists.name, count(*) as total FROM artists JOIN songplays ON artists.artist_id = songplays.artist_id group by artists.name order by total limit 10;

### Paid vs Free
SELECT level, count(*) as total from users group by level

### Users with both Paid and Free subscriptions
select count(*) as DoubleSubscriptions from (SELECT user_id from users where level='paid' intersect SELECT user_id from users where level='free') as t 

### User Agent distribution (top 10)
SELECT user_agent, count(*) as userAgent from songplays group by user_agent order by count(*) desc limit 10



