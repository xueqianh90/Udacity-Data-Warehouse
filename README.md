# Udacity-Data-Warehouse


# Cloud Data Warehouse on AWS

Create a database schema and ETL pipeline for a startup called Sparkify to extract data from S3, stage them in Redshift, and transform data into a set of dimensional tables. 

#### Database Schema Design (files used: sql_queries.py, create.py)

> **Staging Tables**
>> ***staging_events*** - app activity logs
>> ***staging_songs*** - contains metadata about a song and the artist of that song 

> **Fact Table**
>> ***songplays*** - playing records in log data associated with song title, artist name, and song duration time that match from the staging_events and staging_songs tables. 

> **Dimension Tables**
>> ***users*** - users info extracted from staging_events table
>> 
>> ***songs*** - songs info extracted from staging_songs table
>> 
>> ***artists*** - artists info extracted from staging_songs table
>> 
>> ***time*** - timestamps of records in songplays broken down into specific units

#### ETL Pipeline (files used: sql_queries.py, etl.py)

Get all the needed song and log datasets from from S3. Transform the data and then load data to each tables. 


## Data And Files Overview

**create.py** - Drops and creates tables. Run this file to reset tables before each time run ETL scripts.

**etl.py** - Loads and processes song and log datasets from from S3, and copys and inserts them into tables. 

**sql_queries.py** - Contains all sql queries (drop and create tables, copy data to staging tables from S3, and insert data into dimension tables from staging tables), and is imported into the last two files above.

## How to Run The Python Scripts

To run Python scripts, open a command-line and type in the word "python" followed by the path to the script.


## Example Queries And Results For Songplay Analysis

***SELECT u.gender, COUNT(*) 
FROM songplays AS s 
JOIN users AS u ON s.user_id=u.user_id 
GROUP BY u.gender;***

In November 2018, on Sparkify's new music streaming app, females palyed 847 times while males palyed 297 times.

***SELECT t.weekday, COUNT(*) 
FROM songplays AS s 
JOIN time AS t ON s.start_time=t.start_time 
GROUP BY t.weekday 
ORDER BY count DESC;***

In November 2018, on Sparkify's new music streaming app, the most active weekday is on Wedneday, and the least active weekday is on Sunday.

Wednesday - 250
Monday - 207
Tuesday - 191
Friday - 188
Thursday - 160
Saturday - 90
Sunday - 58
