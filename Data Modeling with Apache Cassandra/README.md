# Data Modeling Using Cassandra
*by Shahleem Latif*

## Introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions, and wish to bring you on the project. Your role is to create a database for this analysis. You'll be able to test your database by running queries given to you by the analytics team from Sparkify to create the results.

## Project Steps

Below are steps you can follow to complete each component of this project.

### Modeling your NoSQL database or Apache Cassandra database

1.  Design tables to answer the queries outlined in the project template
2.  Write Apache Cassandra `CREATE KEYSPACE` and `SET KEYSPACE` statements
3.  Develop your `CREATE` statement for each of the tables to address each question
4.  Load the data with `INSERT` statement for each of the tables
5.  Include `IF NOT EXISTS` clauses in your `CREATE` statements to create tables only if the tables do not already exist. We recommend you also include `DROP TABLE` statement for each table, this way you can run drop and create tables whenever you want to reset your database and test your ETL pipeline
6.  Test by running the proper select statements with the correct `WHERE` clause

### Build ETL Pipeline

1.  Implement the logic in section Part I of the notebook template to iterate through each event file in `event_data` to process and create a new CSV file in Python
2.  Make necessary edits to Part II of the notebook template to include Apache Cassandra `CREATE` and `INSERT` statements to load processed records into relevant tables in your data model
3.  Test by running `SELECT` statements after running the queries on your database

## File Structure 

To get started with the project, go to the workspace on the next page, where you'll find the project template files. You can work on your project and submit your work through this workspace. Alternatively, you can download the project template files from the Resources folder if you'd like to develop your project locally.

In addition to the data files, the project workspace includes six files:

1. `sql_queries.py` contains all your sql queries, and is imported into the last three files above. 
2.  `create_tables.py` drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.
3.  `etl.py` reads and processes files from `song_data`  and `log_data` and loads them into your tables. You can fill this out based on your work in the ETL notebook.
4. `etl.ipynb` reads and processes a single file from `song_data` and `log_data` and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables.
5. `test.ipynb` displays the first few rows of each table to let you check your database.
6. `README.md` provides discussion on your project.
  
## Datasets

For this project, you'll be working with one dataset: `event_data`. The directory of CSV files partitioned by date. Here are examples of filepaths to two files in the dataset:

```
event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv
```
The project template includes one Jupyter Notebook file, in which:

-   you will process the `event_datafile_new.csv` dataset to create a denormalized dataset
-   you will model the data tables keeping in mind the queries you need to run
-   you have been provided queries that you will need to model your data tables for
-   you will load the data into tables you create in Apache Cassandra and run your queries

## Database Schema, Tables and Queries

### Schema
A keyspace is the top-level database object that controls the replication for the object it contains at each datacenter in the cluster. Keyspaces contain tables, materialized views and user-defined types, functions and aggregates. Typically, a cluster has one keyspace per application. Since replication is controlled on a per-keyspace basis, store data with different replication requirements (at the same datacenter) in different keyspaces. Keyspaces are not a significant map layer within the data model as stated on the [Datastax](https://docs.datastax.com/en/dse/5.1/cql/cql/cql_using/cqlKeyspacesAbout.html). The keyspace for this projects is **sparkify_events**.
```
cqlsh> SELECT * FROM system_schema.keyspaces;

 keyspace_name      | durable_writes | replication
--------------------+----------------+-------------------------------------------------------------------------------------
        system_auth |           True | {'class': 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '1'}
      system_schema |           True |                             {'class': 'org.apache.cassandra.locator.LocalStrategy'}
    sparkify_events |           True | {'class': 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '1'}
 system_distributed |           True | {'class': 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '3'}
             system |           True |                             {'class': 'org.apache.cassandra.locator.LocalStrategy'}
      system_traces |           True | {'class': 'org.apache.cassandra.locator.SimpleStrategy', 'replication_factor': '2'}

(6 rows)
cqlsh> 
```
### Tables
The three tables_names created for this project are music_app_session, music_app_user, and music_app_username.
```
cqlsh> SELECT * FROM system_schema.columns WHERE keyspace_name = 'sparkify_events' AND table_name IN  ('music_app_session','music_app_user','music_app_username');

 keyspace_name   | table_name         | column_name   | clustering_order | column_name_bytes            | kind          | position | type
-----------------+--------------------+---------------+------------------+------------------------------+---------------+----------+-------
 sparkify_events |  music_app_session |        artist |             none |               0x617274697374 |       regular |       -1 |  text
 sparkify_events |  music_app_session | iteminsession |              asc | 0x6974656d696e73657373696f6e |    clustering |        0 |   int
 sparkify_events |  music_app_session |     sessionid |             none |         0x73657373696f6e6964 | partition_key |        0 |   int
 sparkify_events |  music_app_session |    songlength |             none |       0x736f6e676c656e677468 |       regular |       -1 | float
 sparkify_events |  music_app_session |     songtitle |             none |         0x736f6e677469746c65 |       regular |       -1 |  text
 sparkify_events |  music_app_session | userfirstname |             none | 0x7573657266697273746e616d65 |       regular |       -1 |  text
 sparkify_events |  music_app_session |  userlastname |             none |   0x757365726c6173746e616d65 |       regular |       -1 |  text
 sparkify_events |     music_app_user |        artist |             none |               0x617274697374 |       regular |       -1 |  text
 sparkify_events |     music_app_user | iteminsession |              asc | 0x6974656d696e73657373696f6e |    clustering |        0 |   int
 sparkify_events |     music_app_user |     sessionid |             none |         0x73657373696f6e6964 | partition_key |        1 |   int
 sparkify_events |     music_app_user |     songtitle |             none |         0x736f6e677469746c65 |       regular |       -1 |  text
 sparkify_events |     music_app_user | userfirstname |             none | 0x7573657266697273746e616d65 |       regular |       -1 |  text
 sparkify_events |     music_app_user |        userid |             none |               0x757365726964 | partition_key |        0 |   int
 sparkify_events |     music_app_user |  userlastname |             none |   0x757365726c6173746e616d65 |       regular |       -1 |  text
 sparkify_events | music_app_username |     songtitle |             none |         0x736f6e677469746c65 | partition_key |        0 |  text
 sparkify_events | music_app_username | userfirstname |             none | 0x7573657266697273746e616d65 |       regular |       -1 |  text
 sparkify_events | music_app_username |        userid |              asc |               0x757365726964 |    clustering |        0 |   int
 sparkify_events | music_app_username |  userlastname |             none |   0x757365726c6173746e616d65 |       regular |       -1 |  text

(18 rows)
cqlsh> 
```
Here are the five query results from the music_app_session, music_app_user, and music_app_username tables.
```
cqlsh> SELECT * FROM sparkify_events.music_app_session LIMIT 5;

 sessionid | iteminsession | artist             | songlength | songtitle                         | userfirstname | userlastname
-----------+---------------+--------------------+------------+-----------------------------------+---------------+--------------
        23 |             0 |     Regina Spektor |  191.08527 |   The Calculation (Album Version) |         Layla |      Griffin
        23 |             1 |    Octopus Project |  250.95792 | All Of The Champs That Ever Lived |         Layla |      Griffin
        23 |             2 |     Tegan And Sara |  180.06158 |                        So Jealous |         Layla |      Griffin
        23 |             3 |         Dragonette |  153.39056 |                      Okay Dolores |         Layla |      Griffin
        23 |             4 | Lil Wayne / Eminem |  229.58975 |                    Drop The World |         Layla |      Griffin

(5 rows)
cqlsh> SELECT * FROM sparkify_events.music_app_user LIMIT 5;

 userid | sessionid | iteminsession | artist                | songtitle                     | userfirstname | userlastname
--------+-----------+---------------+-----------------------+-------------------------------+---------------+--------------
     58 |       768 |             0 |      System of a Down |                    Sad Statue |         Emily |       Benson
     58 |       768 |             1 | Ghostland Observatory |                Stranger Lover |         Emily |       Benson
     58 |       768 |             2 |     Evergreen Terrace |                          Zero |         Emily |       Benson
     85 |       776 |             2 |              Deftones |          Head Up (LP Version) |       Kinsley |        Young
     85 |       776 |             3 |  The Notorious B.I.G. | Playa Hater (Amended Version) |       Kinsley |        Young

(5 rows)
cqlsh> SELECT * FROM sparkify_events.music_app_username LIMIT 5;

 songtitle                           | userid | userfirstname | userlastname
-------------------------------------+--------+---------------+--------------
                  Wonder What's Next |     49 |         Chloe |       Cuevas
                 In The Dragon's Den |     49 |         Chloe |       Cuevas
   Too Tough (1994 Digital Remaster) |     44 |        Aleena |        Kirby
 Rio De Janeiro Blue (Album Version) |     49 |         Chloe |       Cuevas
                            My Place |     15 |          Lily |         Koch

(5 rows)
cqlsh> 
```






> Written with [StackEdit](https://stackedit.io/).
