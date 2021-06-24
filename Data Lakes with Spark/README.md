# Data Lake
*by Shahleem Latif*

## Introduction

Sparkify, a music streaming startup wants to update it database and has decided  to use Apache Spark.  I as the data engineer have been instructed to use S3 as a host resource, Spark and data lake as a ETL tool, and utilize Apache Spark procedure using cluster using AWS.          

## Project Steps

### Step 1

 - Create etl.py file in python and create schemas for 
    songs_data, artists_data, users_data, time_data, and songplays_data.  
 - Then extract columns to create songs table and artist tables.
 - Then transfer data from S3 into songs table and artists table.

###  Step 2
   
 - Then extract columns to create users_table, time_table, and songplays_table.
 - Then transfer data from S3 into users_table, time_table and songplays_table.
 
### Step 3
 - Create an Extract Transfer Load file using python.
 
 - ETL Process: Inserted data from the song_data folder and the log_data folder.


## File Structure 
The are fours files that are used to create this project for Sparkify.

1.  `etl.py` collects data from song_data and log_data folder that are stored in S3, then the data is performed in a series orders using Sparks.
2. `dl.cft`  is a configuration file that contains AWS access key and security key.
3. `README.md` provides discussion on your project.

## Datasets
As per the Project Datasets section at Udacity, there are two datasets that you that I you will be given to develop the project.  One will contain the song data and the other will have the log data.

The two datasets files are listed below and a json path file.

-   Song data: `s3://udacity-dend/song_data`
-   Log data: `s3://udacity-dend/log_data`

Log data json path: `s3://udacity-dend/log_json_path.json`

The first dataset is a subset of real data from the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.
```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json

```

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.
```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_lo
```
```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```
And below is an example of what the data in a log file, 2018-11-12-events.json, looks like.![](https://video.udacity-data.com/topher/2019/February/5c6c15e9_log-data/log-data.png)



  
> Written with [StackEdit](https://stackedit.io/).
