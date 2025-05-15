dimport configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['default']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['default']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

'''
Create Schemas

    - Songs_Data
    - Artists_Data
    - Users_Data
    - Time_Data
    - Songplays_Data
'''
schemaSongs_Data = StructType([
        StructField('song_id',StringType(), True),
        StructField('title', StringType(), True),
        StructField('artist_id', StringType(), True),
        StructField('year', IntegerType(), True),
        StructField('duration', DecimalType(), True)
        ])

schemaArtists_Data = StructType([
        StructField("artist_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("location", StringType(), True),
        StructField("latitude", DecimalType(), True),
        StructField("longitude", DecimalType(), True)
        ])
schemaUsers_Data = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("level", StringType(), True)
        ])
schemaTime_Data = StructType([
        StructField("start_time", TimestampType(), True),
        StructField("hour", TimestampType(), True),
        StructField("day", TimestampType(), True),
        StructField("week", TimestampType(), True),
        StructField("month", TimestampType(), True),
        StructField("year", TimestampType(), True),
        StructField("weekday", TimestampType(), True)
        ])

schema_Songplays_Data = StructType([
        StructField("start_time", TimestampType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("level", StringType(), True),
        StructField("song_id", StringType(), True),
        StructField("artist_id", StringType(), True),
        StructField("session_id", IntegerType(), True),
        StructField("location", StringType(), True)
        ])

def process_song_data(spark, input_data, output_data):

    '''
     Insert data from song_data folder from S3 Bucket s3a://sparkifymusicstream/

        - Create songs_table
        - Create artists_table
    '''

    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*"

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(("song_id"),
                            ("title"),
                            ("artist_id"),
                            ("year"),
                            ("duration")).dropDuplicates(["song_id"])

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data + "songs")

    # extract columns to create artists table
    artists_table = df.select(("artist_id"),
                              ("name"),
                              ("location"),
                              ("latitude"),
                              ("longitude")).dropDuplicates(["artist_id"])

    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists")


def process_log_data(spark, input_data, output_data):
    '''
    Insert data from log_data folder from S3 Bucket s3a://sparkifymusicstream/

        - Create  users_table
        - Create  time_table
        - Create songplays_table
    '''

    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*"

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    songplays_table = df.select(("ts"),
                                ("userId"),
                                ("level"),
                                ("sessionId"),
                                ("location"),
                                ("userAgent")).dropDuplicates(["ts"])

    # extract columns for users table
    users_table = df.select(("userId"),
                            ("firstName"),
                            ("lastName"),
                            ("gender"),
                            ("level")).dropDuplicates(["userid"])

    # write users table to parquet files
    users_table.write.parquet(os.path.join(output_data + 'users.parquet'))

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x) / 1000)))
    df = df.withColumn('timestamp', get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x) / 1000.0)))
    df = df.withColumn("datetime", get_datetime(df.ts))

    # extract columns to create time table
    time_table = df.select(
        col('datetime').alias('start_time'),
        hour('datetime').alias('hour'),
        dayofmonth('datetime').alias('day'),
        weekofyear('datetime').alias('week'),
        month('datetime').alias('month'),
        year('datetime').alias('year')).dropDuplicates(["start_time"])
   
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data + 'time.parquet'))

    # read in song data to use for songplays table
    song_data = os.path.join(input_data + "song_data/*/*/*")
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table
    df = df.join(song_df, song_df.title == df.song)

    songplays_table = df.select(
        col('ts').alias('start_time'),
        col('userId').alias('user_id'),
        col('level').alias('level'),
        col('song_id').alias('song_id'),
        col('artist_id').alias('artist_id'),
        col('ssessionId').alias('session_id'),
        col('location').alias('location'),
        col('userAgent').alias('user_agent'),
        col('year').alias('year'),
        month('datetime').alias('month')).dropDuplicates(["start_time"])

    songplays_table = songplays_table.selectExpr("ts as start_time")
    songplays_table.select(monotonically_increasing_id().alias('songplay_id')).collect()

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(os.path.join(output_data + 'songplays.parquet'))
    print("process_log_data completed")

    
def main():
    """
    Extract songs and events data from S3, Transform it into dimensional tables format, and Load it back to S3 in Parquet format
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
