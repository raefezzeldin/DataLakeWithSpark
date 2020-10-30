import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format,dayofweek
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['S3']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['S3']['AWS_SECRET_ACCESS_KEY']

"""Create Spark Session.

Output
    spark --Spark Session

"""
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

"""Process Song data 

Input 
    spark --Spark Session
    input_date --URL of the Source S3 Bucket
    output_date --URL of the target S3 Bucket

Output
"""
def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/A/A/A/*.json'
    
    # read song data file
    df = spark.read.json(song_data, mode='PERMISSIVE', columnNameOfCorruptRecord='corrupt_record').drop_duplicates()

    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id","year","duration").drop_duplicates()  
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "songs/", mode="overwrite", partitionBy=["year","artist_id"])

    # extract columns to create artists table
    artists_table = df.select("artist_id","artist_name","artist_location","artist_latitude","artist_longitude").drop_duplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists/", mode="overwrite")

"""Process Logs data 

Input 
    spark --Spark Session
    input_date --URL of the Source S3 Bucket
    output_date --URL of the target S3 Bucket

"""
def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data+ "log-data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data, mode='PERMISSIVE', columnNameOfCorruptRecord='corrupt_record').drop_duplicates()
    
    # filter by actions for song plays
    df = df.filter(col("page") == "NextSong")

    # extract columns for users table    
    artists_table = df.select("userId","firstName","lastName","gender","level").drop_duplicates()
    
    # write users table to parquet files
    artists_table.write.parquet(os.path.join(output_data, "users/") , mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.utcfromtimestamp(int(x)/1000), TimestampType())
    df = df.filter(col("page") == "NextSong")
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x : datetime.utcfromtimestamp(int(x)/1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp("ts"))
    
    # extract columns to create time table
    time_table = df.withColumn("hour",hour("start_time"))\
                    .withColumn("day",dayofmonth("start_time"))\
                    .withColumn("week",weekofyear("start_time"))\
                    .withColumn("month",month("start_time"))\
                    .withColumn("year",year("start_time"))\
                    .withColumn("weekday",dayofweek("start_time"))\
                    .select("ts","start_time","hour", "day", "week", "month", "year", "weekday").drop_duplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(os.path.join(output_data, "time_table/"), mode='overwrite', partitionBy=["year","month"])

    # read in song data to use for songplays table
    song_df = spark.read\
                .format("parquet")\
                .option("basePath", os.path.join(output_data, "songs/"))\
                .load(os.path.join(output_data, "songs/*/*/"))

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.join(song_df, [df.song == song_df.title,\
                          df.artist == song_df.title,\
                          df.length == song_df.duration], 'inner')\
                        .select(monotonically_increasing_id().alias("songplay_id"),col("start_time"),col("userId").alias("user_id"),\
                                "level","song_id","artist_id", col("sessionId").alias("session_id"),\
                                "location", col("userAgent").alias("user_agent"),\
                                month("start_time").alias("month"),year("start_time").alias("year"))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.drop_duplicates().write.parquet(os.path.join(output_data, "songplays/"), mode="overwrite", partitionBy=["year","month"])


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/output_final/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
