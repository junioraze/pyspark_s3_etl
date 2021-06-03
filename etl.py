import os
import configparser
from datetime import datetime, date
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import TimestampType, DateType
from pyspark.sql.functions import (year, month, dayofmonth, 
                                  hour, weekofyear, date_format, dayofweek)


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['S3']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['S3']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create a spark session.
 
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process song data from json files.
        spark: spark_session object
        input_data: input s3 directory in aws (dl.cfg)
        output_data: output s3 directory in aws (dl.cfg)
 
    """
    # get filepath to song data file
    song_data = input_data + "/song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]) \
                    .distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data+'songs',
                              mode='overwrite',
                              partitionBy=['year','artist_id'])

    # extract columns to create artists table
    artists_table = df.select(["artist_id", "artist_name", "artist_location", 
                               "artist_latitude", "artist_longitude"]) \
                      .distinct()
    
    # rename columns to match with the schema
    artists_table = artists_table.withColumnRenamed("artist_name", "name") \
                                 .withColumnRenamed("artist_location", "location") \
                                 .withColumnRenamed("artist_latitude", "latitude") \
                                 .withColumnRenamed("artist_longitude", "longitude")
    
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data+'artists',
                                mode='overwrite')
    
    # create temp song view to use in process_log_data
    songs_table.createOrReplaceTempView("songs")

def process_log_data(spark, input_data, output_data):
    """
    Process log data from json files.
        spark: spark_session object
        input_data: input s3 directory in aws (dl.cfg)
        output_data: output s3 directory in aws (dl.cfg)
 
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter("page='NextSong'")

    #create temp view to extract actual users state level
    df.select(["ts", "userId", "firstName", 
               "lastName", "gender", "level"]) \
      .createOrReplaceTempView("users")
    
    # extract columns for users table    
    users_table = spark.sql(
        """
            WITH actual_levels AS (
                SELECT ROW_NUMBER() OVER (PARTITION BY userId ORDER BY ts DESC) AS timeOrderRowNum,
                       userId,
                       firstName,
                       lastName,
                       gender,
                       level
                FROM users
            )
            
            SELECT DISTINCT userId as user_id, 
                            firstName as first_name, 
                            lastName as last_name,
                            gender,
                            level
            FROM actual_levels
            WHERE timeOrderRowNum = 1
        """
    )
    
    # write users table to parquet files
    users_table.write.parquet(output_data+'users',
                              mode='overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: datetime.fromtimestamp(ts / 1000.0), TimestampType())
    df = df.withColumn('start_time',get_timestamp('ts'))
    
    # create datetime column from original timestamp column
    # year, month, dayofmonth, hour, weekofyear
    get_datetime = udf(lambda ts: date.fromtimestamp(ts / 1000.0), TimestampType())
    df = df.withColumn('date_time',get_timestamp('ts')) \
           .withColumn('hour', hour('date_time')) \
           .withColumn('day', dayofmonth('date_time')) \
           .withColumn('week', weekofyear('date_time')) \
           .withColumn('month', month('date_time')) \
           .withColumn('year', year('date_time')) \
           .withColumn('weekday', dayofweek('date_time'))
    
    
    # extract columns to create time table
    time_table = df.select(['start_time','hour','day','week',
                           'month','year','weekday']) \
                   .distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data+"times", 
                              mode='overwrite',
                              partitionBy=["year", "month"]
                            )


    # read in song data to use for songplays table 
    df.select(['start_time','song','page','userId','level',
               'sessionId','location','userAgent','month','year',
               'song','length']) \
      .createOrReplaceTempView('stage_songplay')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql(
            """
            SELECT ROW_NUMBER() OVER (ORDER BY start_time DESC) as songplay_id,
                   ss.start_time, 
                   ss.userId AS user_id, 
                   ss.level AS level, 
                   s.song_id AS song_id, 
                   s.artist_id AS artist_id, 
                   ss.sessionId AS session_id, 
                   ss.location AS location, 
                   ss.userAgent AS user_agent,
                   ss.year,
                   ss.month
            FROM stage_songplay ss
            LEFT JOIN songs s 
                   ON ss.song = s.title
                  AND ss.length = s.duration
                  
            """
        )


    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + "songplays", 
                                  mode='overwrite',
                                  partitionBy=["year", "month"]
                                 )


def main():
    """
    Main function to start ETL process.
    """
    bucket_str_format = "s3a://{}/"
    spark = create_spark_session()
    input_data = bucket_str_format.format(config['S3']['input_bucket'])
    output_data = bucket_str_format.format(config['S3']['output_bucket'])
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
