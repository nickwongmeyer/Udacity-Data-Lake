import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format

# configurating all the credential in AWS 
config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']

# create a spark sesson using the spark and hadoop package
def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

# processing the data 
def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")
    
    # read song data file
    df = spark.read.json(song_data).dropDuplicates().cache()

    # extract columns to create songs table
    songs_table = df.select(col('song_id'),col('title'),col('duration'),col('year'),col('artist_id')).distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id').parquet(os.path.join(output_data,'songs.parquet'),'overwrite')

    # extract columns to create artists table
    artists_table = df.select(col('artist_id'),col('artist_latitude'),col('artist_longitude'),col('artist_location'),col('artist_name')).distinct()
    
    # write artists table to parquet files
    artists_table.parquet(os.path.join(output_data,'artists.parquet'),'overwrite')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data, "log_data/*/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data).dropDuplicates()
    
    # filter by actions for song plays
    df=df.filter(df.page =='NextSong').cache()

    # extract columns for users table    
    users_table = df.select(
                 col('userId'),
                 col('firstName'),
                 col('lastName'),
                 col('gender'),
                 col('level')
    ).distinct()
    
    # write users table to parquet files
    users_table.parquet(os.path.join(output_data,'users.parquet'),'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: str(int(int(x)/1000)))
    df = df.withColumn('timestamp',get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: str(datetime.fromtimestamp(int(x)/1000.0)))
    df = df.withColumn('datetime',getdatetime(df.timestamp))
    
    # extract columns to create time table
    time_table = df.select(col('timestamp').alias('start_time'),
                                            hour('datetime').alias('hour'), 
                                            dayofmonth('datetime').alias('day'), 
                                            weekofyear('datetime').alias('week'),
                                            month('datetime').alias('month'),
                                            year('datetime').alias('year'),
                                            date_format('datetime','E').alias('weekday'))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').parquet(os.path.join(output_data,'time/time.parquet'),'overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song_data/*/*/*/*.json")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sqlspark.sql("""
          SELECT DISTINCT 
          ts as start_time,
          month(to_timestamp(ld.ts/1000)) AS month,
          year(to_timestamp(ld.ts/1000)) AS year,
          ld.userId as user_id,
          ld.level,
          sd.song_id,
          sd.artist_id,
          ld.sessionId as session_id,
          ld.location,
          ld.userAgent as user_agent
          FROM log_data ld
          JOIN song_data sd ON 
          ld.artist = sd.artist_name
          and ld.song = sd.title
          and ld.length = sd.duration
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.partitionBy('year', 'month').parquet(os.path.join(output_data,'songplays/songplays.parquet'),'overwrite')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://nw-s3/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
