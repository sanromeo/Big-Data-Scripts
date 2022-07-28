from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.sql.functions import concat_ws, to_timestamp, sum, avg, col
from pyspark.sql.types import DecimalType

spark_Session = SparkSession.builder.getOrCreate()
sparkContext = spark_Session.sparkContext
glueContext = GlueContext(sparkContext)
session = glueContext.spark_session
print("Initializing SparkSession using GlueContext - Done!")
#reading data
df = session.read.parquet("s3://bdc21-roman-korsun-user-bucket/partition-input-data/")
print("READ DATA - Done!")
#concatenating columns 
df = df.select('device', 'ip_address', 'impressions', 'ad_blocking_rate', 'revenue', 'event_hour', 
                'event_date', concat_ws(' ', df.event_date, df.event_hour).alias('event_ts'))
df = df.withColumn('event_ts', to_timestamp('event_ts'))
print("CONCATENATE COLUMNS - Done!")
#aggregating data
df = df.groupBy('event_ts', 'device').agg(sum('impressions').alias('sum_impressions'),
                sum('revenue').alias('sum_revenue'), avg('ad_blocking_rate').alias('avg_ad_blocking_rate'))
print("AGGREGATING DATA - Done!")
#Adding column that named as ecpm
df = df.withColumn('ecpm', (col('sum_revenue') * 1000) / col('sum_impressions'))
print('ADDING COLUMN ecpm - DONE!')
#Casting columns
df = df.withColumn('sum_revenue', df.sum_revenue.cast(DecimalType(38, 2)))
df = df.withColumn('avg_ad_blocking_rate', df.avg_ad_blocking_rate.cast(DecimalType(38, 2)))
df = df.withColumn('ecpm', df.ecpm.cast(DecimalType(38, 2)))
df.printSchema()
print('CASTING COLUMNS - DONE!')
#repartition
df = df.repartition(1)
print('REPARTITION - DONE!')
#write to S3
df = df.write.parquet('s3a://bdc21-roman-korsun-user-bucket/device_agg_data')
print('WRITING DATA TO S3 - DONE!')
