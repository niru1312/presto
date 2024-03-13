from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName('AdvertiseX Data Processing') \
    .getOrCreate()

# Read data from Kafka topic into Spark DataFrame
df = spark \
    .readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('subscribe', 'ad_impressions') \
    .load()

# Process and transform data as needed
# Example: df.selectExpr('CAST(value AS STRING) AS json')...

# Start streaming query
query = df.writeStream \
    .outputMode('append') \
    .format('console') \
    .start()

query.awaitTermination()