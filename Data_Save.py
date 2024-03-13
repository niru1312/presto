# Save processed data to HDFS
processed_data_path = '/user/advertisex/processed_data'
df.write.mode('overwrite').parquet(processed_data_path)

# Create Hive table on top of processed data in HDFS
spark.sql('CREATE TABLE ad_campaign_data USING PARQUET LOCATION "{}"'.format(processed_data_path))

# Query data using Spark SQL
result = spark.sql('SELECT * FROM ad_campaign_data WHERE ...')
