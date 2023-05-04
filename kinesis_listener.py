from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json
import json
if __name__ == "__main__":

    spark = SparkSession.builder.appName("fabrica-acme").getOrCreate()
    print("="*50)
    print("Iniciando computação")
    print("="*50)
    kinesis = spark \
        .readStream \
        .format('kinesis') \
        .option('streamName', 'fabrica-acme') \
        .option('endpointUrl', 'https://kinesis.us-west-1.amazonaws.com')\
        .option('region', 'us-west-1') \
        .option('awsAccessKeyId', 'XXXXXXXXXXXX')\
        .option('awsSecretKey', 'XXXXXXXXXXXX') \
        .option('startingposition', 'TRIM_HORIZON')\
        .load()
    schema = StructType([
            StructField("id", StringType()),
            StructField("temperatura", IntegerType()),
            StructField("random_string", StringType())
            ])
    
    
    query = kinesis.selectExpr('CAST(data AS STRING)')\
        .select(from_json('data', schema).alias('data'))\
        .select('data.*')\
        .groupBy("id").avg('temperatura')\
        .writeStream\
        .outputMode('complete')\
        .format('console')\
        .start()
    query.awaitTermination()
