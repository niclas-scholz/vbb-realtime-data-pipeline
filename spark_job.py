from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder.appName("kafka_to_gtfs")
    .master("spark://spark:7077")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0")
    .getOrCreate()
)

df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "gtfs-rt")
    .load()
)

df.withColumn("value", col("value").cast("string")).writeStream.format(
    "console"
).start().awaitTermination()
