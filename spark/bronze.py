from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

spark = (
    SparkSession.builder.appName("bronze-gtfs-rt")
    .master("spark://spark:7077")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0")
    .getOrCreate()
)

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "gtfs-rt")
    .option(
        "startingOffsets", "earliest"
    )  # first run: take what main.py already produced
    .option("maxOffsetsPerTrigger", 10000)
    .option("failOnDataLoss", "false")
    .load()
)

bronze = raw.select(
    col("key").cast("string").alias("key"),
    col("value").cast("string").alias("value"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
    current_timestamp().alias("ingested_at"),
)

query = (
    bronze.writeStream.queryName("bronze-gtfs-rt")
    .format("parquet")
    .option("path", "/opt/spark/work-dir/data/bronze/gtfs-rt")
    .option("checkpointLocation", "/opt/spark/work-dir/data/checkpoints/bronze")
    .trigger(availableNow=True)
    .outputMode("append")
    .start()
)
query.awaitTermination()

progress = query.lastProgress
if progress is None or progress["numInputRows"] == 0:
    print("bronze: keine neuen Kafka-Daten gelesen (Checkpoint unveraendert)")
else:
    print(
        f"bronze: {progress['numInputRows']} rows gelesen -> "
        f"{progress['numOutputRows']} rows geschrieben, "
        f"Dauer {progress['durationMs']}"
    )
