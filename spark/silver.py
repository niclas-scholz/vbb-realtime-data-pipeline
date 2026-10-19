from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_json
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

stop_time_schema = StructType(
    [
        StructField("stopSequence", IntegerType(), True),
        StructField("stopId", StringType(), True),
        StructField("scheduleRelationship", StringType(), True),
        StructField(
            "arrival",
            StructType(
                [
                    StructField("time", StringType(), True),
                    StructField("delay", IntegerType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "departure",
            StructType(
                [
                    StructField("time", StringType(), True),
                    StructField("delay", IntegerType(), True),
                ]
            ),
            True,
        ),
    ]
)

entity_schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField(
            "tripUpdate",
            StructType(
                [
                    StructField(
                        "trip",
                        StructType(
                            [
                                StructField("tripId", StringType(), True),
                                StructField("routeId", StringType(), True),
                                StructField("startDate", StringType(), True),
                                StructField("scheduleRelationship", StringType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField("stopTimeUpdate", ArrayType(stop_time_schema), True),
                ]
            ),
            True,
        ),
    ]
)

bronze_schema = StructType(
    [
        StructField("key", StringType(), True),
        StructField("value", StringType(), True),
        StructField("topic", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("offset", LongType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("ingested_at", TimestampType(), True),
    ]
)

spark = (
    SparkSession.builder.appName("silver-gtfs-rt")
    .master("spark://spark:7077")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0")
    .getOrCreate()
)

bronze = (
    spark.readStream.format("parquet")
    .schema(bronze_schema)
    .load("/opt/spark/work-dir/data/bronze/gtfs-rt")
)

parsed = bronze.select(
    from_json(col("value"), entity_schema).alias("e"),
    col("ingested_at"),
    col("offset"),
).select(
    col("e.tripUpdate.trip.tripId").alias("tripId"),
    col("e.tripUpdate.trip.routeId").alias("routeId"),
    col("e.tripUpdate.trip.startDate").alias("startDate"),
    explode(col("e.tripUpdate.stopTimeUpdate")).alias("stu"),
    col("ingested_at"),
    col("offset"),
)

silver = parsed.select(
    col("tripId"),
    col("routeId"),
    col("startDate"),
    col("stu.stopId").alias("stopId"),
    col("stu.stopSequence").alias("stopSequence"),
    col("stu.arrival.delay").alias("arrival_delay"),
    col("stu.arrival.time").cast("long").alias("arrival_time"),
    col("stu.departure.delay").alias("departure_delay"),
    col("stu.departure.time").cast("long").alias("departure_time"),
    col("stu.scheduleRelationship").alias("scheduleRelationship"),
    col("ingested_at"),
    col("offset"),
).dropna(subset=["tripId", "stopId"])

silver.writeStream.format("parquet").option(
    "path", "/opt/spark/work-dir/data/silver/stop_updates"
).option("checkpointLocation", "/opt/spark/work-dir/data/checkpoints/silver").trigger(
    availableNow=True
).outputMode("append").start().awaitTermination()
