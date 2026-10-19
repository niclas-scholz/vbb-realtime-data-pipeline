from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    date_trunc,
    lit,
    max,
    percentile_approx,
    sum,
    when,
)
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

silver_schema = StructType(
    [
        StructField("tripId", StringType(), True),
        StructField("routeId", StringType(), True),
        StructField("startDate", StringType(), True),
        StructField("stopId", StringType(), True),
        StructField("stopSequence", IntegerType(), True),
        StructField("arrival_delay", IntegerType(), True),
        StructField("arrival_time", LongType(), True),
        StructField("departure_delay", IntegerType(), True),
        StructField("departure_time", LongType(), True),
        StructField("scheduleRelationship", StringType(), True),
        StructField("ingested_at", TimestampType(), True),
        StructField("offset", LongType(), True),
    ]
)

spark = (
    SparkSession.builder.appName("gold-gtfs-rt")
    .master("spark://spark:7077")
    .config("spark.sql.session.timeZone", "Europe/Berlin")
    .getOrCreate()
)

silver = spark.read.schema(silver_schema).parquet(
    "/opt/spark/work-dir/data/silver/stop_updates"
)

with_delay = silver.select(
    col("routeId"),
    col("tripId"),
    date_trunc("hour", col("ingested_at")).alias("hour"),
    col("scheduleRelationship"),
    col("departure_delay"),
)

# A skipped stop is not an on-time stop, so it must not dilute the averages.
# delay stays NULL when the feed carries no value: avg/percentile_* ignore NULLs,
# which keeps n_reported the honest denominator. 32767 is the GTFS-RT int16
# "no delay info" sentinel, not a nine-hour delay.
gold_delay = when(
    (col("scheduleRelationship") == "SCHEDULED") & (col("departure_delay") < 32767),
    col("departure_delay"),
)

gold = with_delay.groupBy("routeId", "hour").agg(
    count(lit(1)).alias("n_total"),
    countDistinct("tripId").alias("n_trips"),
    sum(when(col("scheduleRelationship") == "SKIPPED", 1).otherwise(0)).alias(
        "n_skipped"
    ),
    count(gold_delay).alias("n_reported"),
    avg(gold_delay).alias("avg_delay"),
    percentile_approx(gold_delay, 0.5).alias("p50_delay"),
    percentile_approx(gold_delay, 0.9).alias("p90_delay"),
    max(gold_delay).alias("max_delay"),
)

gold.write.mode("overwrite").parquet("/opt/spark/work-dir/data/gold/otp_by_route_hour")
