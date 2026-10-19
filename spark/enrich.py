from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    abs,
    avg,
    coalesce,
    col,
    count,
    lit,
    max,
    split,
    to_date,
    unix_timestamp,
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

stop_times_schema = (
    "trip_id STRING, stop_id STRING, stop_sequence INT, pickup_type INT, "
    "drop_off_type INT, stop_headsign STRING, arrival_time STRING, departure_time STRING"
)

spark = (
    SparkSession.builder.appName("enrich-gtfs-rt")
    .master("spark://spark:7077")
    .config("spark.sql.session.timeZone", "Europe/Berlin")
    .getOrCreate()
)

silver = spark.read.schema(silver_schema).parquet(
    "/opt/spark/work-dir/data/silver/stop_updates"
)

stop_times = (
    spark.read.option("header", True)
    .schema(stop_times_schema)
    .csv("/opt/spark/work-dir/data/gtfs/stop_times.txt")
    .select(
        "trip_id",
        "stop_id",
        "stop_sequence",
        col("arrival_time").alias("static_arrival_time"),
        col("departure_time").alias("static_departure_time"),
    )
)

joined = silver.join(
    stop_times,
    on=[
        col("tripId") == col("trip_id"),
        col("stopSequence") == col("stop_sequence"),
    ],
    how="left",
)


def sec_of_day(gtfs_time):
    parts = split(gtfs_time, ":")
    return (
        parts.getItem(0).cast("int") * 3600
        + parts.getItem(1).cast("int") * 60
        + parts.getItem(2).cast("int")
    )


midnight = unix_timestamp(to_date(col("startDate"), "yyyyMMdd"))
sched_arrival = midnight + sec_of_day(col("static_arrival_time"))
sched_departure = midnight + sec_of_day(col("static_departure_time"))

enriched = joined.select(
    col("tripId"),
    col("routeId"),
    col("startDate"),
    col("stopId"),
    col("stopSequence"),
    col("arrival_delay"),
    col("departure_delay"),
    col("arrival_time"),
    col("departure_time"),
    sched_arrival.alias("sched_arrival_epoch"),
    sched_departure.alias("sched_departure_epoch"),
    (col("arrival_time") - sched_arrival).alias("arrival_dev"),
    (col("departure_time") - sched_departure).alias("departure_dev"),
    (
        when(
            col("arrival_delay").isNotNull() & (col("arrival_delay") < 32767),
            col("arrival_time") - sched_arrival - col("arrival_delay"),
        )
    ).alias("delta_to_reported"),
    col("static_arrival_time").isNotNull().alias("schedule_matched"),
    (col("stopId") == col("stop_id")).alias("stop_match"),
    col("scheduleRelationship"),
    col("ingested_at"),
    col("offset"),
)

valid = enriched.filter(
    col("schedule_matched") & (col("scheduleRelationship") == "SCHEDULED")
)

stats = enriched.agg(
    count(lit(1)).alias("n_total"),
    avg(coalesce(col("schedule_matched"), lit(False)).cast("int")).alias("match_rate"),
)
stats.show()

valid.agg(
    avg(col("stop_match").cast("int")).alias("stop_id_match_rate"),
    avg(abs(col("arrival_dev"))).alias("avg_abs_arrival_dev"),
    max(abs(col("arrival_dev"))).alias("max_abs_arrival_dev"),
).show()

summary = valid.groupBy("routeId").agg(
    count(lit(1)).alias("n_matched"),
    avg(abs(col("arrival_dev"))).alias("avg_abs_arrival_dev"),
    max(abs(col("arrival_dev"))).alias("max_abs_arrival_dev"),
)

enriched.write.mode("overwrite").parquet(
    "/opt/spark/work-dir/data/gold/delay_validation"
)

summary.write.mode("overwrite").parquet(
    "/opt/spark/work-dir/data/gold/delay_validation_summary"
)
