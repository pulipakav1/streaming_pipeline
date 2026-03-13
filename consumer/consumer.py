import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType
)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# config
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
S3_BUCKET               = os.getenv("S3_BUCKET", "your-bucket-name")
S3_PREFIX               = os.getenv("S3_PREFIX", "ecommerce-streaming")
CHECKPOINT_BASE         = f"s3a://{S3_BUCKET}/{S3_PREFIX}/checkpoints"
OUTPUT_BASE             = f"s3a://{S3_BUCKET}/{S3_PREFIX}/processed"

TOPICS = "ecommerce.orders,ecommerce.clicks,ecommerce.payments"

# schemas 

ORDER_SCHEMA = StructType([
    StructField("event_type",   StringType()),
    StructField("event_id",     StringType()),
    StructField("timestamp",    StringType()),
    StructField("user_id",      StringType()),
    StructField("session_id",   StringType()),
    StructField("order_id",     StringType()),
    StructField("product_id",   StringType()),
    StructField("product_name", StringType()),
    StructField("category",     StringType()),
    StructField("quantity",     IntegerType()),
    StructField("unit_price",   DoubleType()),
    StructField("discount",     DoubleType()),
    StructField("subtotal",     DoubleType()),
    StructField("total_amount", DoubleType()),
    StructField("currency",     StringType()),
    StructField("status",       StringType()),
    StructField("city",         StringType()),
    StructField("device",       StringType()),
])

CLICK_SCHEMA = StructType([
    StructField("event_type",   StringType()),
    StructField("event_id",     StringType()),
    StructField("timestamp",    StringType()),
    StructField("user_id",      StringType()),
    StructField("session_id",   StringType()),
    StructField("product_id",   StringType()),
    StructField("product_name", StringType()),
    StructField("category",     StringType()),
    StructField("page",         StringType()),
    StructField("device",       StringType()),
    StructField("os",           StringType()),
    StructField("browser",      StringType()),
    StructField("city",         StringType()),
    StructField("time_on_page", IntegerType()),
])

PAYMENT_SCHEMA = StructType([
    StructField("event_type",      StringType()),
    StructField("event_id",        StringType()),
    StructField("timestamp",       StringType()),
    StructField("user_id",         StringType()),
    StructField("order_id",        StringType()),
    StructField("payment_id",      StringType()),
    StructField("amount",          DoubleType()),
    StructField("currency",        StringType()),
    StructField("payment_method",  StringType()),
    StructField("status",          StringType()),
    StructField("failure_reason",  StringType()),
    StructField("city",            StringType()),
    StructField("device",          StringType()),
])


# spark session

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("EcommerceStreamingPipeline")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_BASE)
        .config("spark.sql.shuffle.partitions", "4")  
        .getOrCreate()
    )


# raw kafka stream

def read_kafka_stream(spark: SparkSession):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPICS)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(key AS STRING) as user_id_key",
                    "CAST(value AS STRING) as raw_json",
                    "topic",
                    "timestamp as kafka_timestamp")
    )


# transformations

def parse_and_enrich(raw_df, schema, event_type: str):
    parsed = (
        raw_df
        .filter(F.col("topic") == f"ecommerce.{event_type}s")
        .select(F.from_json(F.col("raw_json"), schema).alias("data"),
                "kafka_timestamp")
        .select("data.*", "kafka_timestamp")
    )

    enriched = (
        parsed
        .withColumn("event_ts",   F.to_timestamp("timestamp"))
        .withColumn("ingest_ts",  F.current_timestamp())
        .withColumn("date",       F.to_date("event_ts"))
        .withColumn("hour",       F.hour("event_ts"))
        .withColumn("is_weekend", F.dayofweek("event_ts").isin([1, 7]))
        .drop("timestamp")
    )

    # Event-specific enrichments
    if event_type == "order":
        enriched = enriched.withColumn(
            "price_tier",
            F.when(F.col("unit_price") < 25,  "budget")
             .when(F.col("unit_price") < 75,  "mid")
             .otherwise("premium")
        )
    elif event_type == "payment":
        enriched = enriched.withColumn(
            "is_successful", F.col("status") == "success"
        )
    elif event_type == "click":
        enriched = enriched.withColumn(
            "engagement",
            F.when(F.col("time_on_page") < 10,  "bounce")
             .when(F.col("time_on_page") < 60,  "low")
             .when(F.col("time_on_page") < 180, "medium")
             .otherwise("high")
        )

    return enriched


# Write to S3

def write_stream(df, event_type: str, checkpoint_suffix: str):
    """Write stream to S3 partitioned by date and hour."""
    return (
        df.writeStream
        .format("parquet")
        .option("path", f"{OUTPUT_BASE}/{event_type}s/")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/{checkpoint_suffix}/")
        .partitionBy("date", "hour")
        .outputMode("append")
        .trigger(processingTime="30 seconds")   # micro-batch every 30s
        .start()
    )


# main

def main():
    logger.info("Starting E-Commerce Streaming Consumer")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = read_kafka_stream(spark)

    # Parse each topic with its own schema
    orders_df   = parse_and_enrich(raw_stream, ORDER_SCHEMA,   "order")
    clicks_df   = parse_and_enrich(raw_stream, CLICK_SCHEMA,   "click")
    payments_df = parse_and_enrich(raw_stream, PAYMENT_SCHEMA, "payment")

    # Start 3 streaming writers
    orders_query   = write_stream(orders_df,   "order",   "orders")
    clicks_query   = write_stream(clicks_df,   "click",   "clicks")
    payments_query = write_stream(payments_df, "payment", "payments")

    logger.info("All 3 streaming queries started. Writing to S3...")
    logger.info(f"Output path: {OUTPUT_BASE}")

    # Wait for all streams
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
