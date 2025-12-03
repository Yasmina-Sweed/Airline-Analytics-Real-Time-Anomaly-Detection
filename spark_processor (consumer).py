import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# --- Configuration ---
# Topic must match the one used in stream_to_kafka.py
KAFKA_TOPIC = "airline-flights-2008"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# Define the schema to parse the JSON data coming from Snowflake
# We read everything as strings initially to avoid parsing errors.
json_schema = StructType([
    StructField("DATE", StringType(), True),
    StructField("UNIQUE_CARRIER", StringType(), True),
    StructField("FLIGHT_NUMBER", StringType(), True),
    StructField("ORIGIN", StringType(), True),
    StructField("DESTINATION", StringType(), True),
    StructField("ARRIVAL_DELAY", StringType(), True), # This is the key field for our alert
])

def create_spark_session():
    # Initialize Spark with the necessary Kafka connector package
    # This package must match the PySpark version installed (3.5.0)
    spark = SparkSession \
        .builder \
        .appName("SnowflakeToKafkaSparkAlerts") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()
    # Reduce logging verbosity to see our output clearly
    spark.sparkContext.setLogLevel("WARN")
    return spark

def process_stream(spark):
    print(f"Attempting to connect to Kafka topic: {KAFKA_TOPIC}...")

    # 1. Read Stream from Kafka
    # This connects to the local Kafka broker started with Docker
    df_raw = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    print("✅ Connected to Kafka Stream. Processing data...")

    # 2. Parse JSON Data
    # Kafka sends data as binary bytes. We cast to string, then parse using our schema.
    df_parsed = df_raw.select(from_json(col("value").cast("string"), json_schema).alias("data")).select("data.*")

    # 3. Anomaly Detection Logic
    # Filter for ARRIVAL_DELAY > 60. We must cast the string column to Double for comparison.
    anomaly_df = df_parsed.filter(col("ARRIVAL_DELAY").cast(DoubleType()) > 60.0)

    # Add an alert message column to the results
    alert_df = anomaly_df.withColumn("ALERT_MESSAGE", lit("HIGH DELAY DETECTED (>60m)"))

    # Select final columns for the output table
    final_output_df = alert_df.select(
        col("DATE"),
        col("UNIQUE_CARRIER"),
        col("FLIGHT_NUMBER"),
        col("ORIGIN"),
        col("DESTINATION"),
        col("ARRIVAL_DELAY"),
        col("ALERT_MESSAGE")
    )

    print("Starting stream query to console output. Please wait for batches...")

    # 4. Write Stream to Console
    # This will print the results to the terminal in near real-time.
    query = final_output_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    # Entry point of the script
    spark = create_spark_session()
    process_stream(spark)