import os
import sys
import platform

# Windows-specific fix for Hadoop
if platform.system() == "Windows":
    # Set HADOOP_HOME to a dummy directory to avoid FileNotFoundException
    if "HADOOP_HOME" not in os.environ:
        hadoop_home = os.path.join(os.path.dirname(__file__), "..", "hadoop")
        hadoop_home = os.path.abspath(hadoop_home)
        os.environ["HADOOP_HOME"] = hadoop_home
        os.environ["hadoop.home.dir"] = hadoop_home
        
        # Add hadoop/bin to PATH so DLLs can be found
        bin_dir = os.path.join(hadoop_home, "bin")
        if bin_dir not in os.environ.get("PATH", ""):
            os.environ["PATH"] = bin_dir + os.pathsep + os.environ.get("PATH", "")
        
        # Create the directory structure if it doesn't exist
        os.makedirs(bin_dir, exist_ok=True)
        
        # Check if winutils.exe exists
        winutils_path = os.path.join(bin_dir, "winutils.exe")
        if not os.path.exists(winutils_path):
            print("=" * 60)
            print("⚠️  WARNING: winutils.exe NOT FOUND")
            print("=" * 60)
            print(f"Expected location: {winutils_path}")
            print("\nPlease run the setup script first:")
            print("  python setup_hadoop_windows.py")
            print("\nOr download manually from:")
            print("  https://github.com/kontext-tech/winutils")
            print("=" * 60)
            print("\nAttempting to continue anyway...")
            print()
        else:
            print(f"[Windows Fix] ✓ Using HADOOP_HOME: {hadoop_home}")
            print(f"[Windows Fix] ✓ Found winutils.exe")


from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, sum as spark_sum, 
    to_timestamp, expr, current_timestamp, lit
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Define Schema
schema = StructType([
    StructField("sensor_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("vehicle_count", IntegerType()),
    StructField("avg_speed", FloatType())
])

# Initialize Spark
spark = SparkSession.builder \
    .appName("SmartCityTraffic") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0") \
    .config("spark.sql.streaming.checkpointLocation", "./data/checkpoints") \
    .config("spark.local.dir", "./data/tmp") \
    .config("spark.sql.warehouse.dir", "./data/spark-warehouse") \
    .config("spark.hadoop.io.native.lib.available", "false") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 1. Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic_data") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON and convert timestamp to proper format
json_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
json_df = json_df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

# 2. Apply 5-Minute Tumbling Windows and Calculate Congestion Index
# Congestion Index Formula: (vehicle_count / avg_speed) * 100
# Higher value = more congestion
windowed_df = json_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "5 minutes"),
        col("sensor_id")
    ) \
    .agg(
        spark_sum("vehicle_count").alias("total_vehicles"),
        avg("avg_speed").alias("avg_speed_window")
    ) \
    .withColumn("congestion_index", 
                expr("CASE WHEN avg_speed_window > 0 THEN (total_vehicles / avg_speed_window) * 10 ELSE total_vehicles * 10 END"))

# Flatten window structure for easier querying
windowed_df = windowed_df.select(
    col("sensor_id"),
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("total_vehicles"),
    col("avg_speed_window"),
    col("congestion_index")
)

# Function to write windowed aggregations to Postgres
def write_windows_to_postgres(batch_df, batch_id):
    if batch_df.count() > 0:
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/smartcity_db") \
            .option("dbtable", "traffic_windows") \
            .option("user", "admin") \
            .option("password", "password") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        print(f"[Batch {batch_id}] Written {batch_df.count()} windowed records to traffic_windows table")

# Start Windowed Data Stream to Postgres
window_query = windowed_df.writeStream \
    .foreachBatch(write_windows_to_postgres) \
    .outputMode("update") \
    .option("checkpointLocation", "./data/checkpoint_windows") \
    .start()

# 3. Detect Critical Traffic (Speed < 10 km/h) from RAW stream
# Add congestion index to individual records  
critical_df = json_df.withColumn(
    "congestion_index",
    expr("CASE WHEN avg_speed > 0 THEN (vehicle_count / avg_speed) * 10 ELSE vehicle_count * 10 END")
).filter(col("avg_speed") < 10)

# Function to write alerts to Postgres
def write_alerts_to_postgres(batch_df, batch_id):
    if batch_df.count() > 0:
        batch_df.select(
            col("sensor_id"),
            col("timestamp").cast("string").alias("timestamp"),
            col("vehicle_count"),
            col("avg_speed"),
            col("congestion_index")
        ).write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/smartcity_db") \
            .option("dbtable", "critical_alerts") \
            .option("user", "admin") \
            .option("password", "password") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        print(f"[Batch {batch_id}] ALERT! Written {batch_df.count()} critical traffic records (speed < 10 km/h)")

# Start Alert Stream
alert_query = critical_df.writeStream \
    .foreachBatch(write_alerts_to_postgres) \
    .outputMode("update") \
    .option("checkpointLocation", "./data/checkpoint_alerts") \
    .start()

# 4. Archive Raw Data to Parquet (Data Lake) for Batch Analysis
archive_query = json_df.writeStream \
    .format("parquet") \
    .option("path", "./data/traffic_history") \
    .option("checkpointLocation", "./data/checkpoint_archive") \
    .outputMode("append") \
    .partitionBy("sensor_id") \
    .start()

print("="*60)
print("Streaming Started Successfully!")
print("- 5-minute windowing with Congestion Index calculation")
print("- Critical traffic alerts (speed < 10 km/h)")
print("- Raw data archiving to Parquet")
print("Press Ctrl+C to stop.")
print("="*60)

spark.streams.awaitAnyTermination()