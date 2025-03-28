import signal
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Summarizer
from pymongo import MongoClient
import time
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
MONGODB_URI = os.getenv("MONGODB_URI")

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Real-Time Census Analytics") \
    .config("spark.mongodb.output.uri", MONGODB_URI) \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Define schema explicitly
schema = StructType([
    StructField("age", IntegerType(), True),
    StructField("workclass", StringType(), True),
    StructField("education", StringType(), True),
    StructField("marital_status", StringType(), True),
    StructField("occupation", StringType(), True),
    StructField("relationship", StringType(), True),
    StructField("race", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("capital_gain", IntegerType(), True),
    StructField("capital_loss", IntegerType(), True),
    StructField("hours_per_week", IntegerType(), True),
    StructField("native_country", StringType(), True),
    StructField("income", IntegerType(), True),
    StructField("capital_income", IntegerType(), True)
])

# Read streaming data from a directory
streaming_df = spark.readStream \
    .schema(schema) \
    .option("header", "false") \
    .csv("/Users/tharushavihanga/Developer/spark_mongo/stream_data")

# Enhance data with derived features
processed_df = streaming_df \
    .withColumn("timestamp", current_timestamp()) \
    .withColumn("age_group", 
                when(col("age") < 18, "Under 18")
                .when(col("age") < 30, "18-29")
                .when(col("age") < 45, "30-44")
                .when(col("age") < 65, "45-64")
                .otherwise("65+")) \
    .withColumn("income_category", 
                when(col("income") == 1, "High Income (>50K)")
                .otherwise("Low Income (<=50K)")) \
    .withColumn("work_hours_category", 
                when(col("hours_per_week") < 20, "Part-time (<20)")
                .when(col("hours_per_week") <= 40, "Full-time (20-40)")
                .otherwise("Overtime (>40)")) \
    .withColumn("capital_income_category", 
                when(col("capital_income") < 0, "Loss")
                .when(col("capital_income") == 0, "Break-even")
                .when(col("capital_income") < 5000, "Low Gain")
                .when(col("capital_income") < 20000, "Medium Gain")
                .otherwise("High Gain"))

# Generate statistics for numeric columns
# Assemble numeric features for statistical analysis
numeric_cols = ["age", "capital_gain", "capital_loss", "hours_per_week", "capital_income"]

# Function to compute stats for each batch
def compute_batch_stats(df, epoch_id):
    # Calculate summary statistics
    stats = df.select([
        mean("age").alias("avg_age"),
        stddev("age").alias("stddev_age"),
        min("age").alias("min_age"),
        max("age").alias("max_age"),
        mean("hours_per_week").alias("avg_hours"),
        stddev("hours_per_week").alias("stddev_hours"),
        mean("capital_income").alias("avg_capital_income"),
        stddev("capital_income").alias("stddev_capital_income")
    ]).collect()[0].asDict()
    
    # Timestamp for this batch
    timestamp = time.time()
    stats["timestamp"] = timestamp
    
    # Calculate income distribution
    income_dist = df.groupBy("income_category").count().collect()
    for row in income_dist:
        stats[f"count_{row['income_category'].replace(' ', '_')}"] = row["count"]

    # Save stats to MongoDB
    client = MongoClient(MONGODB_URI)
    db = client["census"]
    stats_collection = db["summary_statistics"]
    stats_collection.insert_one(stats)

    # Detect potential anomalies (simple z-score approach)
    # Hours outliers (z-score > 3)
    if stats["stddev_hours"] > 0:
        hours_outliers = df.withColumn(
            "hours_z_score", 
            abs((col("hours_per_week") - lit(stats["avg_hours"])) / lit(stats["stddev_hours"]))
        ).filter("hours_z_score > 3")
        
        # Save outliers to MongoDB
        if hours_outliers.count() > 0:
            anomalies_collection = db["anomalies"]
            for row in hours_outliers.collect():
                anomaly_record = row.asDict()
                anomaly_record["anomaly_type"] = "hours_outlier"
                anomaly_record["z_score"] = float(row["hours_z_score"])
                anomaly_record["detected_at"] = timestamp
                anomalies_collection.insert_one(anomaly_record)

# Function to write aggregated data to MongoDB
def write_aggregations_to_mongo(df, epoch_id):
    client = MongoClient(MONGODB_URI)
    db = client["census"]

    # Current timestamp for all aggregations
    timestamp = time.time()
    
    # Age group distribution
    age_group_data = df.groupBy("age_group").count().collect()
    age_group_collection = db["age_group_distribution"]
    for row in age_group_data:
        record = {
            "age_group": row["age_group"],
            "count": row["count"],
            "timestamp": timestamp
        }
        age_group_collection.insert_one(record)
        
    # Education vs Income
    edu_income_data = df.groupBy("education", "income_category").count().collect()
    edu_income_collection = db["education_income"]
    for row in edu_income_data:
        record = {
            "education": row["education"],
            "income_category": row["income_category"],
            "count": row["count"],
            "timestamp": timestamp
        }
        edu_income_collection.insert_one(record)
    
    # Gender and income
    gender_income_data = df.groupBy("gender", "income_category").count().collect()
    gender_income_collection = db["gender_income"]
    for row in gender_income_data:
        record = {
            "gender": row["gender"],
            "income_category": row["income_category"],
            "count": row["count"],
            "timestamp": timestamp
        }
        gender_income_collection.insert_one(record)
    
    # Work hours distribution
    hours_data = df.groupBy("work_hours_category").count().collect()
    hours_collection = db["work_hours"]
    for row in hours_data:
        record = {
            "work_hours_category": row["work_hours_category"],
            "count": row["count"],
            "timestamp": timestamp
        }
        hours_collection.insert_one(record)

    # Occupation statistics - average age and hours by occupation
    occupation_stats = df.groupBy("occupation").agg(
        avg("age").alias("avg_age"),
        avg("hours_per_week").alias("avg_hours"),
        count("*").alias("count")
    ).collect()
    
    occupation_collection = db["occupation_stats"]
    for row in occupation_stats:
        record = {
            "occupation": row["occupation"],
            "avg_age": float(row["avg_age"]) if row["avg_age"] is not None else None,
            "avg_hours": float(row["avg_hours"]) if row["avg_hours"] is not None else None,
            "count": row["count"],
            "timestamp": timestamp
        }
        occupation_collection.insert_one(record)

    # Store raw data
    raw_data_collection = db["raw_data"]
    for row in df.collect():
        raw_data_collection.insert_one(row.asDict())

# Start the streaming queries
stats_query = processed_df.writeStream \
    .outputMode("update") \
    .foreachBatch(compute_batch_stats) \
    .trigger(processingTime="10 seconds") \
    .start()

agg_query = processed_df.writeStream \
    .outputMode("update") \
    .foreachBatch(write_aggregations_to_mongo) \
    .trigger(processingTime="10 seconds") \
    .start()

# Graceful shutdown
def signal_handler(sig, frame):
    print("Stopping streaming queries...")
    stats_query.stop()
    agg_query.stop()
    spark.stop()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# Wait for queries to terminate
stats_query.awaitTermination()
agg_query.awaitTermination()