# Silver Layer Data Processing Notebook
# Real-Time Environmental Event Alerting System
# This notebook processes and cleans raw data from Bronze layer

# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Data Cleaning and Standardization
# MAGIC 
# MAGIC This notebook processes raw data from Bronze layer:
# MAGIC - Data validation and quality checks
# MAGIC - Standardization and enrichment
# MAGIC - Deduplication and consistency
# MAGIC - Geographic enrichment

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import logging
import requests

# Initialize Spark session for Microsoft Fabric
spark = SparkSession.builder \
    .appName("Environmental-Alert-Silver-Processing") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .getOrCreate()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print("✅ Spark session initialized successfully")
print(f"Spark version: {spark.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration and Schema Definitions

# COMMAND ----------

# Lakehouse paths
BRONZE_LAKEHOUSE_PATH = "Files/bronze/"
SILVER_LAKEHOUSE_PATH = "Files/silver/"
EARTHQUAKE_BRONZE_PATH = f"{BRONZE_LAKEHOUSE_PATH}earthquakes/"
WEATHER_BRONZE_PATH = f"{BRONZE_LAKEHOUSE_PATH}weather/"
EARTHQUAKE_SILVER_PATH = f"{SILVER_LAKEHOUSE_PATH}earthquakes/"
WEATHER_SILVER_PATH = f"{SILVER_LAKEHOUSE_PATH}weather/"

# Processing parameters
BATCH_TIMESTAMP = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
DEDUPLICATION_WINDOW_HOURS = 24

# Data quality thresholds
EARTHQUAKE_MIN_MAGNITUDE = 1.0  # Minimum valid magnitude
EARTHQUAKE_MAX_MAGNITUDE = 10.0  # Maximum realistic magnitude
EARTHQUAKE_MIN_DEPTH = -10.0     # Minimum depth (above sea level)
EARTHQUAKE_MAX_DEPTH = 1000.0    # Maximum depth

print(f"📊 Batch timestamp: {BATCH_TIMESTAMP}")
print(f"🔄 Deduplication window: {DEDUPLICATION_WINDOW_HOURS} hours")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Functions

# COMMAND ----------

def validate_coordinates(df, lat_col="latitude", lon_col="longitude"):
    """
    Validate and clean coordinate data
    """
    return df.filter(
        (col(lat_col) >= -90) & 
        (col(lat_col) <= 90) &
        (col(lon_col) >= -180) & 
        (col(lon_col) <= 180) &
        col(lat_col).isNotNull() &
        col(lon_col).isNotNull()
    )

def validate_earthquake_data(df):
    """
    Apply earthquake-specific data quality rules
    """
    return df.filter(
        # Magnitude validation
        (col("magnitude") >= EARTHQUAKE_MIN_MAGNITUDE) &
        (col("magnitude") <= EARTHQUAKE_MAX_MAGNITUDE) &
        col("magnitude").isNotNull() &
        
        # Depth validation
        (col("depth_km") >= EARTHQUAKE_MIN_DEPTH) &
        (col("depth_km") <= EARTHQUAKE_MAX_DEPTH) &
        
        # Essential fields must exist
        col("event_id").isNotNull() &
        col("event_time").isNotNull() &
        
        # Event ID format check (basic)
        length(col("event_id")) > 5
    )

def calculate_earthquake_severity_score(df):
    """
    Calculate a normalized severity score for earthquakes
    """
    return df.withColumn(
        "severity_score",
        when(col("magnitude") >= 8.0, 1.0)
        .when(col("magnitude") >= 7.0, 0.9)
        .when(col("magnitude") >= 6.0, 0.8)
        .when(col("magnitude") >= 5.0, 0.6)
        .when(col("magnitude") >= 4.0, 0.4)
        .when(col("magnitude") >= 3.0, 0.2)
        .otherwise(0.1)
    ).withColumn(
        "severity_category",
        when(col("magnitude") >= 8.0, "Great")
        .when(col("magnitude") >= 7.0, "Major") 
        .when(col("magnitude") >= 6.0, "Strong")
        .when(col("magnitude") >= 5.0, "Moderate")
        .when(col("magnitude") >= 4.0, "Light")
        .when(col("magnitude") >= 3.0, "Minor")
        .otherwise("Micro")
    )

def calculate_weather_severity_score(df):
    """
    Calculate severity score for weather alerts
    """
    return df.withColumn(
        "severity_score",
        when(col("severity") == "Extreme", 1.0)
        .when(col("severity") == "Severe", 0.8)
        .when(col("severity") == "Moderate", 0.6)
        .when(col("severity") == "Minor", 0.4)
        .otherwise(0.2)
    ).withColumn(
        "urgency_score", 
        when(col("urgency") == "Immediate", 1.0)
        .when(col("urgency") == "Expected", 0.8)
        .when(col("urgency") == "Future", 0.6)
        .otherwise(0.4)
    ).withColumn(
        "combined_severity_score",
        (col("severity_score") * 0.7 + col("urgency_score") * 0.3)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Geographic Enrichment Functions

# COMMAND ----------

# Geographic regions lookup (simplified version)
geographic_regions = [
    # North America
    ("North America - West Coast", -125.0, -120.0, 32.0, 49.0),
    ("North America - East Coast", -80.0, -65.0, 25.0, 45.0),
    ("North America - Central", -110.0, -90.0, 30.0, 50.0),
    
    # Pacific Ring of Fire regions
    ("Japan Region", 129.0, 146.0, 30.0, 46.0),
    ("Indonesia Region", 95.0, 141.0, -11.0, 6.0),
    ("Philippines Region", 116.0, 127.0, 5.0, 21.0),
    ("Chile Region", -76.0, -66.0, -56.0, -17.0),
    ("New Zealand Region", 166.0, 179.0, -47.0, -34.0),
    ("Alaska Region", -170.0, -130.0, 54.0, 72.0),
    
    # Mediterranean
    ("Mediterranean", -6.0, 37.0, 30.0, 47.0),
    
    # Mid-Atlantic Ridge
    ("Mid-Atlantic Ridge", -35.0, -10.0, -60.0, 70.0),
    
    # Default
    ("Other", -180.0, 180.0, -90.0, 90.0)
]

def assign_geographic_region(df):
    """
    Assign geographic regions based on coordinates
    """
    # Create region assignment logic
    region_condition = lit("Other")  # Default
    
    for region_name, min_lon, max_lon, min_lat, max_lat in geographic_regions[:-1]:  # Exclude default
        condition = (
            (col("longitude") >= min_lon) & 
            (col("longitude") <= max_lon) &
            (col("latitude") >= min_lat) & 
            (col("latitude") <= max_lat)
        )
        region_condition = when(condition, region_name).otherwise(region_condition)
    
    return df.withColumn("geographic_region", region_condition)

def calculate_distance_to_populated_areas(df):
    """
    Calculate approximate distance to major populated areas
    This is a simplified version - in production, use proper geospatial functions
    """
    
    # Major cities coordinates (lat, lon)
    major_cities = [
        ("Tokyo", 35.6762, 139.6503),
        ("Los Angeles", 34.0522, -118.2437),
        ("San Francisco", 37.7749, -122.4194),
        ("Mexico City", 19.4326, -99.1332),
        ("Jakarta", -6.2088, 106.8456),
        ("Manila", 14.5995, 120.9842),
        ("Santiago", -33.4489, -70.6693)
    ]
    
    # Calculate distance to nearest major city (simplified great circle distance)
    distance_conditions = lit(999999.0)  # Default large distance
    
    for city_name, city_lat, city_lon in major_cities:
        # Simplified distance calculation (not accurate for large distances)
        distance = sqrt(
            pow(col("latitude") - city_lat, 2) + 
            pow(col("longitude") - city_lon, 2)
        ) * 111.0  # Rough km per degree
        
        distance_conditions = least(distance_conditions, distance)
    
    return df.withColumn("distance_to_major_city_km", distance_conditions) \
             .withColumn(
                 "population_impact_risk",
                 when(col("distance_to_major_city_km") <= 50, "High")
                 .when(col("distance_to_major_city_km") <= 200, "Medium")
                 .otherwise("Low")
             )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Earthquake Data

# COMMAND ----------

print("🌍 Processing Earthquake Data...")
print("=" * 50)

try:
    # Read bronze earthquake data (last 24 hours to handle deduplication)
    cutoff_time = datetime.utcnow() - timedelta(hours=DEDUPLICATION_WINDOW_HOURS)
    
    earthquake_bronze_df = spark.read.parquet(EARTHQUAKE_BRONZE_PATH) \
        .filter(col("ingestion_time") >= cutoff_time)
    
    initial_count = earthquake_bronze_df.count()
    print(f"📊 Initial earthquake records: {initial_count:,}")
    
    if initial_count > 0:
        
        # Step 1: Data quality validation
        print("🔍 Step 1: Data quality validation...")
        earthquake_clean_df = validate_coordinates(earthquake_bronze_df) \
            .pipe(validate_earthquake_data)
        
        after_validation_count = earthquake_clean_df.count()
        print(f"✅ Records after validation: {after_validation_count:,} ({(after_validation_count/initial_count*100):.1f}%)")
        
        # Step 2: Deduplication
        print("🔄 Step 2: Deduplication...")
        window_spec = Window.partitionBy("event_id").orderBy(desc("ingestion_time"))
        
        earthquake_dedup_df = earthquake_clean_df \
            .withColumn("row_number", row_number().over(window_spec)) \
            .filter(col("row_number") == 1) \
            .drop("row_number")
        
        after_dedup_count = earthquake_dedup_df.count()
        print(f"✅ Records after deduplication: {after_dedup_count:,}")
        
        # Step 3: Data enrichment
        print("🌍 Step 3: Geographic and severity enrichment...")
        earthquake_enriched_df = earthquake_dedup_df \
            .pipe(calculate_earthquake_severity_score) \
            .pipe(assign_geographic_region) \
            .pipe(calculate_distance_to_populated_areas)
        
        # Step 4: Add standardized columns
        print("📏 Step 4: Standardization...")
        earthquake_silver_df = earthquake_enriched_df \
            .withColumn("event_date", to_date(col("event_time"))) \
            .withColumn("event_hour", hour(col("event_time"))) \
            .withColumn("days_since_event", 
                       datediff(current_timestamp(), col("event_time"))) \
            .withColumn("is_recent", col("days_since_event") <= 7) \
            .withColumn("is_significant", 
                       (col("significance") >= 600) | (col("magnitude") >= 5.0)) \
            .withColumn("requires_alert",
                       (col("magnitude") >= 5.0) & 
                       (col("population_impact_risk").isin(["High", "Medium"]))) \
            .withColumn("processed_time", current_timestamp()) \
            .withColumn("silver_batch_id", lit(BATCH_TIMESTAMP))
        
        # Step 5: Add partitioning columns for efficient storage
        earthquake_silver_final = earthquake_silver_df \
            .withColumn("year", year(col("event_time"))) \
            .withColumn("month", month(col("event_time"))) \
            .withColumn("day", dayofmonth(col("event_time")))
        
        # Display sample of processed data
        print("📋 Sample of processed earthquake data:")
        earthquake_silver_final.select(
            "event_id", "magnitude", "severity_category", "place", 
            "geographic_region", "population_impact_risk", 
            "distance_to_major_city_km", "requires_alert", "event_time"
        ).show(5, truncate=False)
        
        # Write to Silver layer
        output_path = f"{EARTHQUAKE_SILVER_PATH}year={{year}}/month={{month}}/day={{day}}"
        
        earthquake_silver_final.write \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .partitionBy("year", "month", "day") \
            .parquet(output_path)
        
        final_count = earthquake_silver_final.count()
        print(f"✅ Successfully wrote {final_count:,} earthquake records to Silver layer")
        print(f"📁 Path: {output_path}")
        
    else:
        print("ℹ️ No earthquake data to process")
        
except Exception as e:
    logger.error(f"❌ Error processing earthquake data: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Weather Alert Data

# COMMAND ----------

print("\n🌦️ Processing Weather Alert Data...")
print("=" * 50)

try:
    # Read bronze weather alert data
    weather_bronze_df = spark.read.parquet(WEATHER_BRONZE_PATH) \
        .filter(col("ingestion_time") >= cutoff_time)
    
    initial_weather_count = weather_bronze_df.count()
    print(f"📊 Initial weather alert records: {initial_weather_count:,}")
    
    if initial_weather_count > 0:
        
        # Step 1: Data quality validation
        print("🔍 Step 1: Data quality validation...")
        weather_clean_df = weather_bronze_df \
            .filter(
                col("alert_id").isNotNull() &
                (length(col("alert_id")) > 5) &
                col("event_type").isNotNull() &
                col("severity").isNotNull()
            )
        
        after_validation_weather = weather_clean_df.count()
        print(f"✅ Weather records after validation: {after_validation_weather:,}")
        
        # Step 2: Deduplication by alert_id
        print("🔄 Step 2: Deduplication...")
        weather_window_spec = Window.partitionBy("alert_id").orderBy(desc("ingestion_time"))
        
        weather_dedup_df = weather_clean_df \
            .withColumn("row_number", row_number().over(weather_window_spec)) \
            .filter(col("row_number") == 1) \
            .drop("row_number")
        
        after_dedup_weather = weather_dedup_df.count()
        print(f"✅ Weather records after deduplication: {after_dedup_weather:,}")
        
        # Step 3: Calculate severity scores and enrichment
        print("🌊 Step 3: Severity scoring and enrichment...")
        weather_enriched_df = weather_dedup_df \
            .pipe(calculate_weather_severity_score) \
            .withColumn("alert_duration_hours",
                       when(col("expires_time").isNotNull() & col("effective_time").isNotNull(),
                            (unix_timestamp(col("expires_time")) - 
                             unix_timestamp(col("effective_time"))) / 3600)
                       .otherwise(None)) \
            .withColumn("is_active", 
                       (col("expires_time").isNull()) | 
                       (col("expires_time") > current_timestamp())) \
            .withColumn("is_imminent",
                       (col("onset_time").isNotNull()) &
                       (col("onset_time") <= (current_timestamp() + expr("INTERVAL 6 HOURS")))) \
            .withColumn("requires_alert",
                       (col("combined_severity_score") >= 0.7) | 
                       (col("severity") == "Extreme") |
                       (col("is_imminent") & col("severity").isin(["Severe", "Extreme"])))
        
        # Step 4: Categorize event types
        print("🏷️ Step 4: Event categorization...")
        weather_silver_df = weather_enriched_df \
            .withColumn("event_category",
                       when(col("event_type").rlike("(?i)tornado"), "Severe Weather")
                       .when(col("event_type").rlike("(?i)hurricane|typhoon|cyclone"), "Tropical Cyclone")
                       .when(col("event_type").rlike("(?i)flood"), "Flood")
                       .when(col("event_type").rlike("(?i)winter|snow|ice|blizzard"), "Winter Weather")
                       .when(col("event_type").rlike("(?i)heat|temperature"), "Temperature Extreme")
                       .when(col("event_type").rlike("(?i)fire"), "Fire Weather")
                       .when(col("event_type").rlike("(?i)wind"), "High Wind")
                       .otherwise("Other")) \
            .withColumn("processed_time", current_timestamp()) \
            .withColumn("silver_batch_id", lit(BATCH_TIMESTAMP))
        
        # Step 5: Add partitioning columns
        weather_silver_final = weather_silver_df \
            .withColumn("year", year(coalesce(col("sent_time"), col("ingestion_time")))) \
            .withColumn("month", month(coalesce(col("sent_time"), col("ingestion_time")))) \
            .withColumn("day", dayofmonth(coalesce(col("sent_time"), col("ingestion_time"))))
        
        # Display sample of processed weather data
        print("📋 Sample of processed weather alert data:")
        weather_silver_final.select(
            "alert_id", "event_type", "event_category", "severity", 
            "combined_severity_score", "area_desc", "requires_alert", "is_active"
        ).show(5, truncate=False)
        
        # Write to Silver layer
        weather_output_path = f"{WEATHER_SILVER_PATH}year={{year}}/month={{month}}/day={{day}}"
        
        weather_silver_final.write \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .partitionBy("year", "month", "day") \
            .parquet(weather_output_path)
        
        final_weather_count = weather_silver_final.count()
        print(f"✅ Successfully wrote {final_weather_count:,} weather alert records to Silver layer")
        print(f"📁 Path: {weather_output_path}")
        
    else:
        print("ℹ️ No weather alert data to process")
        
except Exception as e:
    logger.error(f"❌ Error processing weather alert data: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Report

# COMMAND ----------

print("\n📊 Silver Layer Data Quality Report")
print("=" * 60)

# Earthquake data summary
try:
    eq_silver_df = spark.read.parquet(EARTHQUAKE_SILVER_PATH)
    eq_count = eq_silver_df.count()
    
    print(f"🌍 EARTHQUAKE DATA SUMMARY:")
    print(f"   Total records: {eq_count:,}")
    
    if eq_count > 0:
        # Magnitude distribution
        print("   Magnitude distribution:")
        eq_silver_df.groupBy("severity_category") \
            .agg(count("*").alias("count"), 
                 round(avg("magnitude"), 2).alias("avg_magnitude")) \
            .orderBy(desc("count")) \
            .show(10, False)
        
        # Geographic distribution
        print("   Geographic distribution:")
        eq_silver_df.groupBy("geographic_region") \
            .agg(count("*").alias("count")) \
            .orderBy(desc("count")) \
            .show(10, False)
        
        # Alert-worthy events
        alert_count = eq_silver_df.filter(col("requires_alert")).count()
        print(f"   Events requiring alerts: {alert_count:,} ({alert_count/eq_count*100:.1f}%)")
        
except Exception as e:
    print(f"❌ Error reading earthquake silver data: {e}")

# Weather data summary  
try:
    weather_silver_df = spark.read.parquet(WEATHER_SILVER_PATH)
    weather_count = weather_silver_df.count()
    
    print(f"\n🌦️ WEATHER ALERT DATA SUMMARY:")
    print(f"   Total records: {weather_count:,}")
    
    if weather_count > 0:
        # Severity distribution
        print("   Severity distribution:")
        weather_silver_df.groupBy("severity", "event_category") \
            .agg(count("*").alias("count")) \
            .orderBy(desc("count")) \
            .show(10, False)
        
        # Active alerts
        active_count = weather_silver_df.filter(col("is_active")).count()
        print(f"   Active alerts: {active_count:,} ({active_count/weather_count*100:.1f}%)")
        
        # Alert-worthy events
        alert_weather_count = weather_silver_df.filter(col("requires_alert")).count()
        print(f"   Alerts requiring notification: {alert_weather_count:,} ({alert_weather_count/weather_count*100:.1f}%)")
        
except Exception as e:
    print(f"❌ Error reading weather silver data: {e}")

print("=" * 60)
print(f"✅ Silver layer processing completed at {datetime.utcnow()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC This notebook has successfully processed raw data into clean, standardized Silver layer data:
# MAGIC 
# MAGIC 1. ✅ **Data Validation**: Removed invalid coordinates, magnitudes, and malformed records
# MAGIC 2. ✅ **Deduplication**: Eliminated duplicate events based on unique identifiers
# MAGIC 3. ✅ **Enrichment**: Added severity scores, geographic regions, and population impact
# MAGIC 4. ✅ **Standardization**: Consistent schema and data types across all events
# MAGIC 5. ✅ **Alert Flagging**: Identified events requiring immediate notification
# MAGIC 
# MAGIC **Next notebook**: `03-gold-aggregation.py` - Business-ready aggregations and analytics
