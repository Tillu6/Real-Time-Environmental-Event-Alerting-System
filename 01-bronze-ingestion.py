# Bronze Layer Data Ingestion Notebook
# Real-Time Environmental Event Alerting System
# This notebook handles raw data ingestion from USGS and NOAA APIs

# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Raw Data Ingestion
# MAGIC 
# MAGIC This notebook ingests real-time data from:
# MAGIC - USGS Earthquake API
# MAGIC - NOAA Weather API 
# MAGIC 
# MAGIC Data is stored in its raw form in the Bronze layer of our Medallion architecture.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

import json
import requests
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# Initialize Spark session for Microsoft Fabric
spark = SparkSession.builder \
    .appName("Environmental-Alert-Bronze-Ingestion") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print("✅ Spark session initialized successfully")
print(f"Spark version: {spark.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Parameters

# COMMAND ----------

# API Configuration
USGS_API_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
NOAA_API_URL = "https://api.weather.gov"

# Fabric Lakehouse paths
BRONZE_LAKEHOUSE_PATH = "Files/bronze/"
EARTHQUAKE_RAW_PATH = f"{BRONZE_LAKEHOUSE_PATH}earthquakes/"
WEATHER_RAW_PATH = f"{BRONZE_LAKEHOUSE_PATH}weather/"

# Processing parameters
BATCH_TIMESTAMP = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
EARTHQUAKE_MIN_MAGNITUDE = 2.5
LOOKBACK_HOURS = 1  # How far back to look for new data

print(f"📊 Batch timestamp: {BATCH_TIMESTAMP}")
print(f"🔍 Earthquake minimum magnitude: {EARTHQUAKE_MIN_MAGNITUDE}")
print(f"⏰ Lookback period: {LOOKBACK_HOURS} hours")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Earthquake Data Ingestion Functions

# COMMAND ----------

def fetch_earthquake_data(start_time, end_time, min_magnitude=2.5, limit=1000):
    """
    Fetch earthquake data from USGS API
    """
    
    params = {
        'format': 'geojson',
        'starttime': start_time.strftime('%Y-%m-%dT%H:%M:%S'),
        'endtime': end_time.strftime('%Y-%m-%dT%H:%M:%S'),
        'minmagnitude': min_magnitude,
        'limit': limit,
        'orderby': 'time-asc'
    }
    
    try:
        logger.info(f"🌍 Fetching earthquakes from USGS API...")
        logger.info(f"   Time range: {start_time} to {end_time}")
        logger.info(f"   Min magnitude: {min_magnitude}")
        
        response = requests.get(USGS_API_URL, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        earthquake_count = len(data.get('features', []))
        
        logger.info(f"✅ Successfully fetched {earthquake_count} earthquake events")
        return data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Error fetching earthquake data: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"❌ Error parsing earthquake API response: {e}")
        raise

def parse_earthquake_geojson(geojson_data):
    """
    Parse USGS GeoJSON response into structured records
    """
    
    records = []
    features = geojson_data.get('features', [])
    
    for feature in features:
        try:
            # Extract geometry
            geometry = feature.get('geometry', {})
            coordinates = geometry.get('coordinates', [])
            
            if len(coordinates) < 3:
                continue
                
            longitude, latitude, depth = coordinates[:3]
            
            # Extract properties  
            props = feature.get('properties', {})
            
            # Parse timestamp
            time_ms = props.get('time')
            timestamp = datetime.utcfromtimestamp(time_ms / 1000) if time_ms else None
            
            # Parse updated timestamp
            updated_ms = props.get('updated')
            updated_timestamp = datetime.utcfromtimestamp(updated_ms / 1000) if updated_ms else None
            
            record = {
                # Identifiers
                'event_id': feature.get('id', ''),
                'net': props.get('net', ''),
                'code': props.get('code', ''),
                
                # Location
                'latitude': float(latitude) if latitude is not None else None,
                'longitude': float(longitude) if longitude is not None else None,
                'depth_km': float(depth) if depth is not None else None,
                'place': props.get('place', ''),
                
                # Magnitude and intensity
                'magnitude': float(props.get('mag')) if props.get('mag') is not None else None,
                'magnitude_type': props.get('magType', ''),
                'cdi': float(props.get('cdi')) if props.get('cdi') is not None else None,  # Community Decimal Intensity
                'mmi': float(props.get('mmi')) if props.get('mmi') is not None else None,  # Modified Mercalli Intensity
                
                # Event details
                'event_type': props.get('type', 'earthquake'),
                'status': props.get('status', ''),
                'significance': int(props.get('sig', 0)),
                'alert_level': props.get('alert', ''),
                'tsunami_warning': bool(props.get('tsunami', 0)),
                
                # Quality metrics
                'nst': int(props.get('nst')) if props.get('nst') is not None else None,  # Number of stations
                'gap': float(props.get('gap')) if props.get('gap') is not None else None,  # Azimuthal gap
                'dmin': float(props.get('dmin')) if props.get('dmin') is not None else None,  # Distance to nearest station
                'rms': float(props.get('rms')) if props.get('rms') is not None else None,  # RMS of residuals
                
                # Community data
                'felt_reports': int(props.get('felt')) if props.get('felt') is not None else None,
                
                # Timestamps
                'event_time': timestamp,
                'updated_time': updated_timestamp,
                'ingestion_time': datetime.utcnow(),
                
                # URLs and metadata
                'detail_url': props.get('detail', ''),
                'url': props.get('url', ''),
                'data_source': 'usgs_api',
                'api_version': '1.0',
                'batch_id': BATCH_TIMESTAMP
            }
            
            records.append(record)
            
        except Exception as e:
            logger.warning(f"⚠️ Error parsing earthquake feature: {e}")
            continue
    
    logger.info(f"📝 Parsed {len(records)} earthquake records")
    return records

# COMMAND ----------

# MAGIC %md
# MAGIC ## Weather Data Ingestion Functions

# COMMAND ----------

def fetch_weather_alerts():
    """
    Fetch active weather alerts from NOAA API
    """
    
    alerts_url = f"{NOAA_API_URL}/alerts/active"
    
    try:
        logger.info("🌦️ Fetching weather alerts from NOAA API...")
        
        response = requests.get(alerts_url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        alert_count = len(data.get('features', []))
        
        logger.info(f"✅ Successfully fetched {alert_count} weather alerts")
        return data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Error fetching weather alert data: {e}")
        raise

def parse_weather_alerts(alerts_data):
    """
    Parse NOAA weather alerts into structured records
    """
    
    records = []
    features = alerts_data.get('features', [])
    
    for feature in features:
        try:
            props = feature.get('properties', {})
            geometry = feature.get('geometry', {})
            
            # Parse timestamps
            sent_time = props.get('sent')
            effective_time = props.get('effective')
            onset_time = props.get('onset')
            expires_time = props.get('expires')
            ends_time = props.get('ends')
            
            def parse_iso_timestamp(iso_string):
                if iso_string:
                    try:
                        return datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
                    except:
                        return None
                return None
            
            record = {
                # Identifiers
                'alert_id': props.get('id', ''),
                'identifier': props.get('identifier', ''),
                'sender': props.get('sender', ''),
                'sender_name': props.get('senderName', ''),
                
                # Alert details
                'event_type': props.get('event', ''),
                'headline': props.get('headline', ''),
                'description': props.get('description', ''),
                'instruction': props.get('instruction', ''),
                
                # Severity and urgency
                'severity': props.get('severity', ''),
                'certainty': props.get('certainty', ''),
                'urgency': props.get('urgency', ''),
                'response_type': props.get('response', ''),
                
                # Geography
                'area_desc': props.get('areaDesc', ''),
                'geocode': json.dumps(props.get('geocode', {})),
                'geometry': json.dumps(geometry) if geometry else None,
                
                # Categories and codes
                'message_type': props.get('messageType', ''),
                'category': props.get('category', ''),
                'status': props.get('status', ''),
                
                # Timestamps
                'sent_time': parse_iso_timestamp(sent_time),
                'effective_time': parse_iso_timestamp(effective_time),
                'onset_time': parse_iso_timestamp(onset_time),
                'expires_time': parse_iso_timestamp(expires_time),
                'ends_time': parse_iso_timestamp(ends_time),
                'ingestion_time': datetime.utcnow(),
                
                # Metadata
                'data_source': 'noaa_api',
                'api_version': '1.0',
                'batch_id': BATCH_TIMESTAMP
            }
            
            records.append(record)
            
        except Exception as e:
            logger.warning(f"⚠️ Error parsing weather alert: {e}")
            continue
    
    logger.info(f"📝 Parsed {len(records)} weather alert records")
    return records

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Ingestion Execution

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest Earthquake Data

# COMMAND ----------

# Set time range for earthquake data
end_time = datetime.utcnow()
start_time = end_time - timedelta(hours=LOOKBACK_HOURS)

print(f"🕐 Earthquake data time range:")
print(f"   Start: {start_time}")
print(f"   End: {end_time}")

# Fetch earthquake data
try:
    earthquake_geojson = fetch_earthquake_data(
        start_time=start_time,
        end_time=end_time,
        min_magnitude=EARTHQUAKE_MIN_MAGNITUDE
    )
    
    # Parse earthquake data
    earthquake_records = parse_earthquake_geojson(earthquake_geojson)
    
    if earthquake_records:
        # Convert to Spark DataFrame
        earthquake_df = spark.createDataFrame(earthquake_records)
        
        # Add partitioning columns
        earthquake_df = earthquake_df \
            .withColumn("year", year(col("event_time"))) \
            .withColumn("month", month(col("event_time"))) \
            .withColumn("day", dayofmonth(col("event_time"))) \
            .withColumn("hour", hour(col("event_time")))
        
        # Display sample data
        print("📋 Earthquake data sample:")
        earthquake_df.select(
            "event_id", "magnitude", "place", "latitude", "longitude", 
            "depth_km", "event_time", "significance", "alert_level"
        ).show(5, truncate=False)
        
        # Write to Bronze layer
        output_path = f"{EARTHQUAKE_RAW_PATH}year={{year}}/month={{month}}/day={{day}}/batch_{BATCH_TIMESTAMP}"
        
        earthquake_df.write \
            .mode("append") \
            .partitionBy("year", "month", "day") \
            .parquet(output_path)
        
        print(f"✅ Successfully wrote {earthquake_df.count()} earthquake records to Bronze layer")
        print(f"📁 Path: {output_path}")
        
    else:
        print("ℹ️ No earthquake records to process")
        
except Exception as e:
    logger.error(f"❌ Error processing earthquake data: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest Weather Alert Data

# COMMAND ----------

# Fetch weather alert data
try:
    weather_alerts_data = fetch_weather_alerts()
    
    # Parse weather alert data
    weather_records = parse_weather_alerts(weather_alerts_data)
    
    if weather_records:
        # Convert to Spark DataFrame
        weather_df = spark.createDataFrame(weather_records)
        
        # Add partitioning columns based on sent_time
        weather_df = weather_df \
            .withColumn("year", year(coalesce(col("sent_time"), col("ingestion_time")))) \
            .withColumn("month", month(coalesce(col("sent_time"), col("ingestion_time")))) \
            .withColumn("day", dayofmonth(coalesce(col("sent_time"), col("ingestion_time")))) \
            .withColumn("hour", hour(coalesce(col("sent_time"), col("ingestion_time"))))
        
        # Display sample data
        print("📋 Weather alert data sample:")
        weather_df.select(
            "alert_id", "event_type", "headline", "severity", "urgency", 
            "area_desc", "sent_time", "expires_time"
        ).show(5, truncate=False)
        
        # Write to Bronze layer
        output_path = f"{WEATHER_RAW_PATH}year={{year}}/month={{month}}/day={{day}}/batch_{BATCH_TIMESTAMP}"
        
        weather_df.write \
            .mode("append") \
            .partitionBy("year", "month", "day") \
            .parquet(output_path)
        
        print(f"✅ Successfully wrote {weather_df.count()} weather alert records to Bronze layer")
        print(f"📁 Path: {output_path}")
        
    else:
        print("ℹ️ No weather alert records to process")
        
except Exception as e:
    logger.error(f"❌ Error processing weather alert data: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

print("🔍 Data Quality Summary:")
print("=" * 50)

# Check if we have earthquake data
try:
    eq_count = spark.read.parquet(f"{EARTHQUAKE_RAW_PATH}*").count()
    print(f"📊 Total earthquake records in Bronze: {eq_count:,}")
except:
    print("📊 No earthquake data found in Bronze layer")

# Check if we have weather data
try:
    weather_count = spark.read.parquet(f"{WEATHER_RAW_PATH}*").count()
    print(f"🌦️ Total weather alert records in Bronze: {weather_count:,}")
except:
    print("🌦️ No weather data found in Bronze layer")

# Check data freshness
try:
    latest_eq = spark.read.parquet(f"{EARTHQUAKE_RAW_PATH}*") \
        .select(max("ingestion_time").alias("latest_ingestion")) \
        .collect()[0]["latest_ingestion"]
    print(f"🕐 Latest earthquake data ingestion: {latest_eq}")
except:
    print("🕐 No timestamp data available for earthquakes")

print("=" * 50)
print(f"✅ Bronze layer ingestion completed at {datetime.utcnow()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC This notebook has successfully ingested raw data into the Bronze layer:
# MAGIC 
# MAGIC 1. ✅ **Earthquake Data**: Raw USGS earthquake events with full metadata
# MAGIC 2. ✅ **Weather Alerts**: NOAA weather alerts with severity information
# MAGIC 3. ✅ **Data Partitioning**: Organized by year/month/day for efficient querying
# MAGIC 4. ✅ **Quality Metrics**: Basic validation and record counts
# MAGIC 
# MAGIC **Next notebook**: `02-silver-processing.py` - Data cleaning and standardization
