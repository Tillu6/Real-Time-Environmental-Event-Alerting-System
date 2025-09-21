# Anomaly Detection Notebook
# Real-Time Environmental Event Alerting System
# This notebook implements ML-based anomaly detection for environmental events

# Databricks notebook source
# MAGIC %md
# MAGIC # Anomaly Detection with Spark MLlib
# MAGIC 
# MAGIC This notebook implements advanced anomaly detection for:
# MAGIC - Earthquake magnitude and frequency anomalies
# MAGIC - Unusual weather pattern detection
# MAGIC - Geographic clustering analysis
# MAGIC - Temporal pattern analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Imports

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler, MinMaxScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.stat import Correlation
from pyspark.ml import Pipeline
from datetime import datetime, timedelta
import numpy as np
import logging

# Initialize Spark session with ML optimizations
spark = SparkSession.builder \
    .appName("Environmental-Alert-Anomaly-Detection") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

print("✅ Spark ML session initialized successfully")
print(f"Spark version: {spark.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration and Parameters

# COMMAND ----------

# Data paths
SILVER_LAKEHOUSE_PATH = "Files/silver/"
GOLD_LAKEHOUSE_PATH = "Files/gold/"
EARTHQUAKE_SILVER_PATH = f"{SILVER_LAKEHOUSE_PATH}earthquakes/"
WEATHER_SILVER_PATH = f"{SILVER_LAKEHOUSE_PATH}weather/"
ANOMALY_RESULTS_PATH = f"{GOLD_LAKEHOUSE_PATH}anomaly_detection/"

# Anomaly detection parameters
ISOLATION_FOREST_CONTAMINATION = 0.05  # Expected proportion of anomalies
STATISTICAL_ZSCORE_THRESHOLD = 3.0      # Z-score threshold for statistical anomalies
KMEANS_CLUSTERS = 8                     # Number of clusters for K-means
TEMPORAL_WINDOW_HOURS = 168             # 7 days for temporal analysis
GEOGRAPHIC_GRID_SIZE = 1.0              # Degrees for geographic grid analysis

# Processing parameters
BATCH_TIMESTAMP = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
LOOKBACK_DAYS = 30  # Days to look back for pattern analysis

print(f"📊 Batch timestamp: {BATCH_TIMESTAMP}")
print(f"🔍 Analysis period: {LOOKBACK_DAYS} days")
print(f"🎯 Anomaly contamination rate: {ISOLATION_FOREST_CONTAMINATION}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Statistical Anomaly Detection Functions

# COMMAND ----------

def detect_statistical_anomalies(df, feature_cols, z_threshold=3.0):
    """
    Detect anomalies using statistical methods (Z-score)
    """
    
    anomaly_df = df
    
    for col_name in feature_cols:
        # Calculate mean and standard deviation
        stats = df.agg(
            avg(col(col_name)).alias("mean"),
            stddev(col(col_name)).alias("std")
        ).collect()[0]
        
        mean_val = stats["mean"]
        std_val = stats["std"]
        
        if std_val and std_val > 0:
            # Calculate Z-score
            anomaly_df = anomaly_df.withColumn(
                f"{col_name}_zscore",
                abs(col(col_name) - mean_val) / std_val
            ).withColumn(
                f"{col_name}_anomaly",
                col(f"{col_name}_zscore") > z_threshold
            )
        else:
            anomaly_df = anomaly_df.withColumn(f"{col_name}_anomaly", lit(False))
    
    # Create overall statistical anomaly flag
    anomaly_conditions = [col(f"{col_name}_anomaly") for col_name in feature_cols]
    overall_condition = anomaly_conditions[0]
    for condition in anomaly_conditions[1:]:
        overall_condition = overall_condition | condition
    
    anomaly_df = anomaly_df.withColumn("statistical_anomaly", overall_condition)
    
    return anomaly_df

def calculate_earthquake_frequency_anomalies(df):
    """
    Detect frequency-based earthquake anomalies using temporal and spatial windows
    """
    
    # Create time windows (hourly)
    df_with_hour = df.withColumn("hour_window", 
                                date_trunc("hour", col("event_time")))
    
    # Calculate hourly earthquake frequency by region
    hourly_freq = df_with_hour.groupBy("geographic_region", "hour_window") \
        .agg(count("*").alias("earthquake_count"),
             avg("magnitude").alias("avg_magnitude"),
             max("magnitude").alias("max_magnitude"))
    
    # Calculate rolling statistics for each region
    window_spec = Window.partitionBy("geographic_region") \
        .orderBy("hour_window") \
        .rowsBetween(-167, -1)  # Previous 7 days (168 hours)
    
    freq_with_baseline = hourly_freq.withColumn(
        "baseline_count_mean", avg("earthquake_count").over(window_spec)
    ).withColumn(
        "baseline_count_std", stddev("earthquake_count").over(window_spec)
    ).withColumn(
        "baseline_mag_mean", avg("avg_magnitude").over(window_spec)
    )
    
    # Detect frequency anomalies
    frequency_anomalies = freq_with_baseline.withColumn(
        "frequency_zscore",
        when(col("baseline_count_std") > 0,
             (col("earthquake_count") - col("baseline_count_mean")) / col("baseline_count_std"))
        .otherwise(0.0)
    ).withColumn(
        "frequency_anomaly",
        (col("frequency_zscore") > 2.0) | (col("earthquake_count") >= 10)  # 10+ earthquakes per hour
    ).withColumn(
        "magnitude_anomaly", 
        (col("max_magnitude") - col("baseline_mag_mean")) > 1.5  # 1.5+ magnitude above baseline
    ).withColumn(
        "combined_anomaly",
        col("frequency_anomaly") | col("magnitude_anomaly")
    )
    
    return frequency_anomalies

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clustering-Based Anomaly Detection

# COMMAND ----------

def prepare_earthquake_features_for_clustering(df):
    """
    Prepare earthquake features for clustering analysis
    """
    
    # Select and engineer features for clustering
    feature_df = df.select(
        "event_id",
        "magnitude",
        "depth_km", 
        "latitude",
        "longitude",
        "significance",
        col("severity_score").alias("severity"),
        hour("event_time").alias("hour_of_day"),
        dayofweek("event_time").alias("day_of_week"),
        col("distance_to_major_city_km").alias("city_distance")
    ).filter(
        # Remove rows with null values in key features
        col("magnitude").isNotNull() &
        col("depth_km").isNotNull() &
        col("latitude").isNotNull() &
        col("longitude").isNotNull()
    )
    
    # Assemble features into a vector
    feature_cols = ["magnitude", "depth_km", "latitude", "longitude", 
                    "significance", "severity", "hour_of_day", 
                    "day_of_week", "city_distance"]
    
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features",
        handleInvalid="skip"
    )
    
    # Scale features
    scaler = StandardScaler(
        inputCol="features",
        outputCol="scaled_features",
        withMean=True,
        withStd=True
    )
    
    # Create pipeline
    pipeline = Pipeline(stages=[assembler, scaler])
    model = pipeline.fit(feature_df)
    scaled_df = model.transform(feature_df)
    
    return scaled_df, feature_cols

def detect_clustering_anomalies(df, n_clusters=8):
    """
    Use K-means clustering to detect anomalies
    """
    
    # Apply K-means clustering
    kmeans = KMeans(
        featuresCol="scaled_features",
        predictionCol="cluster",
        k=n_clusters,
        seed=42,
        maxIter=100
    )
    
    model = kmeans.fit(df)
    clustered_df = model.transform(df)
    
    # Calculate distance to cluster centers
    centers = model.clusterCenters()
    
    # Function to calculate distance to assigned cluster center
    def calculate_cluster_distance(features, cluster_id):
        if cluster_id < len(centers):
            center = centers[cluster_id]
            # Calculate Euclidean distance
            diff = np.array(features) - np.array(center)
            return float(np.sqrt(np.sum(diff ** 2)))
        return 0.0
    
    # Register UDF for distance calculation
    from pyspark.sql.functions import udf
    distance_udf = udf(calculate_cluster_distance, FloatType())
    
    # Add cluster distance and anomaly detection
    result_df = clustered_df.withColumn(
        "cluster_distance",
        distance_udf(col("scaled_features"), col("cluster"))
    )
    
    # Calculate distance statistics for anomaly detection
    distance_stats = result_df.agg(
        avg("cluster_distance").alias("mean_distance"),
        stddev("cluster_distance").alias("std_distance")
    ).collect()[0]
    
    mean_dist = distance_stats["mean_distance"]
    std_dist = distance_stats["std_distance"]
    
    # Mark anomalies as points far from their cluster center
    threshold_distance = mean_dist + (2.0 * std_dist) if std_dist else mean_dist * 1.5
    
    anomaly_df = result_df.withColumn(
        "clustering_anomaly",
        col("cluster_distance") > threshold_distance
    ).withColumn(
        "clustering_anomaly_score", 
        col("cluster_distance") / threshold_distance
    )
    
    return anomaly_df, model

# COMMAND ----------

# MAGIC %md
# MAGIC ## Geographic Pattern Analysis

# COMMAND ----------

def detect_geographic_anomalies(df):
    """
    Detect geographic anomalies using spatial clustering
    """
    
    # Create geographic grid
    grid_df = df.withColumn(
        "lat_grid", floor(col("latitude") / GEOGRAPHIC_GRID_SIZE) * GEOGRAPHIC_GRID_SIZE
    ).withColumn(
        "lon_grid", floor(col("longitude") / GEOGRAPHIC_GRID_SIZE) * GEOGRAPHIC_GRID_SIZE
    ).withColumn(
        "grid_cell", concat(col("lat_grid"), lit("_"), col("lon_grid"))
    )
    
    # Analyze activity by grid cell
    grid_analysis = grid_df.groupBy("grid_cell", "lat_grid", "lon_grid") \
        .agg(
            count("*").alias("event_count"),
            avg("magnitude").alias("avg_magnitude"),
            max("magnitude").alias("max_magnitude"),
            stddev("magnitude").alias("magnitude_std"),
            min("event_time").alias("first_event"),
            max("event_time").alias("last_event")
        ) \
        .withColumn(
            "time_span_hours",
            (unix_timestamp("last_event") - unix_timestamp("first_event")) / 3600
        ) \
        .withColumn(
            "event_rate_per_hour",
            when(col("time_span_hours") > 0, col("event_count") / col("time_span_hours"))
            .otherwise(col("event_count"))
        )
    
    # Calculate global statistics for anomaly detection
    global_stats = grid_analysis.agg(
        avg("event_count").alias("global_avg_count"),
        stddev("event_count").alias("global_std_count"),
        avg("avg_magnitude").alias("global_avg_magnitude"),
        avg("event_rate_per_hour").alias("global_avg_rate")
    ).collect()[0]
    
    # Detect geographic anomalies
    geographic_anomalies = grid_analysis.withColumn(
        "count_zscore",
        (col("event_count") - global_stats["global_avg_count"]) / 
        global_stats["global_std_count"] if global_stats["global_std_count"] > 0 else 0
    ).withColumn(
        "high_activity_anomaly",
        col("count_zscore") > 2.0  # Areas with unusually high activity
    ).withColumn(
        "high_magnitude_anomaly", 
        col("avg_magnitude") > (global_stats["global_avg_magnitude"] + 1.0)
    ).withColumn(
        "rapid_succession_anomaly",
        (col("event_count") >= 5) & (col("time_span_hours") <= 24)  # 5+ events in 24 hours
    ).withColumn(
        "geographic_anomaly",
        col("high_activity_anomaly") | 
        col("high_magnitude_anomaly") | 
        col("rapid_succession_anomaly")
    )
    
    return geographic_anomalies

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Earthquake Anomaly Detection

# COMMAND ----------

print("🌍 Processing Earthquake Anomaly Detection...")
print("=" * 60)

try:
    # Load silver earthquake data
    cutoff_date = datetime.utcnow() - timedelta(days=LOOKBACK_DAYS)
    
    earthquake_df = spark.read.parquet(EARTHQUAKE_SILVER_PATH) \
        .filter(col("event_time") >= cutoff_date) \
        .filter(col("magnitude").isNotNull())
    
    initial_count = earthquake_df.count()
    print(f"📊 Earthquake records for analysis: {initial_count:,}")
    
    if initial_count > 100:  # Need sufficient data for meaningful analysis
        
        # Step 1: Statistical anomaly detection
        print("📈 Step 1: Statistical anomaly detection...")
        statistical_features = ["magnitude", "depth_km", "significance", "severity_score"]
        
        earthquake_statistical = detect_statistical_anomalies(
            earthquake_df, 
            statistical_features, 
            STATISTICAL_ZSCORE_THRESHOLD
        )
        
        stat_anomaly_count = earthquake_statistical.filter(col("statistical_anomaly")).count()
        print(f"   Statistical anomalies detected: {stat_anomaly_count:,}")
        
        # Step 2: Frequency-based anomaly detection
        print("🕐 Step 2: Temporal frequency analysis...")
        frequency_anomalies = calculate_earthquake_frequency_anomalies(earthquake_df)
        
        freq_anomaly_count = frequency_anomalies.filter(col("combined_anomaly")).count()
        print(f"   Frequency anomalies detected: {freq_anomaly_count:,}")
        
        # Step 3: Clustering-based anomaly detection
        print("🎯 Step 3: Clustering-based anomaly detection...")
        
        if initial_count >= 1000:  # Only run clustering with sufficient data
            clustered_features, feature_names = prepare_earthquake_features_for_clustering(earthquake_df)
            clustered_anomalies, clustering_model = detect_clustering_anomalies(
                clustered_features, 
                KMEANS_CLUSTERS
            )
            
            cluster_anomaly_count = clustered_anomalies.filter(col("clustering_anomaly")).count()
            print(f"   Clustering anomalies detected: {cluster_anomaly_count:,}")
            
            # Display cluster information
            print("   Cluster distribution:")
            clustered_anomalies.groupBy("cluster") \
                .agg(count("*").alias("count"),
                     avg("magnitude").alias("avg_magnitude"),
                     countDistinct(when(col("clustering_anomaly"), col("event_id"))).alias("anomalies")) \
                .orderBy("cluster").show(10, False)
        else:
            print("   Insufficient data for clustering analysis")
            clustered_anomalies = earthquake_df.withColumn("clustering_anomaly", lit(False)) \
                                                .withColumn("clustering_anomaly_score", lit(0.0))
        
        # Step 4: Geographic pattern analysis
        print("🗺️ Step 4: Geographic pattern analysis...")
        geographic_anomalies = detect_geographic_anomalies(earthquake_df)
        
        geo_anomaly_count = geographic_anomalies.filter(col("geographic_anomaly")).count()
        print(f"   Geographic anomalies detected: {geo_anomaly_count:,}")
        
        # Display geographic anomaly hotspots
        print("   Geographic anomaly hotspots:")
        geographic_anomalies.filter(col("geographic_anomaly")) \
            .select("grid_cell", "lat_grid", "lon_grid", "event_count", 
                    "avg_magnitude", "event_rate_per_hour") \
            .orderBy(desc("event_count")) \
            .show(10, False)
        
        # Step 5: Combine all anomaly detections
        print("🔗 Step 5: Combining anomaly results...")
        
        # Join statistical anomalies back to main dataset
        earthquake_with_anomalies = earthquake_df.join(
            earthquake_statistical.select("event_id", "statistical_anomaly", 
                                         *[f"{col}_zscore" for col in statistical_features]),
            "event_id", "left"
        )
        
        # Add clustering results if available
        if 'clustered_anomalies' in locals():
            earthquake_with_anomalies = earthquake_with_anomalies.join(
                clustered_anomalies.select("event_id", "clustering_anomaly", 
                                         "clustering_anomaly_score", "cluster"),
                "event_id", "left"
            )
        
        # Add geographic anomaly information
        earthquake_with_anomalies = earthquake_with_anomalies.withColumn(
            "lat_grid", floor(col("latitude") / GEOGRAPHIC_GRID_SIZE) * GEOGRAPHIC_GRID_SIZE
        ).withColumn(
            "lon_grid", floor(col("longitude") / GEOGRAPHIC_GRID_SIZE) * GEOGRAPHIC_GRID_SIZE  
        ).withColumn(
            "grid_cell", concat(col("lat_grid"), lit("_"), col("lon_grid"))
        ).join(
            geographic_anomalies.select("grid_cell", "geographic_anomaly", 
                                      "high_activity_anomaly", "rapid_succession_anomaly"),
            "grid_cell", "left"
        )
        
        # Create master anomaly flag
        final_earthquake_anomalies = earthquake_with_anomalies.fillna({
            "statistical_anomaly": False,
            "clustering_anomaly": False,
            "geographic_anomaly": False
        }).withColumn(
            "is_anomaly",
            col("statistical_anomaly") | 
            col("clustering_anomaly") | 
            col("geographic_anomaly")
        ).withColumn(
            "anomaly_types",
            concat_ws(",",
                when(col("statistical_anomaly"), lit("statistical")).otherwise(lit("")),
                when(col("clustering_anomaly"), lit("clustering")).otherwise(lit("")),
                when(col("geographic_anomaly"), lit("geographic")).otherwise(lit(""))
            )
        ).withColumn(
            "anomaly_score",
            greatest(
                coalesce(col("magnitude_zscore"), lit(0.0)) / STATISTICAL_ZSCORE_THRESHOLD,
                coalesce(col("clustering_anomaly_score"), lit(0.0)),
                when(col("geographic_anomaly"), lit(1.0)).otherwise(lit(0.0))
            )
        ).withColumn(
            "detection_timestamp", current_timestamp()
        ).withColumn(
            "detection_batch_id", lit(BATCH_TIMESTAMP)
        )
        
        # Summary statistics
        total_anomalies = final_earthquake_anomalies.filter(col("is_anomaly")).count()
        anomaly_rate = total_anomalies / initial_count * 100
        
        print(f"📊 Final Anomaly Detection Results:")
        print(f"   Total anomalies: {total_anomalies:,} ({anomaly_rate:.2f}%)")
        
        # Show sample anomalies
        print("   Sample anomaly events:")
        final_earthquake_anomalies.filter(col("is_anomaly")) \
            .select("event_id", "magnitude", "place", "anomaly_types", 
                    "anomaly_score", "event_time") \
            .orderBy(desc("anomaly_score")) \
            .show(10, False)
        
        # Save results to Gold layer
        output_path = f"{ANOMALY_RESULTS_PATH}earthquakes/batch_{BATCH_TIMESTAMP}"
        
        final_earthquake_anomalies.write \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .parquet(output_path)
        
        print(f"✅ Saved anomaly detection results to: {output_path}")
        
        # Save frequency anomalies separately
        freq_output_path = f"{ANOMALY_RESULTS_PATH}earthquake_frequency/batch_{BATCH_TIMESTAMP}"
        frequency_anomalies.write \
            .mode("overwrite") \
            .parquet(freq_output_path)
        
        print(f"✅ Saved frequency analysis to: {freq_output_path}")
        
    else:
        print(f"⚠️ Insufficient earthquake data for meaningful anomaly detection (minimum: 100, found: {initial_count})")
        
except Exception as e:
    logger.error(f"❌ Error in earthquake anomaly detection: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Weather Pattern Anomaly Detection

# COMMAND ----------

print("\n🌦️ Processing Weather Pattern Anomaly Detection...")
print("=" * 60)

try:
    # Load silver weather data
    weather_df = spark.read.parquet(WEATHER_SILVER_PATH) \
        .filter(col("sent_time") >= cutoff_date)
    
    weather_count = weather_df.count()
    print(f"📊 Weather alert records for analysis: {weather_count:,}")
    
    if weather_count > 50:
        
        # Analyze weather alert patterns
        weather_patterns = weather_df.groupBy("event_type", "severity", "area_desc") \
            .agg(
                count("*").alias("alert_frequency"),
                avg("combined_severity_score").alias("avg_severity_score"),
                countDistinct("alert_id").alias("unique_alerts"),
                min("sent_time").alias("first_alert"),
                max("sent_time").alias("last_alert")
            ) \
            .withColumn(
                "time_span_hours",
                (unix_timestamp("last_alert") - unix_timestamp("first_alert")) / 3600
            ) \
            .withColumn(
                "alerts_per_hour",
                when(col("time_span_hours") > 0, col("alert_frequency") / col("time_span_hours"))
                .otherwise(col("alert_frequency"))
            )
        
        # Detect weather anomalies
        weather_stats = weather_patterns.agg(
            avg("alert_frequency").alias("avg_frequency"),
            stddev("alert_frequency").alias("std_frequency"),
            avg("avg_severity_score").alias("global_avg_severity")
        ).collect()[0]
        
        weather_anomalies = weather_patterns.withColumn(
            "frequency_zscore",
            (col("alert_frequency") - weather_stats["avg_frequency"]) /
            weather_stats["std_frequency"] if weather_stats["std_frequency"] > 0 else 0
        ).withColumn(
            "high_frequency_anomaly",
            col("frequency_zscore") > 2.0
        ).withColumn(
            "severe_weather_cluster",
            (col("severity") == "Extreme") & (col("alert_frequency") >= 5)
        ).withColumn(
            "weather_anomaly",
            col("high_frequency_anomaly") | col("severe_weather_cluster")
        ).withColumn(
            "detection_timestamp", current_timestamp()
        ).withColumn(
            "detection_batch_id", lit(BATCH_TIMESTAMP)
        )
        
        weather_anomaly_count = weather_anomalies.filter(col("weather_anomaly")).count()
        print(f"📊 Weather pattern anomalies detected: {weather_anomaly_count:,}")
        
        if weather_anomaly_count > 0:
            print("   Weather anomaly patterns:")
            weather_anomalies.filter(col("weather_anomaly")) \
                .select("event_type", "severity", "area_desc", "alert_frequency", 
                        "avg_severity_score", "alerts_per_hour") \
                .orderBy(desc("alert_frequency")) \
                .show(10, False)
        
        # Save weather anomaly results
        weather_output_path = f"{ANOMALY_RESULTS_PATH}weather_patterns/batch_{BATCH_TIMESTAMP}"
        
        weather_anomalies.write \
            .mode("overwrite") \
            .parquet(weather_output_path)
        
        print(f"✅ Saved weather anomaly results to: {weather_output_path}")
        
    else:
        print(f"⚠️ Insufficient weather data for pattern analysis (minimum: 50, found: {weather_count})")
        
except Exception as e:
    logger.error(f"❌ Error in weather pattern analysis: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Anomaly Detection Summary Report

# COMMAND ----------

print("\n📈 ANOMALY DETECTION SUMMARY REPORT")
print("=" * 80)

try:
    # Load and summarize all anomaly results
    eq_anomalies = spark.read.parquet(f"{ANOMALY_RESULTS_PATH}earthquakes/")
    total_eq_events = eq_anomalies.count()
    total_eq_anomalies = eq_anomalies.filter(col("is_anomaly")).count()
    
    print(f"🌍 EARTHQUAKE ANOMALY SUMMARY:")
    print(f"   Total events analyzed: {total_eq_events:,}")
    print(f"   Anomalies detected: {total_eq_anomalies:,} ({total_eq_anomalies/total_eq_events*100:.2f}%)")
    
    # Anomaly breakdown by type
    print("   Anomaly breakdown by type:")
    eq_anomalies.filter(col("is_anomaly")) \
        .withColumn("anomaly_type_exploded", explode(split(col("anomaly_types"), ","))) \
        .filter(col("anomaly_type_exploded") != "") \
        .groupBy("anomaly_type_exploded") \
        .agg(count("*").alias("count")) \
        .orderBy(desc("count")) \
        .show(10, False)
    
    # High-risk anomalies (score > 0.8)
    high_risk_count = eq_anomalies.filter((col("is_anomaly")) & (col("anomaly_score") > 0.8)).count()
    print(f"   High-risk anomalies (score > 0.8): {high_risk_count:,}")
    
    # Recent anomalies (last 24 hours)
    recent_anomalies = eq_anomalies.filter(
        (col("is_anomaly")) & 
        (col("event_time") > (current_timestamp() - expr("INTERVAL 24 HOURS")))
    ).count()
    print(f"   Recent anomalies (24h): {recent_anomalies:,}")
    
except Exception as e:
    print(f"❌ Error generating earthquake summary: {e}")

try:
    # Weather anomaly summary
    weather_anomalies = spark.read.parquet(f"{ANOMALY_RESULTS_PATH}weather_patterns/")
    total_weather_patterns = weather_anomalies.count()
    total_weather_anomalies = weather_anomalies.filter(col("weather_anomaly")).count()
    
    print(f"\n🌦️ WEATHER ANOMALY SUMMARY:")
    print(f"   Total patterns analyzed: {total_weather_patterns:,}")
    print(f"   Anomalous patterns detected: {total_weather_anomalies:,}")
    
    if total_weather_anomalies > 0:
        print("   Top anomalous weather patterns:")
        weather_anomalies.filter(col("weather_anomaly")) \
            .select("event_type", "severity", "alert_frequency") \
            .orderBy(desc("alert_frequency")) \
            .show(5, False)
    
except Exception as e:
    print(f"❌ Error generating weather summary: {e}")

print("=" * 80)
print(f"✅ Anomaly detection completed at {datetime.utcnow()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC 
# MAGIC This notebook has successfully implemented comprehensive anomaly detection:
# MAGIC 
# MAGIC 1. ✅ **Statistical Anomaly Detection**: Z-score based outlier detection
# MAGIC 2. ✅ **Clustering Analysis**: K-means based pattern anomalies  
# MAGIC 3. ✅ **Temporal Pattern Analysis**: Frequency and succession anomalies
# MAGIC 4. ✅ **Geographic Analysis**: Spatial clustering and hotspot detection
# MAGIC 5. ✅ **Weather Pattern Analysis**: Alert frequency and severity anomalies
# MAGIC 6. ✅ **Integrated Scoring**: Combined anomaly scoring and classification
# MAGIC 
# MAGIC **Detected anomalies are ready for:**
# MAGIC - Real-time alerting systems
# MAGIC - Power BI dashboard visualization
# MAGIC - Automated notification triggers
# MAGIC - Historical trend analysis
