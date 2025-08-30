#!/usr/bin/env python3
"""
Spark Job for Processing Market Data
Calculates real-time analytics and financial metrics
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import sys
import os

def create_spark_session():
    """Create Spark session with appropriate configuration."""
    return (SparkSession
            .builder
            .appName("FinanceDataProcessing")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .getOrCreate())

def read_market_data(spark, jdbc_url, table_name):
    """Read market data from PostgreSQL."""
    return (spark
            .read
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", table_name)
            .option("user", "postgres")
            .option("password", "postgres")
            .option("driver", "org.postgresql.Driver")
            .load())

def calculate_moving_averages(df):
    """Calculate various moving averages."""
    window_20 = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-20, 0)
    window_50 = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-50, 0)
    window_200 = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-200, 0)
    
    return (df
            .withColumn("sma_20", avg("price").over(window_20))
            .withColumn("sma_50", avg("price").over(window_50))
            .withColumn("sma_200", avg("price").over(window_200))
            .withColumn("ema_12", expr("price"))  # Simplified EMA calculation
            .withColumn("ema_26", expr("price")))  # Simplified EMA calculation

def calculate_volatility(df):
    """Calculate price volatility metrics."""
    window_20 = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-20, 0)
    
    return (df
            .withColumn("price_std", stddev("price").over(window_20))
            .withColumn("volatility", col("price_std") / col("sma_20") * 100)
            .withColumn("high_low_range", col("high_price") - col("low_price"))
            .withColumn("true_range", col("high_low_range")))

def calculate_volume_metrics(df):
    """Calculate volume-based metrics."""
    window_20 = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-20, 0)
    
    return (df
            .withColumn("volume_sma", avg("volume").over(window_20))
            .withColumn("volume_ratio", col("volume") / col("volume_sma"))
            .withColumn("volume_trend", 
                       when(col("volume") > col("volume_sma") * 1.5, "high")
                       .when(col("volume") < col("volume_sma") * 0.5, "low")
                       .otherwise("normal")))

def calculate_technical_indicators(df):
    """Calculate technical indicators."""
    window_14 = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-14, 0)
    
    # RSI calculation (simplified)
    df_with_gains = (df
                    .withColumn("price_change", col("price") - lag("price").over(Window.partitionBy("symbol").orderBy("timestamp")))
                    .withColumn("gain", when(col("price_change") > 0, col("price_change")).otherwise(0))
                    .withColumn("loss", when(col("price_change") < 0, -col("price_change")).otherwise(0)))
    
    return (df_with_gains
            .withColumn("avg_gain", avg("gain").over(window_14))
            .withColumn("avg_loss", avg("loss").over(window_14))
            .withColumn("rs", col("avg_gain") / col("avg_loss"))
            .withColumn("rsi", 100 - (100 / (1 + col("rs"))))
            .withColumn("macd", col("ema_12") - col("ema_26")))

def detect_anomalies(df):
    """Detect price and volume anomalies."""
    window_20 = Window.partitionBy("symbol").orderBy("timestamp").rowsBetween(-20, 0)
    
    return (df
            .withColumn("price_mean", avg("price").over(window_20))
            .withColumn("price_std", stddev("price").over(window_20))
            .withColumn("price_zscore", (col("price") - col("price_mean")) / col("price_std"))
            .withColumn("price_anomaly", abs(col("price_zscore")) > 3)
            .withColumn("volume_anomaly", col("volume_ratio") > 3))

def write_processed_data(df, jdbc_url, table_name):
    """Write processed data back to PostgreSQL."""
    (df.write
     .format("jdbc")
     .option("url", jdbc_url)
     .option("dbtable", table_name)
     .option("user", "postgres")
     .option("password", "postgres")
     .option("driver", "org.postgresql.Driver")
     .mode("append")
     .save())

def main():
    """Main processing function."""
    # Initialize Spark
    spark = create_spark_session()
    
    # Configuration
    jdbc_url = "jdbc:postgresql://postgres:5432/finance_data"
    
    try:
        # Read raw tick data
        print("Reading market data...")
        tick_data = read_market_data(spark, jdbc_url, "tick_data")
        
        # Cache the data for multiple operations
        tick_data.cache()
        
        print(f"Processing {tick_data.count()} records...")
        
        # Apply transformations
        processed_data = (tick_data
                         .transform(calculate_moving_averages)
                         .transform(calculate_volatility)
                         .transform(calculate_volume_metrics)
                         .transform(calculate_technical_indicators)
                         .transform(detect_anomalies))
        
        # Select relevant columns for output
        output_data = (processed_data
                      .select(
                          "symbol",
                          "timestamp",
                          "price",
                          "volume",
                          "sma_20",
                          "sma_50",
                          "sma_200",
                          "volatility",
                          "rsi",
                          "macd",
                          "volume_ratio",
                          "volume_trend",
                          "price_anomaly",
                          "volume_anomaly"
                      )
                      .filter(col("timestamp") >= current_timestamp() - expr("INTERVAL 1 HOUR")))
        
        # Write processed data
        print("Writing processed data...")
        write_processed_data(output_data, jdbc_url, "processed_market_data")
        
        # Generate summary statistics
        summary = (output_data
                  .groupBy("symbol")
                  .agg(
                      count("*").alias("record_count"),
                      avg("price").alias("avg_price"),
                      avg("volatility").alias("avg_volatility"),
                      avg("rsi").alias("avg_rsi"),
                      sum(when(col("price_anomaly"), 1).otherwise(0)).alias("anomaly_count")
                  ))
        
        print("Summary statistics:")
        summary.show()
        
        # Write summary to a separate table
        write_processed_data(summary, jdbc_url, "market_summary")
        
        print("Processing completed successfully!")
        
    except Exception as e:
        print(f"Error during processing: {e}")
        raise
    finally:
        # Cleanup
        if 'tick_data' in locals():
            tick_data.unpersist()
        spark.stop()

if __name__ == "__main__":
    main()
