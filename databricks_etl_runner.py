"""
Databricks ETL Runner
====================

This script is optimized for running in Databricks environment.
It provides a simple interface to execute the complete NYC Taxi ETL pipeline.

Usage:
1. Upload this file to your Databricks workspace
2. Run it in a notebook cell or as a job
3. Monitor the execution through the provided status updates
"""

# Databricks notebook source
# MAGIC %md
# MAGIC # NYC Taxi Data ETL Pipeline - Databricks Runner
# MAGIC 
# MAGIC This notebook runs the complete ETL pipeline for NYC Taxi data processing.
# MAGIC 
# MAGIC ## Steps:
# MAGIC 1. **Setup**: Configure environment and paths
# MAGIC 2. **Bronze**: Ingest raw data from NYC TLC
# MAGIC 3. **Silver**: Clean and validate data
# MAGIC 4. **Gold**: Create analytics-ready datasets
# MAGIC 5. **Analysis**: Run required analyses

# COMMAND ----------

# MAGIC %md ## Step 1: Setup and Configuration

# COMMAND ----------

# Install required packages if not available
# %pip install loguru pyyaml

# Import required libraries
import os
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Configuration for Databricks environment
DATABRICKS_CONFIG = {
    "app_name": "NYC_Taxi_ETL_Pipeline_Databricks",
    "data_paths": {
        "bronze": "/dbfs/mnt/datalake/bronze",  # Adjust for your mount point
        "silver": "/dbfs/mnt/datalake/silver",
        "gold": "/dbfs/mnt/datalake/gold"
    },
    "data_params": {
        "year": 2023,
        "months": [1, 2, 3, 4, 5]  # January to May
    }
}

print("NYC Taxi ETL Pipeline - Databricks Edition")
print("=" * 50)
print(f"Spark Version: {spark.version}")
print(f"Configuration: {DATABRICKS_CONFIG}")

# COMMAND ----------

# MAGIC %md ## Step 2: Bronze Layer - Data Ingestion

# COMMAND ----------

# MAGIC %run ./src/bronze_layer

# Create Bronze layer job
def run_bronze_layer_databricks():
    """Execute Bronze layer in Databricks environment."""
    
    print("Starting Bronze Layer Processing...")
    
    try:
        # Import bronze layer functionality
        from bronze_layer import BronzeLayer
        
        bronze_processor = BronzeLayer(
            spark=spark,
            bronze_path=DATABRICKS_CONFIG["data_paths"]["bronze"]
        )
        
        # Execute ingestion
        success = bronze_processor.ingest_data(
            year=DATABRICKS_CONFIG["data_params"]["year"],
            months=DATABRICKS_CONFIG["data_params"]["months"]
        )
        
        if success:
            # Validate results
            validation = bronze_processor.validate_bronze_data()
            print(f"Bronze Layer Completed Successfully")
            print(f"   Files processed: {validation.get('total_files', 0)}")
            print(f"   Total records: {validation.get('total_records', 0):,}")
            return True
        else:
            print("Bronze Layer Failed")
            return False
            
    except Exception as e:
        print(f"Bronze Layer Error: {str(e)}")
        return False

# Execute Bronze Layer
bronze_success = run_bronze_layer_databricks()

# COMMAND ----------

# MAGIC %md ## Step 3: Silver Layer - Data Cleaning

# COMMAND ----------

# MAGIC %run ./src/silver_layer

def run_silver_layer_databricks():
    """Execute Silver layer in Databricks environment."""
    
    print("Starting Silver Layer Processing...")
    
    if not bronze_success:
        print("Skipping Silver Layer - Bronze Layer failed")
        return False
    
    try:
        from silver_layer import SilverLayer
        
        silver_processor = SilverLayer(
            spark=spark,
            silver_path=DATABRICKS_CONFIG["data_paths"]["silver"]
        )
        
        # Execute cleaning and transformation
        success = silver_processor.process_to_silver()
        
        if success:
            # Get quality metrics
            silver_df = spark.table("silver_taxi_trips_clean")
            record_count = silver_df.count()
            
            print(f"Silver Layer Completed Successfully")
            print(f"   Clean records: {record_count:,}")
            return True
        else:
            print("Silver Layer Failed")
            return False
            
    except Exception as e:
        print(f"Silver Layer Error: {str(e)}")
        return False

# Execute Silver Layer
silver_success = run_silver_layer_databricks()

# COMMAND ----------

# MAGIC %md ## Step 4: Gold Layer - Analytics Preparation

# COMMAND ----------

# MAGIC %run ./src/gold_layer

def run_gold_layer_databricks():
    """Execute Gold layer in Databricks environment."""
    
    print("Starting Gold Layer Processing...")
    
    if not silver_success:
        print("Skipping Gold Layer - Silver Layer failed")
        return False
    
    try:
        from gold_layer import GoldLayer
        
        gold_processor = GoldLayer(
            spark=spark,
            gold_path=DATABRICKS_CONFIG["data_paths"]["gold"]
        )
        
        # Execute aggregations
        success = gold_processor.process_to_gold()
        
        if success:
            print(f"Gold Layer Completed Successfully")
            
            # Show available tables
            tables = spark.sql("SHOW TABLES").collect()
            gold_tables = [t.tableName for t in tables if t.tableName.startswith('gold_')]
            
            print(f"   Created tables:")
            for table in gold_tables:
                count = spark.table(table).count()
                print(f"     - {table}: {count:,} records")
            
            return True
        else:
            print("Gold Layer Failed")
            return False
            
    except Exception as e:
        print(f"Gold Layer Error: {str(e)}")
        return False

# Execute Gold Layer
gold_success = run_gold_layer_databricks()

# COMMAND ----------

# MAGIC %md ## Step 5: Required Analyses

# COMMAND ----------

def run_required_analyses():
    """Execute the required analyses."""
    
    print("Running Required Analyses...")
    
    if not gold_success:
        print("Skipping Analyses - Gold Layer failed")
        return False
    
    try:
        # Analysis 1: Monthly average total_amount
        print("\n1. Monthly Average Total Amount:")
        monthly_query = """
        SELECT 
            pickup_month,
            CASE pickup_month
                WHEN 1 THEN 'January'
                WHEN 2 THEN 'February' 
                WHEN 3 THEN 'March'
                WHEN 4 THEN 'April'
                WHEN 5 THEN 'May'
            END as month_name,
            ROUND(avg_total_amount, 2) as avg_total_amount,
            total_trips
        FROM gold_monthly_aggregations 
        ORDER BY pickup_month
        """
        
        monthly_df = spark.sql(monthly_query)
        monthly_results = monthly_df.collect()
        
        for row in monthly_results:
            print(f"   {row.month_name}: ${row.avg_total_amount:.2f} (from {row.total_trips:,} trips)")
        
        # Analysis 2: Hourly average passenger_count in May
        print("\n2. Hourly Average Passenger Count (May):")
        hourly_query = """
        SELECT 
            pickup_hour,
            ROUND(avg_passenger_count, 2) as avg_passenger_count,
            total_trips
        FROM gold_hourly_aggregations_may 
        ORDER BY pickup_hour
        """
        
        hourly_df = spark.sql(hourly_query)
        hourly_results = hourly_df.collect()
        
        # Show key hours (every 3 hours for brevity)
        for i in range(0, len(hourly_results), 3):
            row = hourly_results[i]
            hour_display = f"{row.pickup_hour:02d}:00"
            print(f"   {hour_display}: {row.avg_passenger_count:.2f} passengers (from {row.total_trips:,} trips)")
        
        print(f"\nRequired Analyses Completed Successfully")
        print(f"   Monthly data points: {len(monthly_results)}")
        print(f"   Hourly data points: {len(hourly_results)}")
        
        return True
        
    except Exception as e:
        print(f"Analysis Error: {str(e)}")
        return False

# Execute Required Analyses
analysis_success = run_required_analyses()

# COMMAND ----------

# MAGIC %md ## Step 6: Pipeline Summary

# COMMAND ----------

def print_pipeline_summary():
    """Print a summary of the pipeline execution."""
    
    print("\n" + "=" * 60)
    print("NYC TAXI ETL PIPELINE - EXECUTION SUMMARY")
    print("=" * 60)
    
    # Status indicators
    bronze_status = "SUCCESS" if bronze_success else "FAILED"
    silver_status = "SUCCESS" if silver_success else "FAILED"
    gold_status = "SUCCESS" if gold_success else "FAILED"
    analysis_status = "SUCCESS" if analysis_success else "FAILED"
    
    print(f"Bronze Layer (Ingestion):     {bronze_status}")
    print(f"Silver Layer (Cleaning):      {silver_status}")
    print(f"Gold Layer (Aggregation):     {gold_status}")
    print(f"Required Analyses:            {analysis_status}")
    
    # Overall status
    overall_success = all([bronze_success, silver_success, gold_success, analysis_success])
    overall_status = "COMPLETE" if overall_success else "INCOMPLETE"
    
    print(f"\nOverall Pipeline Status:      {overall_status}")
    
    if overall_success:
        print("\nAll requirements have been successfully fulfilled!")
        print("The required analyses are now available in the Gold layer tables.")
        print("You can run additional queries on the gold_* tables for further insights.")
    else:
        print("\nSome steps failed. Please check the error messages above.")
        print("Review the logs and retry the failed steps.")
    
    print("=" * 60)
    
    return overall_success

# Print final summary
pipeline_success = print_pipeline_summary()

# COMMAND ----------

# MAGIC %md ## Next Steps
# MAGIC 
# MAGIC If the pipeline completed successfully, you can:
# MAGIC 
# MAGIC 1. **Explore the data**: Query the gold layer tables directly
# MAGIC 2. **Create visualizations**: Use Databricks' built-in visualization tools
# MAGIC 3. **Run additional analyses**: Build on the existing gold layer datasets
# MAGIC 4. **Schedule the pipeline**: Set up automated runs using Databricks Jobs
# MAGIC 
# MAGIC ### Available Gold Layer Tables:
# MAGIC - `gold_monthly_aggregations`: Monthly statistics
# MAGIC - `gold_hourly_aggregations_may`: Hourly statistics for May
# MAGIC - `gold_vendor_analysis`: Vendor performance metrics
# MAGIC - `gold_weekend_analysis`: Weekend vs weekday patterns
# MAGIC - `gold_business_kpis`: Overall business metrics
# MAGIC 
# MAGIC ### Sample Queries:
# MAGIC ```sql
# MAGIC -- Monthly trends
# MAGIC SELECT * FROM gold_monthly_aggregations ORDER BY pickup_month;
# MAGIC 
# MAGIC -- Peak hours in May
# MAGIC SELECT * FROM gold_hourly_aggregations_may ORDER BY avg_passenger_count DESC;
# MAGIC 
# MAGIC -- Business summary
# MAGIC SELECT * FROM gold_business_kpis;
# MAGIC ```

# COMMAND ----------

# Final status for Databricks job monitoring
if pipeline_success:
    dbutils.notebook.exit("SUCCESS: NYC Taxi ETL Pipeline completed successfully")
else:
    dbutils.notebook.exit("FAILED: NYC Taxi ETL Pipeline encountered errors")

# COMMAND ----------

