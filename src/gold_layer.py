"""
Gold Layer - Analytics and Consumption Module
============================================

This module handles the creation of analytics-ready datasets and aggregations
for the Gold layer of our data lake architecture.

The Gold layer provides business-ready data optimized for analytics and reporting.
"""

from typing import Dict, List, Optional
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, avg, sum, count, min, max, round, 
    month, hour, year, desc, asc
)
from pyspark.sql.types import *
from loguru import logger


class GoldLayer:
    """
    Handles analytics aggregations and business-ready datasets for the Gold layer.
    
    The Gold layer is responsible for:
    - Creating business-ready aggregated tables
    - Optimizing data for analytics workloads
    - Implementing business logic and KPIs
    - Providing clean, documented datasets for consumption
    """
    
    def __init__(self, spark: SparkSession, gold_path: str):
        """
        Initialize Gold Layer processor.
        
        Args:
            spark: SparkSession instance
            gold_path: Path to store gold layer data
        """
        self.spark = spark
        self.gold_path = gold_path
    
    def get_silver_data(self) -> DataFrame:
        """
        Read clean data from Silver layer.
        
        Returns:
            DataFrame from Silver layer
        """
        try:
            silver_df = self.spark.table("silver_taxi_trips_clean")
            logger.info(f"Loaded Silver layer data: {silver_df.count()} records")
            return silver_df
            
        except Exception as e:
            logger.error(f"Failed to read Silver layer data: {str(e)}")
            raise
    
    def create_monthly_aggregations(self, df: DataFrame) -> DataFrame:
        """
        Create monthly aggregations for business analytics.
        
        Args:
            df: Input DataFrame from Silver layer
            
        Returns:
            DataFrame with monthly aggregations
        """
        try:
            logger.info("Creating monthly aggregations")
            
            monthly_agg = df.groupBy("pickup_year", "pickup_month") \
                .agg(
                    count("*").alias("total_trips"),
                    round(avg("total_amount"), 2).alias("avg_total_amount"),
                    round(avg("passenger_count"), 2).alias("avg_passenger_count"),
                    round(avg("trip_duration_minutes"), 2).alias("avg_trip_duration_minutes"),
                    round(sum("total_amount"), 2).alias("total_revenue"),
                    min("total_amount").alias("min_total_amount"),
                    max("total_amount").alias("max_total_amount"),
                    count("VendorID").alias("total_vendors")
                ) \
                .orderBy("pickup_year", "pickup_month")
            
            logger.info(f"Monthly aggregations created: {monthly_agg.count()} records")
            return monthly_agg
            
        except Exception as e:
            logger.error(f"Monthly aggregations failed: {str(e)}")
            raise
    
    def create_hourly_aggregations(self, df: DataFrame, target_month: int = 5) -> DataFrame:
        """
        Create hourly aggregations for the specified month.
        
        Args:
            df: Input DataFrame from Silver layer
            target_month: Month to analyze (default: May = 5)
            
        Returns:
            DataFrame with hourly aggregations for the target month
        """
        try:
            logger.info(f"Creating hourly aggregations for month {target_month}")
            
            # Filter for the target month
            month_df = df.filter(col("pickup_month") == target_month)
            
            hourly_agg = month_df.groupBy("pickup_hour") \
                .agg(
                    count("*").alias("total_trips"),
                    round(avg("passenger_count"), 2).alias("avg_passenger_count"),
                    round(avg("total_amount"), 2).alias("avg_total_amount"),
                    round(avg("trip_duration_minutes"), 2).alias("avg_trip_duration_minutes"),
                    round(sum("total_amount"), 2).alias("total_revenue")
                ) \
                .orderBy("pickup_hour")
            
            logger.info(f"Hourly aggregations created for month {target_month}: {hourly_agg.count()} records")
            return hourly_agg
            
        except Exception as e:
            logger.error(f"Hourly aggregations failed: {str(e)}")
            raise
    
    def create_vendor_analysis(self, df: DataFrame) -> DataFrame:
        """
        Create vendor performance analysis.
        
        Args:
            df: Input DataFrame from Silver layer
            
        Returns:
            DataFrame with vendor analysis
        """
        try:
            logger.info("Creating vendor analysis")
            
            vendor_analysis = df.groupBy("VendorID") \
                .agg(
                    count("*").alias("total_trips"),
                    round(avg("total_amount"), 2).alias("avg_total_amount"),
                    round(avg("passenger_count"), 2).alias("avg_passenger_count"),
                    round(avg("trip_duration_minutes"), 2).alias("avg_trip_duration_minutes"),
                    round(sum("total_amount"), 2).alias("total_revenue"),
                    (round(sum("total_amount"), 2) / count("*")).alias("revenue_per_trip")
                ) \
                .orderBy(desc("total_trips"))
            
            logger.info(f"Vendor analysis created: {vendor_analysis.count()} vendors")
            return vendor_analysis
            
        except Exception as e:
            logger.error(f"Vendor analysis failed: {str(e)}")
            raise
    
    def create_weekend_vs_weekday_analysis(self, df: DataFrame) -> DataFrame:
        """
        Create weekend vs weekday comparison analysis.
        
        Args:
            df: Input DataFrame from Silver layer
            
        Returns:
            DataFrame with weekend vs weekday analysis
        """
        try:
            logger.info("Creating weekend vs weekday analysis")
            
            weekend_analysis = df.groupBy("is_weekend") \
                .agg(
                    count("*").alias("total_trips"),
                    round(avg("total_amount"), 2).alias("avg_total_amount"),
                    round(avg("passenger_count"), 2).alias("avg_passenger_count"),
                    round(avg("trip_duration_minutes"), 2).alias("avg_trip_duration_minutes"),
                    round(sum("total_amount"), 2).alias("total_revenue")
                ) \
                .withColumn("day_type", 
                           when(col("is_weekend") == 1, "Weekend").otherwise("Weekday"))
            
            logger.info("Weekend vs weekday analysis created")
            return weekend_analysis
            
        except Exception as e:
            logger.error(f"Weekend vs weekday analysis failed: {str(e)}")
            raise
    
    def create_business_kpis(self, df: DataFrame) -> DataFrame:
        """
        Create key business performance indicators.
        
        Args:
            df: Input DataFrame from Silver layer
            
        Returns:
            DataFrame with business KPIs
        """
        try:
            logger.info("Creating business KPIs")
            
            # Overall KPIs
            kpis = df.agg(
                count("*").alias("total_trips"),
                round(avg("total_amount"), 2).alias("overall_avg_fare"),
                round(sum("total_amount"), 2).alias("total_revenue"),
                round(avg("passenger_count"), 2).alias("overall_avg_passengers"),
                round(avg("trip_duration_minutes"), 2).alias("overall_avg_duration"),
                min("tpep_pickup_datetime").alias("data_start_date"),
                max("tpep_pickup_datetime").alias("data_end_date")
            )
            
            # Add calculated KPIs
            kpis_enhanced = kpis.withColumn("avg_revenue_per_minute", 
                                          round(col("overall_avg_fare") / col("overall_avg_duration"), 2)) \
                               .withColumn("avg_revenue_per_passenger",
                                         round(col("overall_avg_fare") / col("overall_avg_passengers"), 2)) \
                               .withColumn("report_generated_at",
                                         self.spark.sql("SELECT current_timestamp()").collect()[0][0])
            
            logger.info("Business KPIs created")
            return kpis_enhanced
            
        except Exception as e:
            logger.error(f"Business KPIs creation failed: {str(e)}")
            raise
    
    def process_to_gold(self) -> bool:
        """
        Complete Gold layer processing pipeline.
        
        This method creates all analytics-ready tables:
        1. Monthly aggregations
        2. Hourly aggregations (May focus)
        3. Vendor analysis
        4. Weekend vs Weekday analysis
        5. Business KPIs
        
        Returns:
            True if processing successful, False otherwise
        """
        try:
            logger.info("Starting Gold layer processing")
            
            # Get clean data from Silver layer
            silver_df = self.get_silver_data()
            
            # Create monthly aggregations
            monthly_agg = self.create_monthly_aggregations(silver_df)
            monthly_agg.write \
                .mode("overwrite") \
                .option("path", f"{self.gold_path}/monthly_aggregations") \
                .saveAsTable("gold_monthly_aggregations")
            
            # Create hourly aggregations for May
            hourly_agg = self.create_hourly_aggregations(silver_df, target_month=5)
            hourly_agg.write \
                .mode("overwrite") \
                .option("path", f"{self.gold_path}/hourly_aggregations_may") \
                .saveAsTable("gold_hourly_aggregations_may")
            
            # Create vendor analysis
            vendor_analysis = self.create_vendor_analysis(silver_df)
            vendor_analysis.write \
                .mode("overwrite") \
                .option("path", f"{self.gold_path}/vendor_analysis") \
                .saveAsTable("gold_vendor_analysis")
            
            # Create weekend vs weekday analysis
            weekend_analysis = self.create_weekend_vs_weekday_analysis(silver_df)
            weekend_analysis.write \
                .mode("overwrite") \
                .option("path", f"{self.gold_path}/weekend_analysis") \
                .saveAsTable("gold_weekend_analysis")
            
            # Create business KPIs
            kpis = self.create_business_kpis(silver_df)
            kpis.write \
                .mode("overwrite") \
                .option("path", f"{self.gold_path}/business_kpis") \
                .saveAsTable("gold_business_kpis")
            
            logger.info("Gold layer processing completed successfully")
            
            # Log summary statistics
            logger.info("=== GOLD LAYER SUMMARY ===")
            logger.info(f"Monthly aggregations: {monthly_agg.count()} records")
            logger.info(f"Hourly aggregations (May): {hourly_agg.count()} records")
            logger.info(f"Vendor analysis: {vendor_analysis.count()} vendors")
            logger.info(f"Weekend analysis: {weekend_analysis.count()} categories")
            logger.info(f"Business KPIs: {kpis.count()} record")
            
            return True
            
        except Exception as e:
            logger.error(f"Gold layer processing failed: {str(e)}")
            return False
    
    def get_required_analytics_results(self) -> Dict:
        """
        Get the specific analytics results required by the project.
        
        Returns:
            Dictionary with required analytics results
        """
        try:
            logger.info("Generating required analytics results")
            
            results = {}
            
            # 1. Average total_amount per month considering all trips
            monthly_avg = self.spark.sql("""
                SELECT 
                    pickup_month,
                    avg_total_amount as monthly_avg_total_amount
                FROM gold_monthly_aggregations 
                ORDER BY pickup_month
            """).collect()
            
            results["monthly_avg_total_amount"] = [
                {"month": row.pickup_month, "avg_total_amount": row.monthly_avg_total_amount}
                for row in monthly_avg
            ]
            
            # 2. Average passenger_count per hour in May
            hourly_avg_may = self.spark.sql("""
                SELECT 
                    pickup_hour,
                    avg_passenger_count as hourly_avg_passenger_count
                FROM gold_hourly_aggregations_may 
                ORDER BY pickup_hour
            """).collect()
            
            results["hourly_avg_passenger_count_may"] = [
                {"hour": row.pickup_hour, "avg_passenger_count": row.hourly_avg_passenger_count}
                for row in hourly_avg_may
            ]
            
            # Additional insights
            business_kpis = self.spark.sql("""
                SELECT * FROM gold_business_kpis
            """).collect()
            
            if business_kpis:
                kpi_row = business_kpis[0]
                results["business_summary"] = {
                    "total_trips": kpi_row.total_trips,
                    "total_revenue": kpi_row.total_revenue,
                    "overall_avg_fare": kpi_row.overall_avg_fare,
                    "overall_avg_passengers": kpi_row.overall_avg_passengers,
                    "data_period": f"{kpi_row.data_start_date} to {kpi_row.data_end_date}"
                }
            
            logger.info("Required analytics results generated successfully")
            return results
            
        except Exception as e:
            logger.error(f"Failed to generate required analytics results: {str(e)}")
            return {"error": str(e)}


def create_gold_layer_job(spark: SparkSession, gold_path: str) -> bool:
    """
    Convenience function to create and run a Gold layer processing job.
    
    Args:
        spark: SparkSession instance
        gold_path: Path for gold layer storage
        
    Returns:
        True if successful, False otherwise
    """
    gold_layer = GoldLayer(spark, gold_path)
    
    logger.info("Starting Gold layer data processing")
    success = gold_layer.process_to_gold()
    
    if success:
        logger.info("Gold layer processing completed successfully")
        
        # Generate and display required analytics results
        results = gold_layer.get_required_analytics_results()
        logger.info("Required Analytics Results:")
        logger.info(f"Monthly averages: {results.get('monthly_avg_total_amount', 'N/A')}")
        logger.info(f"Hourly averages (May): {results.get('hourly_avg_passenger_count_may', 'N/A')}")
        
    else:
        logger.error("Gold layer processing failed")
        
    return success


if __name__ == "__main__":
    # Example usage for testing
    spark = SparkSession.builder \
        .appName("NYC_Taxi_Gold_Layer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    # Configure gold path (adjust for your environment)
    gold_path = "/tmp/datalake/gold"  # For local testing
    # gold_path = "s3://your-bucket/datalake/gold"  # For AWS S3
    # gold_path = "/dbfs/mnt/datalake/gold"  # For Databricks
    
    success = create_gold_layer_job(spark, gold_path)
    
            if success:
            print("Gold layer processing completed successfully")
        else:
            print("Gold layer processing failed")
    
    spark.stop()

