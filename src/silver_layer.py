"""
Silver Layer - Data Transformation and Quality Module
====================================================

This module handles data cleaning, transformation, and quality validation
for the Silver layer of our data lake architecture.

The Silver layer provides cleaned, validated, and standardized data ready for analytics.
"""

from typing import Dict, List, Optional
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, isnan, isnull, count, avg, min, max,
    to_timestamp, hour, month, year, dayofweek,
    regexp_replace, trim, upper, lower
)
from pyspark.sql.types import *
from loguru import logger

class SilverLayer:
    """
    Handles data transformation and quality validation for the Silver layer.

    The Silver layer is responsible for:
    - Data cleaning and standardization
    - Quality validation and filtering
    - Schema enforcement
    - Data type conversions
    """

    def __init__(self, spark: SparkSession, silver_path: str):
        """
        Initialize Silver Layer processor.

        Args:
            spark: SparkSession instance
            silver_path: Path to store silver layer data
        """
        self.spark = spark
        self.silver_path = silver_path

        # Define required columns and their expected types
        self.required_columns = {
            'VendorID': IntegerType(),
            'passenger_count': IntegerType(),
            'total_amount': DoubleType(),
            'tpep_pickup_datetime': TimestampType(),
            'tpep_dropoff_datetime': TimestampType()
        }

        # Define data quality rules
        self.quality_rules = {
            'passenger_count': {'min': 0, 'max': 8},
            'total_amount': {'min': 0, 'max': 1000},
            'trip_duration_minutes': {'min': 0, 'max': 480}  # 8 hours max
        }

    def get_bronze_data(self) -> DataFrame:
        """
        Read all bronze layer data and combine into single DataFrame.

        Returns:
            Combined DataFrame from all bronze tables
        """
        try:
            # Get all bronze tables
            tables = self.spark.sql("SHOW TABLES").collect()
            bronze_tables = [table.tableName for table in tables if table.tableName.startswith('bronze_taxi_data')]

            if not bronze_tables:
                raise ValueError("No bronze layer tables found")

            # Union all bronze tables
            dfs = []
            for table_name in bronze_tables:
                df = self.spark.table(table_name)
                dfs.append(df)

            # Combine all DataFrames
            combined_df = dfs[0]
            for df in dfs[1:]:
                combined_df = combined_df.union(df)

            logger.info(f"Combined {len(bronze_tables)} bronze tables into single DataFrame")
            logger.info(f"Total records in combined bronze data: {combined_df.count()}")

            return combined_df

        except Exception as e:
            logger.error(f"Failed to read bronze data: {str(e)}")
            raise

    def validate_schema(self, df: DataFrame) -> DataFrame:
        """
        Validate and enforce schema for required columns.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with validated schema
        """
        try:
            logger.info("Validating and enforcing schema")

            # Check if all required columns exist
            missing_columns = set(self.required_columns.keys()) - set(df.columns)
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")

            # Cast columns to correct types
            validated_df = df
            for column_name, expected_type in self.required_columns.items():
                if column_name in df.columns:
                    validated_df = validated_df.withColumn(
                        column_name,
                        col(column_name).cast(expected_type)
                    )

            logger.info("Schema validation completed")
            return validated_df

        except Exception as e:
            logger.error(f"Schema validation failed: {str(e)}")
            raise

    def clean_data(self, df: DataFrame) -> DataFrame:
        """
        Apply data cleaning transformations.

        Args:
            df: Input DataFrame

        Returns:
            Cleaned DataFrame
        """
        try:
            logger.info("Starting data cleaning process")

            # Remove records with null values in required columns
            cleaned_df = df.filter(
                col('VendorID').isNotNull() &
                col('passenger_count').isNotNull() &
                col('total_amount').isNotNull() &
                col('tpep_pickup_datetime').isNotNull() &
                col('tpep_dropoff_datetime').isNotNull()
            )

            # Filter out invalid passenger counts
            cleaned_df = cleaned_df.filter(
                (col('passenger_count') >= self.quality_rules['passenger_count']['min']) &
                (col('passenger_count') <= self.quality_rules['passenger_count']['max'])
            )

            # Filter out invalid total amounts
            cleaned_df = cleaned_df.filter(
                (col('total_amount') >= self.quality_rules['total_amount']['min']) &
                (col('total_amount') <= self.quality_rules['total_amount']['max'])
            )

            # Filter out trips where pickup is after dropoff
            cleaned_df = cleaned_df.filter(
                col('tpep_pickup_datetime') < col('tpep_dropoff_datetime')
            )

            # Calculate trip duration and filter outliers
            cleaned_df = cleaned_df.withColumn(
                'trip_duration_minutes',
                (col('tpep_dropoff_datetime').cast('long') -
                 col('tpep_pickup_datetime').cast('long')) / 60
            )

            cleaned_df = cleaned_df.filter(
                (col('trip_duration_minutes') >= self.quality_rules['trip_duration_minutes']['min']) &
                (col('trip_duration_minutes') <= self.quality_rules['trip_duration_minutes']['max'])
            )

            records_before = df.count()
            records_after = cleaned_df.count()
            records_removed = records_before - records_after

            logger.info(f"Data cleaning completed:")
            logger.info(f"  Records before cleaning: {records_before}")
            logger.info(f"  Records after cleaning: {records_after}")
            logger.info(f"  Records removed: {records_removed} ({records_removed/records_before*100:.2f}%)")

            return cleaned_df

        except Exception as e:
            logger.error(f"Data cleaning failed: {str(e)}")
            raise

    def enrich_data(self, df: DataFrame) -> DataFrame:
        """
        Add derived columns and enrichments.

        Args:
            df: Input DataFrame

        Returns:
            Enriched DataFrame
        """
        try:
            logger.info("Starting data enrichment")

            enriched_df = df \
                .withColumn('pickup_year', year(col('tpep_pickup_datetime'))) \
                .withColumn('pickup_month', month(col('tpep_pickup_datetime'))) \
                .withColumn('pickup_day', dayofweek(col('tpep_pickup_datetime'))) \
                .withColumn('pickup_hour', hour(col('tpep_pickup_datetime'))) \
                .withColumn('dropoff_hour', hour(col('tpep_dropoff_datetime'))) \
                .withColumn('is_weekend',
                           when(col('pickup_day').isin([1, 7]), 1).otherwise(0)) \
                .withColumn('processing_timestamp',
                           when(col('processing_timestamp').isNull(),
                                self.spark.sql("SELECT current_timestamp()").collect()[0][0])
                           .otherwise(col('processing_timestamp')))

            logger.info("Data enrichment completed")
            return enriched_df

        except Exception as e:
            logger.error(f"Data enrichment failed: {str(e)}")
            raise

    def generate_quality_report(self, df: DataFrame) -> Dict:
        """
        Generate data quality report for the processed data.

        Args:
            df: DataFrame to analyze

        Returns:
            Dictionary containing quality metrics
        """
        try:
            logger.info("Generating data quality report")

            # Basic statistics
            total_records = df.count()

            # Null counts for required columns
            null_counts = {}
            for col_name in self.required_columns.keys():
                null_count = df.filter(col(col_name).isNull()).count()
                null_counts[col_name] = {
                    'null_count': null_count,
                    'null_percentage': (null_count / total_records * 100) if total_records > 0 else 0
                }

            # Statistical summary for numeric columns
            numeric_stats = df.select([
                avg('passenger_count').alias('avg_passenger_count'),
                min('passenger_count').alias('min_passenger_count'),
                max('passenger_count').alias('max_passenger_count'),
                avg('total_amount').alias('avg_total_amount'),
                min('total_amount').alias('min_total_amount'),
                max('total_amount').alias('max_total_amount'),
                avg('trip_duration_minutes').alias('avg_trip_duration'),
                min('trip_duration_minutes').alias('min_trip_duration'),
                max('trip_duration_minutes').alias('max_trip_duration')
            ]).collect()[0].asDict()

            # Date range
            date_range = df.select([
                min('tpep_pickup_datetime').alias('earliest_pickup'),
                max('tpep_pickup_datetime').alias('latest_pickup')
            ]).collect()[0].asDict()

            quality_report = {
                'total_records': total_records,
                'null_analysis': null_counts,
                'numeric_statistics': numeric_stats,
                'date_range': date_range,
                'report_timestamp': datetime.now().isoformat()
            }

            logger.info(f"Quality report generated for {total_records} records")
            return quality_report

        except Exception as e:
            logger.error(f"Quality report generation failed: {str(e)}")
            return {'error': str(e)}

    def process_to_silver(self) -> bool:
        """
        Complete Silver layer processing pipeline.

        This method orchestrates the full Silver layer transformation:
        1. Read bronze data
        2. Validate schema
        3. Clean data
        4. Enrich data
        5. Generate quality report
        6. Save to Silver layer

        Returns:
            True if processing successful, False otherwise
        """
        try:
            logger.info("Starting Silver layer processing")

            # Step 1: Read bronze data
            bronze_df = self.get_bronze_data()

            # Step 2: Validate schema
            validated_df = self.validate_schema(bronze_df)

            # Step 3: Clean data
            cleaned_df = self.clean_data(validated_df)

            # Step 4: Enrich data
            enriched_df = self.enrich_data(cleaned_df)

            # Step 5: Generate quality report
            quality_report = self.generate_quality_report(enriched_df)

            # Step 6: Save to Silver layer
            logger.info("Saving processed data to Silver layer")

            # Partition by year and month for better query performance
            enriched_df.write \
                .mode("overwrite") \
                .partitionBy("pickup_year", "pickup_month") \
                .option("path", f"{self.silver_path}/taxi_trips_clean") \
                .saveAsTable("silver_taxi_trips_clean")

            # Save quality report as a separate table
            quality_df = self.spark.createDataFrame([quality_report])
            quality_df.write \
                .mode("append") \
                .option("path", f"{self.silver_path}/quality_reports") \
                .saveAsTable("silver_quality_reports")

            logger.info("Silver layer processing completed successfully")
            logger.info(f"Final record count: {enriched_df.count()}")

            return True

        except Exception as e:
            logger.error(f"Silver layer processing failed: {str(e)}")
            return False

def create_silver_layer_job(spark: SparkSession, silver_path: str) -> bool:
    """
    Convenience function to create and run a Silver layer processing job.

    Args:
        spark: SparkSession instance
        silver_path: Path for silver layer storage

    Returns:
        True if successful, False otherwise
    """
    silver_layer = SilverLayer(spark, silver_path)

    logger.info("Starting Silver layer data processing")
    success = silver_layer.process_to_silver()

    if success:
        logger.info("Silver layer processing completed successfully")
    else:
        logger.error("Silver layer processing failed")

    return success

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("NYC_Taxi_Silver_Layer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

    silver_path = "/tmp/datalake/silver"
    success = create_silver_layer_job(spark, silver_path)
    spark.stop()

