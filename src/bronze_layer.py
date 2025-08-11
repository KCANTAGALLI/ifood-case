"""
Bronze Layer - Data Ingestion Module
====================================

This module handles the ingestion of raw NYC Yellow Taxi data from the official source
into the Bronze layer of our data lake architecture.

The Bronze layer stores raw, unprocessed data exactly as received from the source.
"""

import os
import requests
from typing import List, Optional
from datetime import datetime
from pyspark.sql import SparkSession
from loguru import logger
import tempfile


class BronzeLayer:
    """
    Handles data ingestion for the Bronze layer of the data lake.
    
    The Bronze layer is responsible for:
    - Downloading raw data from NYC TLC website
    - Storing data in its original format
    - Maintaining data lineage and audit trail
    """
    
    def __init__(self, spark: SparkSession, bronze_path: str):
        """
        Initialize Bronze Layer processor.
        
        Args:
            spark: SparkSession instance
            bronze_path: Path to store bronze layer data (e.g., 's3://bucket/bronze/')
        """
        self.spark = spark
        self.bronze_path = bronze_path
        self.base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
        
    def get_data_urls(self, year: int = 2023, months: List[int] = None) -> List[str]:
        """
        Generate URLs for NYC Yellow Taxi data files.
        
        Args:
            year: Year of data to download
            months: List of months (1-12) to download. Defaults to Jan-May 2023.
            
        Returns:
            List of URLs to download
        """
        if months is None:
            months = [1, 2, 3, 4, 5]  # January to May 2023
            
        urls = []
        for month in months:
            filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
            url = f"{self.base_url}/{filename}"
            urls.append(url)
            
        return urls
    
    def download_file(self, url: str, local_path: str) -> bool:
        """
        Download a file from URL to local path.
        
        Args:
            url: URL to download from
            local_path: Local path to save the file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Downloading {url}")
            response = requests.get(url, stream=True)
            response.raise_for_status()
            
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
                    
            logger.info(f"Successfully downloaded to {local_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to download {url}: {str(e)}")
            return False
    
    def ingest_data(self, year: int = 2023, months: List[int] = None) -> bool:
        """
        Ingest raw NYC taxi data into Bronze layer.
        
        This method:
        1. Downloads raw parquet files from NYC TLC
        2. Stores them in the Bronze layer with metadata
        3. Creates audit trail for data lineage
        
        Args:
            year: Year of data to ingest
            months: List of months to ingest
            
        Returns:
            True if ingestion successful, False otherwise
        """
        try:
            if months is None:
                months = [1, 2, 3, 4, 5]
                
            urls = self.get_data_urls(year, months)
            
            with tempfile.TemporaryDirectory() as temp_dir:
                for url in urls:
                    # Extract filename from URL
                    filename = url.split('/')[-1]
                    local_path = os.path.join(temp_dir, filename)
                    
                    # Download file
                    if not self.download_file(url, local_path):
                        logger.error(f"Failed to download {filename}")
                        continue
                    
                    # Read the parquet file to validate and add metadata
                    df = self.spark.read.parquet(local_path)
                    
                    # Add metadata columns for audit trail
                    df_with_metadata = df.withColumn("ingestion_timestamp", 
                                                   self.spark.sql("SELECT current_timestamp()").collect()[0][0]) \
                                       .withColumn("source_file", 
                                                 self.spark.sql(f"SELECT '{filename}'").collect()[0][0]) \
                                       .withColumn("ingestion_year", 
                                                 self.spark.sql(f"SELECT {year}").collect()[0][0]) \
                                       .withColumn("ingestion_month", 
                                                 self.spark.sql(f"SELECT {filename.split('_')[2].split('-')[1].split('.')[0]}").collect()[0][0])
                    
                    # Save to Bronze layer
                    bronze_file_path = f"{self.bronze_path}/year={year}/month={filename.split('_')[2].split('-')[1].split('.')[0]}"
                    
                    logger.info(f"Writing {filename} to Bronze layer: {bronze_file_path}")
                    df_with_metadata.write \
                        .mode("overwrite") \
                        .option("path", bronze_file_path) \
                        .saveAsTable(f"bronze_taxi_data_{year}_{filename.split('_')[2].split('-')[1].split('.')[0]}")
                    
                    logger.info(f"Successfully ingested {filename} to Bronze layer")
                    
            return True
            
        except Exception as e:
            logger.error(f"Bronze layer ingestion failed: {str(e)}")
            return False
    
    def validate_bronze_data(self) -> dict:
        """
        Validate bronze layer data and return quality metrics.
        
        Returns:
            Dictionary containing validation results and metrics
        """
        try:
            results = {
                "total_files": 0,
                "total_records": 0,
                "validation_timestamp": datetime.now().isoformat(),
                "files_processed": []
            }
            
            # List all tables in bronze layer
            tables = self.spark.sql("SHOW TABLES").collect()
            bronze_tables = [table.tableName for table in tables if table.tableName.startswith('bronze_taxi_data')]
            
            for table_name in bronze_tables:
                df = self.spark.table(table_name)
                record_count = df.count()
                
                results["total_files"] += 1
                results["total_records"] += record_count
                results["files_processed"].append({
                    "table": table_name,
                    "record_count": record_count,
                    "columns": len(df.columns)
                })
                
                logger.info(f"Bronze table {table_name}: {record_count} records")
            
            logger.info(f"Bronze layer validation complete: {results['total_files']} files, {results['total_records']} total records")
            return results
            
        except Exception as e:
            logger.error(f"Bronze layer validation failed: {str(e)}")
            return {"error": str(e)}


def create_bronze_layer_job(spark: SparkSession, bronze_path: str, 
                           year: int = 2023, months: List[int] = None) -> bool:
    """
    Convenience function to create and run a Bronze layer ingestion job.
    
    Args:
        spark: SparkSession instance
        bronze_path: Path for bronze layer storage
        year: Year of data to process
        months: Months to process
        
    Returns:
        True if successful, False otherwise
    """
    bronze_layer = BronzeLayer(spark, bronze_path)
    
    logger.info("Starting Bronze layer data ingestion")
    success = bronze_layer.ingest_data(year, months)
    
    if success:
        logger.info("Bronze layer ingestion completed successfully")
        validation_results = bronze_layer.validate_bronze_data()
        logger.info(f"Validation results: {validation_results}")
    else:
        logger.error("Bronze layer ingestion failed")
        
    return success


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("NYC_Taxi_Bronze_Layer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    bronze_path = "/tmp/datalake/bronze"
    success = create_bronze_layer_job(spark, bronze_path)
    spark.stop()

