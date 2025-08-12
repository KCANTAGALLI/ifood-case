"""
ETL Pipeline Orchestrator
========================

Main orchestrator for the NYC Taxi Data Pipeline.
Coordinates the execution of Bronze, Silver, and Gold layer processing.
"""

import os
import sys
from typing import Dict, List, Optional
from datetime import datetime
from pyspark.sql import SparkSession
from loguru import logger
import yaml
import json

# Import our layer processors
from bronze_layer import create_bronze_layer_job
from silver_layer import create_silver_layer_job
from gold_layer import create_gold_layer_job

class ETLPipeline:
    """
    Main ETL Pipeline orchestrator for NYC Taxi data processing.

    Manages the complete data pipeline from raw data ingestion
    through to analytics-ready gold layer datasets.
    """

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize ETL Pipeline.

        Args:
            config_path: Path to configuration file (optional)
        """
        self.config = self.load_config(config_path)
        self.spark = None
        self.execution_log = []

    def load_config(self, config_path: Optional[str] = None) -> Dict:
        """
        Load configuration from file or use defaults.

        Args:
            config_path: Path to config file

        Returns:
            Configuration dictionary
        """
        default_config = {
            "app_name": "NYC_Taxi_ETL_Pipeline",
            "data_paths": {
                "bronze": "/tmp/datalake/bronze",
                "silver": "/tmp/datalake/silver",
                "gold": "/tmp/datalake/gold"
            },
            "data_params": {
                "year": 2023,
                "months": [1, 2, 3, 4, 5]  # Jan to May
            },
            "spark_config": {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
            },
            "execution_options": {
                "run_bronze": True,
                "run_silver": True,
                "run_gold": True,
                "validate_each_layer": True
            }
        }

        if config_path and os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    if config_path.endswith('.yaml') or config_path.endswith('.yml'):
                        file_config = yaml.safe_load(f)
                    else:
                        file_config = json.load(f)

                # Merge with defaults
                default_config.update(file_config)
                logger.info(f"Configuration loaded from {config_path}")

            except Exception as e:
                logger.warning(f"Failed to load config from {config_path}: {str(e)}")
                logger.info("Using default configuration")

        return default_config

    def create_spark_session(self) -> SparkSession:
        """
        Create and configure Spark session.

        Returns:
            Configured SparkSession
        """
        try:
            builder = SparkSession.builder.appName(self.config["app_name"])

            # Apply Spark configuration
            for key, value in self.config["spark_config"].items():
                builder = builder.config(key, value)

            # Enable Hive support for table operations
            spark = builder.enableHiveSupport().getOrCreate()

            # Set log level to reduce noise
            spark.sparkContext.setLogLevel("WARN")

            logger.info(f"Spark session created: {spark.version}")
            logger.info(f"Spark UI available at: {spark.sparkContext.uiWebUrl}")

            return spark

        except Exception as e:
            logger.error(f"Failed to create Spark session: {str(e)}")
            raise

    def log_execution_step(self, layer: str, status: str,
                          duration: float = None, error: str = None):
        """
        Log execution step for audit trail.

        Args:
            layer: Layer name (bronze/silver/gold)
            status: Status (started/completed/failed)
            duration: Execution duration in seconds
            error: Error message if failed
        """
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "layer": layer,
            "status": status,
            "duration_seconds": duration,
            "error": error
        }

        self.execution_log.append(log_entry)

        if status == "failed":
            logger.error(f"{layer.upper()} layer {status}: {error}")
        else:
            logger.info(f"{layer.upper()} layer {status}" +
                       (f" in {duration:.2f}s" if duration else ""))

    def run_bronze_layer(self) -> bool:
        """
        Execute Bronze layer processing.

        Returns:
            True if successful, False otherwise
        """
        if not self.config["execution_options"]["run_bronze"]:
            logger.info("Bronze layer execution skipped (disabled in config)")
            return True

        start_time = datetime.now()
        self.log_execution_step("bronze", "started")

        try:
            success = create_bronze_layer_job(
                spark=self.spark,
                bronze_path=self.config["data_paths"]["bronze"],
                year=self.config["data_params"]["year"],
                months=self.config["data_params"]["months"]
            )

            duration = (datetime.now() - start_time).total_seconds()

            if success:
                self.log_execution_step("bronze", "completed", duration)
                return True
            else:
                self.log_execution_step("bronze", "failed", duration, "Bronze layer job returned False")
                return False

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            self.log_execution_step("bronze", "failed", duration, str(e))
            return False

    def run_silver_layer(self) -> bool:
        """
        Execute Silver layer processing.

        Returns:
            True if successful, False otherwise
        """
        if not self.config["execution_options"]["run_silver"]:
            logger.info("Silver layer execution skipped (disabled in config)")
            return True

        start_time = datetime.now()
        self.log_execution_step("silver", "started")

        try:
            success = create_silver_layer_job(
                spark=self.spark,
                silver_path=self.config["data_paths"]["silver"]
            )

            duration = (datetime.now() - start_time).total_seconds()

            if success:
                self.log_execution_step("silver", "completed", duration)
                return True
            else:
                self.log_execution_step("silver", "failed", duration, "Silver layer job returned False")
                return False

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            self.log_execution_step("silver", "failed", duration, str(e))
            return False

    def run_gold_layer(self) -> bool:
        """
        Execute Gold layer processing.

        Returns:
            True if successful, False otherwise
        """
        if not self.config["execution_options"]["run_gold"]:
            logger.info("Gold layer execution skipped (disabled in config)")
            return True

        start_time = datetime.now()
        self.log_execution_step("gold", "started")

        try:
            success = create_gold_layer_job(
                spark=self.spark,
                gold_path=self.config["data_paths"]["gold"]
            )

            duration = (datetime.now() - start_time).total_seconds()

            if success:
                self.log_execution_step("gold", "completed", duration)
                return True
            else:
                self.log_execution_step("gold", "failed", duration, "Gold layer job returned False")
                return False

        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            self.log_execution_step("gold", "failed", duration, str(e))
            return False

    def validate_layer(self, layer: str) -> Dict:
        """
        Validate a specific layer's data quality.

        Args:
            layer: Layer to validate (bronze/silver/gold)

        Returns:
            Validation results dictionary
        """
        try:
            if layer == "bronze":
                tables = self.spark.sql("SHOW TABLES").collect()
                bronze_tables = [t.tableName for t in tables if t.tableName.startswith('bronze_taxi_data')]

                total_records = 0
                for table in bronze_tables:
                    count = self.spark.table(table).count()
                    total_records += count

                return {
                    "layer": "bronze",
                    "tables_found": len(bronze_tables),
                    "total_records": total_records,
                    "status": "valid" if total_records > 0 else "empty"
                }

            elif layer == "silver":
                try:
                    silver_df = self.spark.table("silver_taxi_trips_clean")
                    record_count = silver_df.count()

                    return {
                        "layer": "silver",
                        "total_records": record_count,
                        "status": "valid" if record_count > 0 else "empty"
                    }
                except Exception:
                    return {"layer": "silver", "status": "not_found"}

            elif layer == "gold":
                gold_tables = ["gold_monthly_aggregations", "gold_hourly_aggregations_may",
                              "gold_vendor_analysis", "gold_weekend_analysis", "gold_business_kpis"]

                results = {}
                for table in gold_tables:
                    try:
                        count = self.spark.table(table).count()
                        results[table] = count
                    except Exception:
                        results[table] = "not_found"

                return {
                    "layer": "gold",
                    "tables": results,
                    "status": "valid" if all(isinstance(v, int) and v > 0 for v in results.values()) else "incomplete"
                }

        except Exception as e:
            return {"layer": layer, "status": "error", "error": str(e)}

    def run_pipeline(self) -> bool:
        """
        Execute the complete ETL pipeline.

        Returns:
            True if pipeline successful, False otherwise
        """
        pipeline_start_time = datetime.now()
        logger.info("="*60)
        logger.info("STARTING NYC TAXI DATA ETL PIPELINE")
        logger.info("="*60)

        try:
            # Create Spark session
            self.spark = self.create_spark_session()

            # Execute layers in sequence
            bronze_success = self.run_bronze_layer()

            if bronze_success and self.config["execution_options"]["validate_each_layer"]:
                bronze_validation = self.validate_layer("bronze")
                logger.info(f"Bronze validation: {bronze_validation}")

            if not bronze_success:
                logger.error("Pipeline failed at Bronze layer")
                return False

            silver_success = self.run_silver_layer()

            if silver_success and self.config["execution_options"]["validate_each_layer"]:
                silver_validation = self.validate_layer("silver")
                logger.info(f"Silver validation: {silver_validation}")

            if not silver_success:
                logger.error("Pipeline failed at Silver layer")
                return False

            gold_success = self.run_gold_layer()

            if gold_success and self.config["execution_options"]["validate_each_layer"]:
                gold_validation = self.validate_layer("gold")
                logger.info(f"Gold validation: {gold_validation}")

            if not gold_success:
                logger.error("Pipeline failed at Gold layer")
                return False

            # Pipeline completed successfully
            total_duration = (datetime.now() - pipeline_start_time).total_seconds()

            logger.info("="*60)
            logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
            logger.info(f"Total execution time: {total_duration:.2f} seconds")
            logger.info("="*60)

            # Save execution log
            self.save_execution_log()

            return True

        except Exception as e:
            logger.error(f"Pipeline execution failed: {str(e)}")
            return False

        finally:
            if self.spark:
                self.spark.stop()

    def save_execution_log(self):
        """Save execution log to file for audit purposes."""
        try:
            log_filename = f"pipeline_execution_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

            with open(log_filename, 'w') as f:
                json.dump({
                    "config": self.config,
                    "execution_log": self.execution_log,
                    "pipeline_summary": {
                        "total_steps": len(self.execution_log),
                        "successful_steps": len([log for log in self.execution_log if log["status"] == "completed"]),
                        "failed_steps": len([log for log in self.execution_log if log["status"] == "failed"])
                    }
                }, f, indent=2, default=str)

            logger.info(f"Execution log saved to {log_filename}")

        except Exception as e:
            logger.warning(f"Failed to save execution log: {str(e)}")

def main():
    """Main entry point for the ETL pipeline."""
    logger.add(
        "pipeline_{time:YYYY-MM-DD}.log",
        rotation="1 day",
        retention="30 days",
        level="INFO"
    )

    config_path = None
    if len(sys.argv) > 1:
        config_path = sys.argv[1]

    pipeline = ETLPipeline(config_path)
    success = pipeline.run_pipeline()

    if success:
        print("ETL Pipeline completed successfully")
        sys.exit(0)
    else:
        print("ETL Pipeline failed")
        sys.exit(1)

if __name__ == "__main__":
    main()

