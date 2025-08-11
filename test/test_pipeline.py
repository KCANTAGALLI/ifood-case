"""
NYC Taxi ETL Pipeline - Test Suite
=================================

Simple test script to validate the pipeline components and data quality.
"""

import unittest
from unittest.mock import Mock, patch
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Add src directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

class TestNYCTaxiPipeline(unittest.TestCase):
    """Test suite for NYC Taxi ETL Pipeline."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        cls.spark = SparkSession.builder \
            .appName("NYC_Taxi_Pipeline_Tests") \
            .config("spark.sql.adaptive.enabled", "false") \
            .getOrCreate()
        
        cls.spark.sparkContext.setLogLevel("WARN")
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        cls.spark.stop()
    
    def test_spark_session(self):
        """Test Spark session is available."""
        self.assertIsNotNone(self.spark)
        self.assertIn("3.", self.spark.version)  # Spark 3.x
    
    def test_bronze_layer_import(self):
        """Test Bronze layer module can be imported."""
        try:
            from bronze_layer import BronzeLayer
            self.assertTrue(True)
        except ImportError as e:
            self.fail(f"Failed to import BronzeLayer: {e}")
    
    def test_silver_layer_import(self):
        """Test Silver layer module can be imported."""
        try:
            from silver_layer import SilverLayer
            self.assertTrue(True)
        except ImportError as e:
            self.fail(f"Failed to import SilverLayer: {e}")
    
    def test_gold_layer_import(self):
        """Test Gold layer module can be imported."""
        try:
            from gold_layer import GoldLayer
            self.assertTrue(True)
        except ImportError as e:
            self.fail(f"Failed to import GoldLayer: {e}")
    
    def test_etl_pipeline_import(self):
        """Test ETL pipeline module can be imported."""
        try:
            from etl_pipeline import ETLPipeline
            self.assertTrue(True)
        except ImportError as e:
            self.fail(f"Failed to import ETLPipeline: {e}")
    
    def test_bronze_layer_initialization(self):
        """Test Bronze layer can be initialized."""
        from bronze_layer import BronzeLayer
        
        bronze = BronzeLayer(self.spark, "/tmp/test_bronze")
        self.assertIsNotNone(bronze.spark)
        self.assertEqual(bronze.bronze_path, "/tmp/test_bronze")
    
    def test_silver_layer_initialization(self):
        """Test Silver layer can be initialized."""
        from silver_layer import SilverLayer
        
        silver = SilverLayer(self.spark, "/tmp/test_silver")
        self.assertIsNotNone(silver.spark)
        self.assertEqual(silver.silver_path, "/tmp/test_silver")
        
        # Test required columns are defined
        self.assertIn('VendorID', silver.required_columns)
        self.assertIn('passenger_count', silver.required_columns)
        self.assertIn('total_amount', silver.required_columns)
        self.assertIn('tpep_pickup_datetime', silver.required_columns)
        self.assertIn('tpep_dropoff_datetime', silver.required_columns)
    
    def test_gold_layer_initialization(self):
        """Test Gold layer can be initialized."""
        from gold_layer import GoldLayer
        
        gold = GoldLayer(self.spark, "/tmp/test_gold")
        self.assertIsNotNone(gold.spark)
        self.assertEqual(gold.gold_path, "/tmp/test_gold")
    
    def test_etl_pipeline_initialization(self):
        """Test ETL pipeline can be initialized."""
        from etl_pipeline import ETLPipeline
        
        pipeline = ETLPipeline()
        self.assertIsNotNone(pipeline.config)
        self.assertIn('app_name', pipeline.config)
        self.assertIn('data_paths', pipeline.config)
    
    def test_data_urls_generation(self):
        """Test NYC taxi data URLs are generated correctly."""
        from bronze_layer import BronzeLayer
        
        bronze = BronzeLayer(self.spark, "/tmp/test")
        urls = bronze.get_data_urls(2023, [1, 2])
        
        self.assertEqual(len(urls), 2)
        self.assertIn("yellow_tripdata_2023-01.parquet", urls[0])
        self.assertIn("yellow_tripdata_2023-02.parquet", urls[1])
    
    def test_schema_validation_logic(self):
        """Test schema validation with sample data."""
        from silver_layer import SilverLayer
        
        # Create sample DataFrame with required columns
        sample_data = [
            (1, 2, 15.50, "2023-01-01 10:00:00", "2023-01-01 10:30:00"),
            (2, 1, 12.75, "2023-01-01 11:00:00", "2023-01-01 11:25:00")
        ]
        
        schema = StructType([
            StructField("VendorID", IntegerType(), True),
            StructField("passenger_count", IntegerType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("tpep_pickup_datetime", StringType(), True),
            StructField("tpep_dropoff_datetime", StringType(), True)
        ])
        
        df = self.spark.createDataFrame(sample_data, schema)
        
        silver = SilverLayer(self.spark, "/tmp/test")
        
        # Convert datetime strings to timestamps for validation
        df_with_timestamps = df.withColumn(
            "tpep_pickup_datetime", 
            self.spark.sql("SELECT to_timestamp('2023-01-01 10:00:00')").collect()[0][0]
        ).withColumn(
            "tpep_dropoff_datetime",
            self.spark.sql("SELECT to_timestamp('2023-01-01 10:30:00')").collect()[0][0]
        )
        
        # This should not raise an exception
        validated_df = silver.validate_schema(df_with_timestamps)
        self.assertEqual(validated_df.count(), 2)
    
    def test_quality_rules_configuration(self):
        """Test data quality rules are properly configured."""
        from silver_layer import SilverLayer
        
        silver = SilverLayer(self.spark, "/tmp/test")
        
        # Test passenger count rules
        self.assertIn('passenger_count', silver.quality_rules)
        self.assertEqual(silver.quality_rules['passenger_count']['min'], 0)
        self.assertEqual(silver.quality_rules['passenger_count']['max'], 8)
        
        # Test total amount rules
        self.assertIn('total_amount', silver.quality_rules)
        self.assertEqual(silver.quality_rules['total_amount']['min'], 0)
        self.assertEqual(silver.quality_rules['total_amount']['max'], 1000)
    
    def test_analysis_imports(self):
        """Test analysis module can be imported."""
        try:
            sys.path.append(os.path.join(os.path.dirname(__file__), 'analysis'))
            from nyc_taxi_analysis import NYCTaxiAnalyzer
            self.assertTrue(True)
        except ImportError as e:
            self.fail(f"Failed to import NYCTaxiAnalyzer: {e}")


class TestDataQuality(unittest.TestCase):
    """Test data quality validation functions."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        cls.spark = SparkSession.builder \
            .appName("Data_Quality_Tests") \
            .getOrCreate()
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        cls.spark.stop()
    
    def test_required_columns_validation(self):
        """Test that required columns are properly validated."""
        from silver_layer import SilverLayer
        
        silver = SilverLayer(self.spark, "/tmp/test")
        required_cols = silver.required_columns.keys()
        
        expected_cols = {
            'VendorID', 'passenger_count', 'total_amount', 
            'tpep_pickup_datetime', 'tpep_dropoff_datetime'
        }
        
        self.assertEqual(set(required_cols), expected_cols)
    
    def test_data_type_mapping(self):
        """Test that data types are properly mapped."""
        from silver_layer import SilverLayer
        
        silver = SilverLayer(self.spark, "/tmp/test")
        
        self.assertIsInstance(silver.required_columns['VendorID'], IntegerType)
        self.assertIsInstance(silver.required_columns['passenger_count'], IntegerType)
        self.assertIsInstance(silver.required_columns['total_amount'], DoubleType)
        self.assertIsInstance(silver.required_columns['tpep_pickup_datetime'], TimestampType)
        self.assertIsInstance(silver.required_columns['tpep_dropoff_datetime'], TimestampType)


def run_tests():
    """Run all tests and display results."""
    print("Running NYC Taxi ETL Pipeline Tests")
    print("=" * 50)
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test classes
    suite.addTests(loader.loadTestsFromTestCase(TestNYCTaxiPipeline))
    suite.addTests(loader.loadTestsFromTestCase(TestDataQuality))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print("\n" + "=" * 50)
    if result.wasSuccessful():
        print("All tests passed successfully!")
        print(f"   Tests run: {result.testsRun}")
        print(f"   Failures: {len(result.failures)}")
        print(f"   Errors: {len(result.errors)}")
    else:
        print("Some tests failed!")
        print(f"   Tests run: {result.testsRun}")
        print(f"   Failures: {len(result.failures)}")
        print(f"   Errors: {len(result.errors)}")
        
        if result.failures:
            print("\nFailures:")
            for test, traceback in result.failures:
                print(f"  - {test}: {traceback}")
        
        if result.errors:
            print("\nErrors:")
            for test, traceback in result.errors:
                print(f"  - {test}: {traceback}")
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_tests()
    exit(0 if success else 1)
