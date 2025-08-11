"""
NYC Taxi Data Analysis
=====================

This script contains the required analytical queries and additional insights
for the NYC Taxi data project.

Required Analyses:
1. Calculate average total_amount per month considering all trips
2. Calculate average passenger_count per hour in May
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from loguru import logger
import json
from datetime import datetime


class NYCTaxiAnalyzer:
    """
    Analyzer class for NYC Taxi data insights and required calculations.
    """
    
    def __init__(self, spark: SparkSession):
        """
        Initialize analyzer.
        
        Args:
            spark: SparkSession instance
        """
        self.spark = spark
        
    def get_monthly_average_total_amount(self) -> pd.DataFrame:
        """
        Calculate average total_amount per month considering all trips.
        
        This is one of the required analyses for the project.
        
        Returns:
            Pandas DataFrame with monthly averages
        """
        try:
            logger.info("Calculating monthly average total_amount")
            
            query = """
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
            
            result_df = self.spark.sql(query)
            pandas_df = result_df.toPandas()
            
            logger.info("Monthly average total_amount calculated successfully")
            logger.info("Results:")
            for _, row in pandas_df.iterrows():
                logger.info(f"  {row['month_name']}: ${row['avg_total_amount']:.2f} (from {row['total_trips']:,} trips)")
            
            return pandas_df
            
        except Exception as e:
            logger.error(f"Failed to calculate monthly averages: {str(e)}")
            raise
    
    def get_hourly_average_passenger_count_may(self) -> pd.DataFrame:
        """
        Calculate average passenger_count per hour in May.
        
        This is one of the required analyses for the project.
        
        Returns:
            Pandas DataFrame with hourly averages for May
        """
        try:
            logger.info("Calculating hourly average passenger_count for May")
            
            query = """
            SELECT 
                pickup_hour,
                CASE 
                    WHEN pickup_hour = 0 THEN '12:00 AM'
                    WHEN pickup_hour < 12 THEN CONCAT(pickup_hour, ':00 AM')
                    WHEN pickup_hour = 12 THEN '12:00 PM'
                    ELSE CONCAT(pickup_hour - 12, ':00 PM')
                END as hour_display,
                ROUND(avg_passenger_count, 2) as avg_passenger_count,
                total_trips
            FROM gold_hourly_aggregations_may 
            ORDER BY pickup_hour
            """
            
            result_df = self.spark.sql(query)
            pandas_df = result_df.toPandas()
            
            logger.info("Hourly average passenger_count for May calculated successfully")
            logger.info("Results:")
            for _, row in pandas_df.iterrows():
                logger.info(f"  {row['hour_display']}: {row['avg_passenger_count']:.2f} passengers (from {row['total_trips']:,} trips)")
            
            return pandas_df
            
        except Exception as e:
            logger.error(f"Failed to calculate hourly averages for May: {str(e)}")
            raise
    
    def create_monthly_revenue_visualization(self, monthly_df: pd.DataFrame):
        """
        Create visualization for monthly average total amounts.
        
        Args:
            monthly_df: DataFrame with monthly data
        """
        try:
            plt.figure(figsize=(12, 6))
            
            # Bar plot
            plt.subplot(1, 2, 1)
            bars = plt.bar(monthly_df['month_name'], monthly_df['avg_total_amount'], 
                          color='skyblue', alpha=0.8)
            plt.title('Average Trip Amount by Month\n(Jan-May 2023)', fontsize=14, fontweight='bold')
            plt.xlabel('Month')
            plt.ylabel('Average Total Amount ($)')
            plt.xticks(rotation=45)
            
            # Add value labels on bars
            for bar, value in zip(bars, monthly_df['avg_total_amount']):
                plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1,
                        f'${value:.2f}', ha='center', va='bottom', fontweight='bold')
            
            # Line plot
            plt.subplot(1, 2, 2)
            plt.plot(monthly_df['month_name'], monthly_df['avg_total_amount'], 
                    marker='o', linewidth=2, markersize=8, color='darkblue')
            plt.title('Monthly Trend: Average Trip Amount\n(Jan-May 2023)', fontsize=14, fontweight='bold')
            plt.xlabel('Month')
            plt.ylabel('Average Total Amount ($)')
            plt.xticks(rotation=45)
            plt.grid(True, alpha=0.3)
            
            # Add value labels
            for i, (month, value) in enumerate(zip(monthly_df['month_name'], monthly_df['avg_total_amount'])):
                plt.annotate(f'${value:.2f}', (i, value), textcoords="offset points", 
                           xytext=(0,10), ha='center', fontweight='bold')
            
            plt.tight_layout()
            plt.savefig('analysis/monthly_average_amounts.png', dpi=300, bbox_inches='tight')
            plt.show()
            
            logger.info("Monthly revenue visualization created")
            
        except Exception as e:
            logger.error(f"Failed to create monthly visualization: {str(e)}")
    
    def create_hourly_passenger_visualization(self, hourly_df: pd.DataFrame):
        """
        Create visualization for hourly passenger counts in May.
        
        Args:
            hourly_df: DataFrame with hourly data
        """
        try:
            plt.figure(figsize=(15, 8))
            
            # Bar plot
            plt.subplot(2, 1, 1)
            bars = plt.bar(range(24), hourly_df['avg_passenger_count'], 
                          color='lightcoral', alpha=0.8)
            plt.title('Average Passengers per Hour in May 2023', fontsize=16, fontweight='bold')
            plt.xlabel('Hour of Day')
            plt.ylabel('Average Passenger Count')
            plt.xticks(range(24), [f'{h:02d}:00' for h in range(24)], rotation=45)
            plt.grid(True, alpha=0.3)
            
            # Add value labels on bars
            for i, (bar, value) in enumerate(zip(bars, hourly_df['avg_passenger_count'])):
                plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.01,
                        f'{value:.2f}', ha='center', va='bottom', fontsize=8)
            
            # Line plot for trend
            plt.subplot(2, 1, 2)
            plt.plot(range(24), hourly_df['avg_passenger_count'], 
                    marker='o', linewidth=2, markersize=6, color='darkred')
            plt.title('Hourly Trend: Average Passengers (May 2023)', fontsize=16, fontweight='bold')
            plt.xlabel('Hour of Day')
            plt.ylabel('Average Passenger Count')
            plt.xticks(range(24), [f'{h:02d}:00' for h in range(24)], rotation=45)
            plt.grid(True, alpha=0.3)
            
            # Highlight peak hours
            max_passengers = hourly_df['avg_passenger_count'].max()
            peak_hours = hourly_df[hourly_df['avg_passenger_count'] == max_passengers]['pickup_hour'].values
            
            for peak_hour in peak_hours:
                plt.axvline(x=peak_hour, color='red', linestyle='--', alpha=0.7, 
                           label=f'Peak Hour: {peak_hour:02d}:00')
            
            plt.legend()
            plt.tight_layout()
            plt.savefig('analysis/hourly_passenger_counts_may.png', dpi=300, bbox_inches='tight')
            plt.show()
            
            logger.info("Hourly passenger visualization created")
            
        except Exception as e:
            logger.error(f"Failed to create hourly visualization: {str(e)}")
    
    def generate_business_insights(self) -> dict:
        """
        Generate additional business insights from the data.
        
        Returns:
            Dictionary with business insights
        """
        try:
            logger.info("Generating business insights")
            
            insights = {}
            
            # Get business KPIs
            kpis_query = """
            SELECT * FROM gold_business_kpis
            """
            kpis = self.spark.sql(kpis_query).collect()[0]
            
            insights['overall_metrics'] = {
                'total_trips': kpis.total_trips,
                'total_revenue': f"${kpis.total_revenue:,.2f}",
                'average_fare': f"${kpis.overall_avg_fare:.2f}",
                'average_passengers': f"{kpis.overall_avg_passengers:.2f}",
                'average_duration': f"{kpis.overall_avg_duration:.1f} minutes"
            }
            
            # Vendor analysis
            vendor_query = """
            SELECT 
                VendorID,
                total_trips,
                ROUND(avg_total_amount, 2) as avg_fare,
                ROUND(total_revenue, 2) as total_revenue,
                ROUND(revenue_per_trip, 2) as revenue_per_trip
            FROM gold_vendor_analysis
            ORDER BY total_trips DESC
            """
            vendor_df = self.spark.sql(vendor_query).toPandas()
            insights['vendor_analysis'] = vendor_df.to_dict('records')
            
            # Weekend vs Weekday
            weekend_query = """
            SELECT 
                day_type,
                total_trips,
                ROUND(avg_total_amount, 2) as avg_fare,
                ROUND(total_revenue, 2) as total_revenue
            FROM gold_weekend_analysis
            ORDER BY is_weekend
            """
            weekend_df = self.spark.sql(weekend_query).toPandas()
            insights['weekend_analysis'] = weekend_df.to_dict('records')
            
            # Peak hours analysis
            peak_hours_query = """
            SELECT 
                pickup_hour,
                total_trips,
                ROUND(avg_passenger_count, 2) as avg_passengers,
                ROUND(avg_total_amount, 2) as avg_fare
            FROM gold_hourly_aggregations_may
            ORDER BY total_trips DESC
            LIMIT 5
            """
            peak_hours_df = self.spark.sql(peak_hours_query).toPandas()
            insights['peak_hours_may'] = peak_hours_df.to_dict('records')
            
            logger.info("Business insights generated successfully")
            return insights
            
        except Exception as e:
            logger.error(f"Failed to generate business insights: {str(e)}")
            return {'error': str(e)}
    
    def export_results_to_json(self, monthly_df: pd.DataFrame, 
                              hourly_df: pd.DataFrame, insights: dict):
        """
        Export all results to JSON for easy consumption.
        
        Args:
            monthly_df: Monthly analysis results
            hourly_df: Hourly analysis results  
            insights: Business insights
        """
        try:
            results = {
                'analysis_timestamp': datetime.now().isoformat(),
                'required_analyses': {
                    'monthly_average_total_amount': monthly_df.to_dict('records'),
                    'hourly_average_passenger_count_may': hourly_df.to_dict('records')
                },
                'business_insights': insights,
                'summary': {
                    'data_period': 'January - May 2023',
                    'months_analyzed': len(monthly_df),
                    'hours_analyzed': len(hourly_df),
                    'highest_avg_fare_month': monthly_df.loc[monthly_df['avg_total_amount'].idxmax(), 'month_name'],
                    'peak_passenger_hour': hourly_df.loc[hourly_df['avg_passenger_count'].idxmax(), 'hour_display']
                }
            }
            
            with open('analysis/nyc_taxi_analysis_results.json', 'w') as f:
                json.dump(results, f, indent=2, default=str)
            
            logger.info("Results exported to analysis/nyc_taxi_analysis_results.json")
            
        except Exception as e:
            logger.error(f"Failed to export results: {str(e)}")
    
    def run_complete_analysis(self):
        """
        Run the complete analysis pipeline.
        """
        logger.info("="*60)
        logger.info("STARTING NYC TAXI DATA ANALYSIS")
        logger.info("="*60)
        
        try:
            # Required Analysis 1: Monthly average total_amount
            logger.info("\n1. CALCULATING MONTHLY AVERAGE TOTAL AMOUNTS")
            monthly_df = self.get_monthly_average_total_amount()
            
            # Required Analysis 2: Hourly average passenger_count in May
            logger.info("\n2. CALCULATING HOURLY AVERAGE PASSENGER COUNT (MAY)")
            hourly_df = self.get_hourly_average_passenger_count_may()
            
            # Generate additional business insights
            logger.info("\n3. GENERATING BUSINESS INSIGHTS")
            insights = self.generate_business_insights()
            
            # Create visualizations
            logger.info("\n4. CREATING VISUALIZATIONS")
            self.create_monthly_revenue_visualization(monthly_df)
            self.create_hourly_passenger_visualization(hourly_df)
            
            # Export results
            logger.info("\n5. EXPORTING RESULTS")
            self.export_results_to_json(monthly_df, hourly_df, insights)
            
            logger.info("="*60)
            logger.info("ANALYSIS COMPLETED SUCCESSFULLY")
            logger.info("="*60)
            
            # Print key findings
            print("\nKEY FINDINGS:")
            print("="*50)
            
            print("\nREQUIRED ANALYSIS RESULTS:")
            print("\n1. Monthly Average Total Amount:")
            for _, row in monthly_df.iterrows():
                print(f"   {row['month_name']}: ${row['avg_total_amount']:.2f}")
            
            print(f"\n2. Hourly Average Passenger Count (May):")
            peak_hour = hourly_df.loc[hourly_df['avg_passenger_count'].idxmax()]
            print(f"   Peak hour: {peak_hour['hour_display']} with {peak_hour['avg_passenger_count']:.2f} passengers")
            print(f"   Full hourly data available in analysis results")
            
            if 'overall_metrics' in insights:
                print(f"\nBUSINESS SUMMARY:")
                metrics = insights['overall_metrics']
                print(f"   Total trips: {metrics['total_trips']:,}")
                print(f"   Total revenue: {metrics['total_revenue']}")
                print(f"   Average fare: {metrics['average_fare']}")
                print(f"   Average passengers: {metrics['average_passengers']}")
            
            print(f"\nAll detailed results saved to: analysis/nyc_taxi_analysis_results.json")
            print(f"Visualizations saved to: analysis/")
            
        except Exception as e:
            logger.error(f"Analysis failed: {str(e)}")
            raise


def main():
    """Main function to run the analysis."""
    logger.add(
        "analysis/analysis_{time:YYYY-MM-DD}.log",
        rotation="1 day",
        retention="7 days",
        level="INFO"
    )
    
    spark = SparkSession.builder \
        .appName("NYC_Taxi_Analysis") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .enableHiveSupport() \
        .getOrCreate()
    
    try:
        analyzer = NYCTaxiAnalyzer(spark)
        analyzer.run_complete_analysis()
        print("Analysis completed successfully")
    except Exception as e:
        print(f"Analysis failed: {str(e)}")
        logger.error(f"Analysis failed: {str(e)}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

