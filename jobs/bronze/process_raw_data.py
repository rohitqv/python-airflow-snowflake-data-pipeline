"""
Bronze Layer: Raw Data Processing Job

This job processes all raw CSV files and loads them into Snowflake Bronze layer
with minimal transformations and comprehensive error handling.
"""

import os
import sys
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Add the project root to the path for imports
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from jobs.common.spark_session import get_spark_session
from jobs.common.config_manager import ConfigManager
from jobs.common.data_quality import DataQualityValidator
from jobs.common.snowflake_connector import SnowflakeConnector


class BronzeDataProcessor:
    """
    Processes raw data files and loads them into Snowflake Bronze layer.
    
    Features:
    - Automatic file discovery
    - Schema validation
    - Data quality checks
    - Error handling and logging
    - Metadata enrichment
    """
    
    def __init__(self, config_path: str = "config/config.yaml", environment: str = "dev"):
        """Initialize the Bronze Data Processor."""
        self.config_manager = ConfigManager(config_path, environment)
        self.config = self.config_manager.get_config()
        self.spark = get_spark_session(self.config)
        self.logger = self._setup_logging()
        self.data_quality = DataQualityValidator(self.config)
        self.snowflake = SnowflakeConnector(self.config)
        
        # Processing tracking
        self.processed_tables = []
        self.failed_tables = []
        self.processing_stats = {}
    
    def _setup_logging(self) -> logging.Logger:
        """Set up logging configuration."""
        log_level = self.config.get('monitoring', {}).get('log_level', 'INFO')
        logging.basicConfig(
            level=getattr(logging, log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        return logging.getLogger(__name__)
    
    def discover_raw_files(self) -> List[Tuple[str, str]]:
        """
        Discover all CSV files in the raw data directory.
        
        Returns:
            List of tuples (file_path, table_name)
        """
        raw_path = self.config['data']['raw_path']
        csv_files = []
        
        if not os.path.exists(raw_path):
            raise FileNotFoundError(f"Raw data directory not found: {raw_path}")
        
        for file in os.listdir(raw_path):
            if file.endswith('.csv'):
                full_path = os.path.join(raw_path, file)
                # Convert filename to table name (remove .csv and convert to uppercase)
                table_name = file.replace('.csv', '').upper()
                csv_files.append((full_path, table_name))
        
        self.logger.info(f"Discovered {len(csv_files)} CSV files to process")
        return csv_files
    
    def read_csv_file(self, file_path: str, table_name: str) -> DataFrame:
        """
        Read CSV file with comprehensive error handling.
        
        Args:
            file_path: Path to the CSV file
            table_name: Name of the target table
            
        Returns:
            Spark DataFrame with the data
        """
        try:
            self.logger.info(f"Reading file: {file_path}")
            
            # Read CSV with flexible options
            df = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "false") \
                .option("mode", "PERMISSIVE") \
                .option("columnNameOfCorruptRecord", "_corrupt_record") \
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
                .option("dateFormat", "yyyy-MM-dd") \
                .csv(file_path)
            
            # Handle corrupt records
            total_records = df.count()
            corrupt_records = df.filter(col("_corrupt_record").isNotNull())
            corrupt_count = corrupt_records.count()
            
            if corrupt_count > 0:
                corrupt_percentage = (corrupt_count / total_records) * 100
                max_corrupt_percent = self.config.get('data_quality', {}).get('max_corrupt_records_percent', 5)
                
                self.logger.warning(f"Found {corrupt_count} corrupt records ({corrupt_percentage:.2f}%) in {table_name}")
                
                if corrupt_percentage > max_corrupt_percent:
                    raise ValueError(f"Corrupt records exceed threshold: {corrupt_percentage:.2f}% > {max_corrupt_percent}%")
                
                # Log corrupt records for investigation
                self._log_corrupt_records(corrupt_records, table_name)
            
            # Return clean dataframe
            clean_df = df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
            
            self.logger.info(f"Successfully read {clean_df.count()} clean records from {table_name}")
            return clean_df
            
        except Exception as e:
            self.logger.error(f"Error reading {table_name}: {str(e)}")
            raise
    
    def _log_corrupt_records(self, corrupt_df: DataFrame, table_name: str):
        """Log corrupt records for investigation."""
        try:
            corrupt_log_path = f"logs/corrupt_records/{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            corrupt_df.select("_corrupt_record").write.mode("overwrite").text(corrupt_log_path)
            self.logger.info(f"Corrupt records logged to: {corrupt_log_path}")
        except Exception as e:
            self.logger.warning(f"Failed to log corrupt records: {str(e)}")
    
    def apply_bronze_transformations(self, df: DataFrame, table_name: str) -> DataFrame:
        """
        Apply Bronze layer transformations.
        
        Args:
            df: Input DataFrame
            table_name: Name of the table
            
        Returns:
            Transformed DataFrame with metadata columns
        """
        try:
            # Add standard metadata columns
            batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            transformed_df = df \
                .withColumn("_ingestion_timestamp", current_timestamp()) \
                .withColumn("_source_file", lit(table_name)) \
                .withColumn("_processing_date", current_date()) \
                .withColumn("_batch_id", lit(batch_id)) \
                .withColumn("_record_hash", sha2(concat_ws("|", *df.columns), 256))
            
            # Apply table-specific transformations
            if hasattr(self, f'_transform_{table_name.lower()}'):
                transform_method = getattr(self, f'_transform_{table_name.lower()}')
                transformed_df = transform_method(transformed_df)
            
            return transformed_df
            
        except Exception as e:
            self.logger.error(f"Error transforming {table_name}: {str(e)}")
            raise
    
    def _transform_olist_customers_dataset(self, df: DataFrame) -> DataFrame:
        """Customer-specific transformations."""
        return df \
            .withColumn("customer_city", upper(trim(col("customer_city")))) \
            .withColumn("customer_state", upper(trim(col("customer_state"))))
    
    def _transform_olist_orders_dataset(self, df: DataFrame) -> DataFrame:
        """Order-specific transformations."""
        return df \
            .withColumn("order_purchase_timestamp", to_timestamp(col("order_purchase_timestamp"))) \
            .withColumn("order_approved_at", to_timestamp(col("order_approved_at"))) \
            .withColumn("order_delivered_carrier_date", to_timestamp(col("order_delivered_carrier_date"))) \
            .withColumn("order_delivered_customer_date", to_timestamp(col("order_delivered_customer_date"))) \
            .withColumn("order_estimated_delivery_date", to_date(col("order_estimated_delivery_date")))
    
    def _transform_olist_products_dataset(self, df: DataFrame) -> DataFrame:
        """Product-specific transformations."""
        return df \
            .withColumn("product_weight_g", col("product_weight_g").cast("double")) \
            .withColumn("product_length_cm", col("product_length_cm").cast("double")) \
            .withColumn("product_height_cm", col("product_height_cm").cast("double")) \
            .withColumn("product_width_cm", col("product_width_cm").cast("double"))
    
    def _transform_olist_order_payments_dataset(self, df: DataFrame) -> DataFrame:
        """Payment-specific transformations."""
        return df \
            .withColumn("payment_value", col("payment_value").cast("decimal(10,2)")) \
            .withColumn("payment_installments", col("payment_installments").cast("int"))
    
    def validate_and_write_to_snowflake(self, df: DataFrame, table_name: str):
        """
        Validate data quality and write to Snowflake Bronze layer.
        
        Args:
            df: DataFrame to write
            table_name: Target table name
        """
        try:
            # Run data quality validations
            if self.config.get('data_quality', {}).get('enable_validation', True):
                validation_results = self.data_quality.validate_dataframe(df, table_name)
                if not validation_results['is_valid']:
                    raise ValueError(f"Data quality validation failed: {validation_results['errors']}")
            
            # Write to Snowflake
            record_count = df.count()
            self.snowflake.write_dataframe(df, f"BRONZE.{table_name}", mode="overwrite")
            
            # Track processing stats
            self.processing_stats[table_name] = {
                'record_count': record_count,
                'processing_time': datetime.now(),
                'status': 'success'
            }
            
            self.processed_tables.append(table_name)
            self.logger.info(f"Successfully wrote {record_count} records to BRONZE.{table_name}")
            
        except Exception as e:
            self.processing_stats[table_name] = {
                'error': str(e),
                'processing_time': datetime.now(),
                'status': 'failed'
            }
            self.failed_tables.append(table_name)
            self.logger.error(f"Error writing {table_name} to Snowflake: {str(e)}")
            raise
    
    def process_all_files(self):
        """Main method to process all raw files."""
        try:
            self.logger.info("Starting Bronze layer processing...")
            
            # Discover files
            raw_files = self.discover_raw_files()
            
            if not raw_files:
                self.logger.warning("No CSV files found in raw data directory")
                return
            
            # Process each file
            for file_path, table_name in raw_files:
                try:
                    self.logger.info(f"Processing {table_name}...")
                    
                    # Read file
                    df = self.read_csv_file(file_path, table_name)
                    
                    # Apply transformations
                    transformed_df = self.apply_bronze_transformations(df, table_name)
                    
                    # Validate and write to Snowflake
                    self.validate_and_write_to_snowflake(transformed_df, table_name)
                    
                    self.logger.info(f"Successfully processed {table_name}")
                    
                except Exception as e:
                    self.logger.error(f"Failed to process {table_name}: {str(e)}")
                    # Continue with other files instead of failing completely
                    continue
            
            # Generate summary report
            self._generate_summary_report()
            
        except Exception as e:
            self.logger.error(f"Critical error in Bronze processing: {str(e)}")
            raise
        finally:
            self.spark.stop()
    
    def _generate_summary_report(self):
        """Generate processing summary report."""
        self.logger.info("=" * 60)
        self.logger.info("BRONZE LAYER PROCESSING SUMMARY")
        self.logger.info("=" * 60)
        self.logger.info(f"Successfully processed: {len(self.processed_tables)} tables")
        self.logger.info(f"Failed tables: {len(self.failed_tables)}")
        
        if self.processed_tables:
            self.logger.info(f"Processed tables: {', '.join(self.processed_tables)}")
            
        if self.failed_tables:
            self.logger.error(f"Failed tables: {', '.join(self.failed_tables)}")
        
        # Log processing stats
        for table, stats in self.processing_stats.items():
            if stats['status'] == 'success':
                self.logger.info(f"{table}: {stats['record_count']} records processed")
            else:
                self.logger.error(f"{table}: Failed - {stats['error']}")
        
        self.logger.info("=" * 60)


def main():
    """Main entry point for the Bronze data processing job."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Bronze Layer Data Processing')
    parser.add_argument('--config', default='config/config.yaml', help='Configuration file path')
    parser.add_argument('--env', default='dev', choices=['dev', 'staging', 'prod'], help='Environment')
    
    args = parser.parse_args()
    
    try:
        processor = BronzeDataProcessor(args.config, args.env)
        processor.process_all_files()
        
        # Exit with error code if any tables failed
        if processor.failed_tables:
            sys.exit(1)
        else:
            sys.exit(0)
            
    except Exception as e:
        logging.error(f"Critical error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
