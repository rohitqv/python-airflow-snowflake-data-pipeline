"""
Data Quality Validator

Comprehensive data quality validation framework for Spark DataFrames.
"""

import logging
from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *


class DataQualityValidator:
    """
    Validates data quality for Spark DataFrames with configurable rules.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize data quality validator.
        
        Args:
            config: Configuration dictionary containing data quality settings
        """
        self.config = config
        self.dq_config = config.get('data_quality', {})
        self.logger = logging.getLogger(__name__)
    
    def validate_dataframe(self, df: DataFrame, table_name: str) -> Dict[str, Any]:
        """
        Run comprehensive data quality validations on DataFrame.
        
        Args:
            df: DataFrame to validate
            table_name: Name of the table for logging
            
        Returns:
            Dictionary with validation results
        """
        validation_results = {
            'table_name': table_name,
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'metrics': {}
        }
        
        try:
            self.logger.info(f"Running data quality validations for {table_name}")
            
            # Basic validations
            self._validate_not_empty(df, validation_results)
            self._validate_schema(df, validation_results)
            self._validate_null_values(df, validation_results)
            self._validate_duplicates(df, validation_results)
            
            # Table-specific validations
            if hasattr(self, f'_validate_{table_name.lower()}'):
                validate_method = getattr(self, f'_validate_{table_name.lower()}')
                validate_method(df, validation_results)
            
            # Calculate overall validity
            validation_results['is_valid'] = len(validation_results['errors']) == 0
            
            # Log results
            self._log_validation_results(validation_results)
            
        except Exception as e:
            validation_results['is_valid'] = False
            validation_results['errors'].append(f"Validation error: {str(e)}")
            self.logger.error(f"Error during validation: {str(e)}")
        
        return validation_results
    
    def _validate_not_empty(self, df: DataFrame, results: Dict[str, Any]):
        """Validate that DataFrame is not empty."""
        try:
            record_count = df.count()
            results['metrics']['record_count'] = record_count
            
            if record_count == 0:
                results['errors'].append("DataFrame is empty")
            else:
                self.logger.info(f"Record count validation passed: {record_count} records")
                
        except Exception as e:
            results['errors'].append(f"Error counting records: {str(e)}")
    
    def _validate_schema(self, df: DataFrame, results: Dict[str, Any]):
        """Validate DataFrame schema."""
        try:
            schema = df.schema
            column_count = len(schema.fields)
            results['metrics']['column_count'] = column_count
            
            if column_count == 0:
                results['errors'].append("DataFrame has no columns")
            
            # Check for duplicate column names
            column_names = [field.name for field in schema.fields]
            duplicate_columns = [name for name in set(column_names) if column_names.count(name) > 1]
            
            if duplicate_columns:
                results['errors'].append(f"Duplicate column names found: {duplicate_columns}")
            
            self.logger.info(f"Schema validation completed: {column_count} columns")
            
        except Exception as e:
            results['errors'].append(f"Error validating schema: {str(e)}")
    
    def _validate_null_values(self, df: DataFrame, results: Dict[str, Any]):
        """Validate null values in DataFrame."""
        try:
            if not self.dq_config.get('required_columns_check', True):
                return
            
            null_threshold = self.dq_config.get('null_check_threshold', 10)
            total_records = results['metrics'].get('record_count', df.count())
            
            null_counts = {}
            for column in df.columns:
                if not column.startswith('_'):  # Skip metadata columns
                    null_count = df.filter(col(column).isNull()).count()
                    null_percentage = (null_count / total_records) * 100 if total_records > 0 else 0
                    
                    null_counts[column] = {
                        'count': null_count,
                        'percentage': null_percentage
                    }
                    
                    if null_percentage > null_threshold:
                        results['warnings'].append(
                            f"Column '{column}' has {null_percentage:.2f}% null values (threshold: {null_threshold}%)"
                        )
            
            results['metrics']['null_counts'] = null_counts
            self.logger.info("Null value validation completed")
            
        except Exception as e:
            results['errors'].append(f"Error validating null values: {str(e)}")
    
    def _validate_duplicates(self, df: DataFrame, results: Dict[str, Any]):
        """Validate duplicate records in DataFrame."""
        try:
            total_records = results['metrics'].get('record_count', df.count())
            distinct_records = df.distinct().count()
            duplicate_count = total_records - distinct_records
            
            results['metrics']['duplicate_count'] = duplicate_count
            results['metrics']['distinct_count'] = distinct_records
            
            if duplicate_count > 0:
                duplicate_percentage = (duplicate_count / total_records) * 100
                results['warnings'].append(
                    f"Found {duplicate_count} duplicate records ({duplicate_percentage:.2f}%)"
                )
            
            self.logger.info(f"Duplicate validation completed: {duplicate_count} duplicates found")
            
        except Exception as e:
            results['errors'].append(f"Error validating duplicates: {str(e)}")
    
    def _validate_olist_customers_dataset(self, df: DataFrame, results: Dict[str, Any]):
        """Customer dataset specific validations."""
        try:
            # Check for required columns
            required_columns = ['customer_id', 'customer_city', 'customer_state']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                results['errors'].append(f"Missing required columns: {missing_columns}")
            
            # Validate customer_id uniqueness
            if 'customer_id' in df.columns:
                total_customers = df.count()
                unique_customers = df.select('customer_id').distinct().count()
                
                if total_customers != unique_customers:
                    results['errors'].append("customer_id is not unique")
            
            self.logger.info("Customer dataset validation completed")
            
        except Exception as e:
            results['errors'].append(f"Error in customer validation: {str(e)}")
    
    def _validate_olist_orders_dataset(self, df: DataFrame, results: Dict[str, Any]):
        """Orders dataset specific validations."""
        try:
            # Check for required columns
            required_columns = ['order_id', 'customer_id', 'order_status']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                results['errors'].append(f"Missing required columns: {missing_columns}")
            
            # Validate order_id uniqueness
            if 'order_id' in df.columns:
                total_orders = df.count()
                unique_orders = df.select('order_id').distinct().count()
                
                if total_orders != unique_orders:
                    results['errors'].append("order_id is not unique")
            
            # Validate order status values
            if 'order_status' in df.columns:
                valid_statuses = ['delivered', 'shipped', 'processing', 'canceled', 'unavailable', 'invoiced', 'created', 'approved']
                invalid_statuses = df.filter(~col('order_status').isin(valid_statuses)).select('order_status').distinct().collect()
                
                if invalid_statuses:
                    invalid_list = [row['order_status'] for row in invalid_statuses]
                    results['warnings'].append(f"Invalid order statuses found: {invalid_list}")
            
            self.logger.info("Orders dataset validation completed")
            
        except Exception as e:
            results['errors'].append(f"Error in orders validation: {str(e)}")
    
    def _validate_olist_products_dataset(self, df: DataFrame, results: Dict[str, Any]):
        """Products dataset specific validations."""
        try:
            # Check for required columns
            required_columns = ['product_id']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                results['errors'].append(f"Missing required columns: {missing_columns}")
            
            # Validate product_id uniqueness
            if 'product_id' in df.columns:
                total_products = df.count()
                unique_products = df.select('product_id').distinct().count()
                
                if total_products != unique_products:
                    results['errors'].append("product_id is not unique")
            
            # Validate numeric columns are positive
            numeric_columns = ['product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']
            for col_name in numeric_columns:
                if col_name in df.columns:
                    negative_count = df.filter(col(col_name) < 0).count()
                    if negative_count > 0:
                        results['warnings'].append(f"Found {negative_count} negative values in {col_name}")
            
            self.logger.info("Products dataset validation completed")
            
        except Exception as e:
            results['errors'].append(f"Error in products validation: {str(e)}")
    
    def _log_validation_results(self, results: Dict[str, Any]):
        """Log validation results."""
        table_name = results['table_name']
        
        if results['is_valid']:
            self.logger.info(f"✅ Data quality validation PASSED for {table_name}")
        else:
            self.logger.error(f"❌ Data quality validation FAILED for {table_name}")
        
        # Log errors
        for error in results['errors']:
            self.logger.error(f"ERROR: {error}")
        
        # Log warnings
        for warning in results['warnings']:
            self.logger.warning(f"WARNING: {warning}")
        
        # Log metrics
        metrics = results['metrics']
        if 'record_count' in metrics:
            self.logger.info(f"Records: {metrics['record_count']}")
        if 'duplicate_count' in metrics:
            self.logger.info(f"Duplicates: {metrics['duplicate_count']}")
        if 'column_count' in metrics:
            self.logger.info(f"Columns: {metrics['column_count']}")
