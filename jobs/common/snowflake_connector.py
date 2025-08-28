"""
Snowflake Connector

Handles Snowflake database connections and operations for Spark DataFrames.
"""

import logging
from typing import Dict, Any, Optional
from pyspark.sql import DataFrame


class SnowflakeConnector:
    """
    Manages Snowflake connections and data operations.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Snowflake connector.
        
        Args:
            config: Configuration dictionary containing Snowflake settings
        """
        self.config = config
        self.snowflake_config = config.get('snowflake', {})
        self.logger = logging.getLogger(__name__)
        
        # Validate required configuration
        self._validate_config()
    
    def _validate_config(self):
        """Validate required Snowflake configuration parameters."""
        required_params = ['account', 'user', 'password', 'warehouse', 'database', 'schema']
        missing_params = []
        
        for param in required_params:
            if not self.snowflake_config.get(param):
                missing_params.append(param)
        
        if missing_params:
            raise ValueError(f"Missing required Snowflake configuration parameters: {missing_params}")
    
    def get_connection_options(self) -> Dict[str, str]:
        """
        Get Snowflake connection options for Spark.
        
        Returns:
            Dictionary of connection options
        """
        return {
            "sfURL": f"{self.snowflake_config['account']}.snowflakecomputing.com",
            "sfUser": self.snowflake_config['user'],
            "sfPassword": self.snowflake_config['password'],
            "sfDatabase": self.snowflake_config['database'],
            "sfSchema": self.snowflake_config['schema'],
            "sfWarehouse": self.snowflake_config['warehouse'],
            "sfRole": self.snowflake_config.get('role', 'PUBLIC')
        }
    
    def write_dataframe(self, 
                       df: DataFrame, 
                       table_name: str, 
                       mode: str = "overwrite",
                       pre_actions: Optional[str] = None,
                       post_actions: Optional[str] = None) -> None:
        """
        Write DataFrame to Snowflake table.
        
        Args:
            df: Spark DataFrame to write
            table_name: Target table name (can include schema)
            mode: Write mode ('overwrite', 'append', 'ignore', 'error')
            pre_actions: SQL commands to execute before writing
            post_actions: SQL commands to execute after writing
        """
        try:
            self.logger.info(f"Writing DataFrame to Snowflake table: {table_name}")
            
            # Get connection options
            options = self.get_connection_options()
            
            # Build write operation
            writer = df.write.format("snowflake").options(**options)
            
            # Add table name
            writer = writer.option("dbtable", table_name)
            
            # Add pre/post actions if specified
            if pre_actions:
                writer = writer.option("preactions", pre_actions)
            
            if post_actions:
                writer = writer.option("postactions", post_actions)
            
            # Execute write
            writer.mode(mode).save()
            
            record_count = df.count()
            self.logger.info(f"Successfully wrote {record_count} records to {table_name}")
            
        except Exception as e:
            self.logger.error(f"Error writing to Snowflake table {table_name}: {str(e)}")
            raise
    
    def read_table(self, table_name: str) -> DataFrame:
        """
        Read table from Snowflake.
        
        Args:
            table_name: Table name to read
            
        Returns:
            Spark DataFrame with table data
        """
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            
            if not spark:
                raise RuntimeError("No active Spark session found")
            
            self.logger.info(f"Reading table from Snowflake: {table_name}")
            
            # Get connection options
            options = self.get_connection_options()
            
            # Read table
            df = spark.read.format("snowflake") \
                .options(**options) \
                .option("dbtable", table_name) \
                .load()
            
            self.logger.info(f"Successfully read table {table_name}")
            return df
            
        except Exception as e:
            self.logger.error(f"Error reading Snowflake table {table_name}: {str(e)}")
            raise
    
    def execute_query(self, query: str) -> DataFrame:
        """
        Execute SQL query on Snowflake.
        
        Args:
            query: SQL query to execute
            
        Returns:
            Spark DataFrame with query results
        """
        try:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()
            
            if not spark:
                raise RuntimeError("No active Spark session found")
            
            self.logger.info("Executing query on Snowflake")
            
            # Get connection options
            options = self.get_connection_options()
            
            # Execute query
            df = spark.read.format("snowflake") \
                .options(**options) \
                .option("query", query) \
                .load()
            
            self.logger.info("Successfully executed query")
            return df
            
        except Exception as e:
            self.logger.error(f"Error executing query: {str(e)}")
            raise
    
    def table_exists(self, table_name: str) -> bool:
        """
        Check if table exists in Snowflake.
        
        Args:
            table_name: Table name to check
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            # Parse table name to get schema and table
            if '.' in table_name:
                schema, table = table_name.split('.', 1)
            else:
                schema = self.snowflake_config['schema']
                table = table_name
            
            query = f"""
            SELECT COUNT(*) as table_count
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{schema.upper()}' 
            AND TABLE_NAME = '{table.upper()}'
            """
            
            result_df = self.execute_query(query)
            count = result_df.collect()[0]['TABLE_COUNT']
            
            return count > 0
            
        except Exception as e:
            self.logger.warning(f"Error checking table existence: {str(e)}")
            return False
