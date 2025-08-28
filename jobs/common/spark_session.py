"""
Spark Session Management

Centralized Spark session creation and configuration management.
"""

import os
from typing import Dict, Any
from pyspark.sql import SparkSession


def get_spark_session(config: Dict[str, Any]) -> SparkSession:
    """
    Create and configure Spark session based on environment configuration.
    
    Args:
        config: Configuration dictionary containing Spark settings
        
    Returns:
        Configured SparkSession instance
    """
    spark_config = config.get('spark', {})
    app_name = spark_config.get('app_name', 'DataPipeline')
    master = spark_config.get('master', 'local[*]')
    
    # Build Spark session
    builder = SparkSession.builder \
        .appName(app_name) \
        .master(master)
    
    # Add Spark configurations
    spark_conf = spark_config.get('config', {})
    for key, value in spark_conf.items():
        builder = builder.config(key, value)
    
    # Add Snowflake connector JARs (use local JARs if available, otherwise packages)
    jar_path = "/opt/airflow/jars"
    if os.path.exists(jar_path):
        # Use local JARs (Docker environment)
        jars = [
            f"{jar_path}/snowflake-jdbc-3.13.22.jar",
            f"{jar_path}/spark-snowflake_2.12-2.11.0-spark_3.2.jar"
        ]
        builder = builder.config("spark.jars", ",".join(jars))
    else:
        # Use Maven packages (local development)
        builder = builder.config(
            "spark.jars.packages",
            "net.snowflake:snowflake-jdbc:3.13.22,net.snowflake:spark-snowflake_2.12:2.11.0-spark_3.2"
        )
    
    # Create session
    spark = builder.getOrCreate()
    
    # Set log level
    log_level = config.get('monitoring', {}).get('log_level', 'INFO')
    spark.sparkContext.setLogLevel(log_level)
    
    return spark


def stop_spark_session(spark: SparkSession):
    """
    Properly stop Spark session.
    
    Args:
        spark: SparkSession to stop
    """
    if spark:
        spark.stop()
