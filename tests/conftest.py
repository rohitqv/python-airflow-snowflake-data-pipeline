"""
Pytest Configuration and Fixtures

Shared test configuration and fixtures for the entire test suite.
"""

import pytest
import os
import tempfile
from unittest.mock import Mock, MagicMock
from pyspark.sql import SparkSession
import pandas as pd


@pytest.fixture(scope="session")
def spark_session():
    """Create a Spark session for testing."""
    spark = SparkSession.builder \
        .appName("DataPipelineTests") \
        .master("local[2]") \
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp()) \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()
    
    yield spark
    
    spark.stop()


@pytest.fixture
def sample_config():
    """Sample configuration for testing."""
    return {
        'project': {
            'name': 'test-pipeline',
            'version': '1.0.0'
        },
        'data': {
            'raw_path': 'tests/fixtures/raw',
            'processed_path': 'tests/fixtures/processed'
        },
        'spark': {
            'app_name': 'TestApp',
            'master': 'local[2]',
            'config': {
                'spark.sql.adaptive.enabled': 'false'
            }
        },
        'snowflake': {
            'account': 'test_account',
            'user': 'test_user',
            'password': 'test_password',
            'warehouse': 'test_warehouse',
            'database': 'test_database',
            'schema': 'test_schema'
        },
        'data_quality': {
            'enable_validation': True,
            'max_corrupt_records_percent': 5,
            'null_check_threshold': 10
        },
        'monitoring': {
            'log_level': 'DEBUG'
        }
    }


@pytest.fixture
def sample_customer_data():
    """Sample customer data for testing."""
    return pd.DataFrame({
        'customer_id': ['cust_001', 'cust_002', 'cust_003'],
        'customer_city': ['São Paulo', 'Rio de Janeiro', 'Belo Horizonte'],
        'customer_state': ['SP', 'RJ', 'MG'],
        'customer_zip_code_prefix': ['01000', '20000', '30000']
    })


@pytest.fixture
def sample_orders_data():
    """Sample orders data for testing."""
    return pd.DataFrame({
        'order_id': ['order_001', 'order_002', 'order_003'],
        'customer_id': ['cust_001', 'cust_002', 'cust_003'],
        'order_status': ['delivered', 'shipped', 'processing'],
        'order_purchase_timestamp': ['2023-01-01 10:00:00', '2023-01-02 11:00:00', '2023-01-03 12:00:00']
    })


@pytest.fixture
def mock_snowflake_connector():
    """Mock Snowflake connector for testing."""
    mock_connector = Mock()
    mock_connector.write_dataframe = Mock()
    mock_connector.read_table = Mock()
    mock_connector.execute_query = Mock()
    mock_connector.table_exists = Mock(return_value=True)
    return mock_connector


@pytest.fixture
def temp_data_dir():
    """Create temporary directory for test data."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create subdirectories
        raw_dir = os.path.join(temp_dir, 'raw')
        processed_dir = os.path.join(temp_dir, 'processed')
        os.makedirs(raw_dir)
        os.makedirs(processed_dir)
        
        yield {
            'base': temp_dir,
            'raw': raw_dir,
            'processed': processed_dir
        }


@pytest.fixture
def sample_csv_files(temp_data_dir, sample_customer_data, sample_orders_data):
    """Create sample CSV files for testing."""
    customer_file = os.path.join(temp_data_dir['raw'], 'olist_customers_dataset.csv')
    orders_file = os.path.join(temp_data_dir['raw'], 'olist_orders_dataset.csv')
    
    sample_customer_data.to_csv(customer_file, index=False)
    sample_orders_data.to_csv(orders_file, index=False)
    
    return {
        'customers': customer_file,
        'orders': orders_file
    }


@pytest.fixture(autouse=True)
def setup_test_environment():
    """Set up test environment variables."""
    os.environ['ENVIRONMENT'] = 'test'
    os.environ['SNOWFLAKE_ACCOUNT'] = 'test_account'
    os.environ['SNOWFLAKE_USER'] = 'test_user'
    os.environ['SNOWFLAKE_PASSWORD'] = 'test_password'
    
    yield
    
    # Cleanup
    test_vars = ['ENVIRONMENT', 'SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD']
    for var in test_vars:
        if var in os.environ:
            del os.environ[var]


# Test markers
def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line("markers", "unit: Unit tests")
    config.addinivalue_line("markers", "integration: Integration tests")
    config.addinivalue_line("markers", "e2e: End-to-end tests")
    config.addinivalue_line("markers", "slow: Slow running tests")
    config.addinivalue_line("markers", "spark: Tests requiring Spark")
    config.addinivalue_line("markers", "snowflake: Tests requiring Snowflake connection")
