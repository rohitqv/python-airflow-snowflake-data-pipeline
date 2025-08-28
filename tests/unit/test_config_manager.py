"""
Unit tests for ConfigManager class.
"""

import pytest
import tempfile
import os
import yaml
from unittest.mock import patch, mock_open

# Add the project root to the path for imports
import sys
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from jobs.common.config_manager import ConfigManager


@pytest.mark.unit
class TestConfigManager:
    """Test cases for ConfigManager."""
    
    def test_load_base_config(self, sample_config):
        """Test loading base configuration."""
        config_content = yaml.dump(sample_config)
        
        with patch('builtins.open', mock_open(read_data=config_content)):
            with patch('os.path.exists', return_value=True):
                manager = ConfigManager('config/config.yaml', 'dev')
                config = manager.get_config()
                
                assert config['project']['name'] == 'test-pipeline'
                assert config['spark']['master'] == 'local[2]'
    
    def test_environment_override(self, sample_config):
        """Test environment-specific configuration override."""
        base_config = sample_config.copy()
        env_config = {
            'spark': {
                'master': 'local[4]',
                'config': {
                    'spark.executor.memory': '4g'
                }
            }
        }
        
        def mock_load_yaml(file_path):
            if 'environments/dev.yaml' in file_path:
                return env_config
            return base_config
        
        with patch.object(ConfigManager, '_load_yaml_file', side_effect=mock_load_yaml):
            with patch('os.path.exists', return_value=True):
                manager = ConfigManager('config/config.yaml', 'dev')
                config = manager.get_config()
                
                # Check that environment config overrides base config
                assert config['spark']['master'] == 'local[4]'
                assert config['spark']['config']['spark.executor.memory'] == '4g'
                # Check that non-overridden values remain
                assert config['project']['name'] == 'test-pipeline'
    
    def test_environment_variable_substitution(self):
        """Test environment variable substitution."""
        config_with_env_vars = {
            'snowflake': {
                'account': '${SNOWFLAKE_ACCOUNT}',
                'user': '${SNOWFLAKE_USER}',
                'password': '${SNOWFLAKE_PASSWORD:default_password}'
            }
        }
        
        with patch.object(ConfigManager, '_load_yaml_file', return_value=config_with_env_vars):
            with patch('os.path.exists', return_value=True):
                with patch.dict(os.environ, {
                    'SNOWFLAKE_ACCOUNT': 'test_account',
                    'SNOWFLAKE_USER': 'test_user'
                    # SNOWFLAKE_PASSWORD not set, should use default
                }):
                    manager = ConfigManager('config/config.yaml', 'dev')
                    config = manager.get_config()
                    
                    assert config['snowflake']['account'] == 'test_account'
                    assert config['snowflake']['user'] == 'test_user'
                    assert config['snowflake']['password'] == 'default_password'
    
    def test_get_with_dot_notation(self, sample_config):
        """Test getting configuration values with dot notation."""
        with patch.object(ConfigManager, '_load_yaml_file', return_value=sample_config):
            with patch('os.path.exists', return_value=True):
                manager = ConfigManager('config/config.yaml', 'dev')
                
                assert manager.get('project.name') == 'test-pipeline'
                assert manager.get('spark.config.spark.sql.adaptive.enabled') == 'false'
                assert manager.get('nonexistent.key', 'default') == 'default'
    
    def test_deep_merge(self):
        """Test deep merging of configuration dictionaries."""
        manager = ConfigManager.__new__(ConfigManager)  # Create instance without __init__
        
        base = {
            'level1': {
                'level2': {
                    'key1': 'value1',
                    'key2': 'value2'
                },
                'other_key': 'other_value'
            }
        }
        
        override = {
            'level1': {
                'level2': {
                    'key2': 'new_value2',
                    'key3': 'value3'
                }
            }
        }
        
        result = manager._deep_merge(base, override)
        
        assert result['level1']['level2']['key1'] == 'value1'  # Preserved
        assert result['level1']['level2']['key2'] == 'new_value2'  # Overridden
        assert result['level1']['level2']['key3'] == 'value3'  # Added
        assert result['level1']['other_key'] == 'other_value'  # Preserved
    
    def test_missing_config_file(self):
        """Test handling of missing configuration file."""
        with patch('os.path.exists', return_value=False):
            with patch.object(ConfigManager, '_load_yaml_file', return_value={}):
                manager = ConfigManager('nonexistent.yaml', 'dev')
                config = manager.get_config()
                
                assert config == {}
    
    def test_invalid_yaml_file(self):
        """Test handling of invalid YAML file."""
        invalid_yaml = "invalid: yaml: content: ["
        
        with patch('builtins.open', mock_open(read_data=invalid_yaml)):
            with patch('os.path.exists', return_value=True):
                with pytest.raises(ValueError, match="Error parsing YAML file"):
                    ConfigManager('config/config.yaml', 'dev')
    
    def test_reload_config(self, sample_config):
        """Test configuration reloading."""
        with patch.object(ConfigManager, '_load_yaml_file', return_value=sample_config):
            with patch('os.path.exists', return_value=True):
                manager = ConfigManager('config/config.yaml', 'dev')
                original_name = manager.get('project.name')
                
                # Modify the config and reload
                modified_config = sample_config.copy()
                modified_config['project']['name'] = 'modified-pipeline'
                
                with patch.object(ConfigManager, '_load_yaml_file', return_value=modified_config):
                    manager.reload_config()
                    
                    assert manager.get('project.name') == 'modified-pipeline'
                    assert manager.get('project.name') != original_name
