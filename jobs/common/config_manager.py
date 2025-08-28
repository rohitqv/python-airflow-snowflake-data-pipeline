"""
Configuration Management

Handles loading and merging of configuration files across different environments.
"""

import os
import yaml
from typing import Dict, Any, Optional


class ConfigManager:
    """
    Manages configuration loading and environment-specific overrides.
    """
    
    def __init__(self, config_path: str = "config/config.yaml", environment: str = "dev"):
        """
        Initialize configuration manager.
        
        Args:
            config_path: Path to the base configuration file
            environment: Environment name (dev, staging, prod)
        """
        self.config_path = config_path
        self.environment = environment
        self.config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        """
        Load and merge configuration files.
        
        Returns:
            Merged configuration dictionary
        """
        # Load base configuration
        base_config = self._load_yaml_file(self.config_path)
        
        # Load environment-specific configuration
        env_config_path = f"config/environments/{self.environment}.yaml"
        env_config = {}
        
        if os.path.exists(env_config_path):
            env_config = self._load_yaml_file(env_config_path)
        
        # Merge configurations (environment overrides base)
        merged_config = self._deep_merge(base_config, env_config)
        
        # Substitute environment variables
        merged_config = self._substitute_env_vars(merged_config)
        
        return merged_config
    
    def _load_yaml_file(self, file_path: str) -> Dict[str, Any]:
        """
        Load YAML file safely.
        
        Args:
            file_path: Path to YAML file
            
        Returns:
            Parsed YAML content as dictionary
        """
        try:
            with open(file_path, 'r') as file:
                return yaml.safe_load(file) or {}
        except FileNotFoundError:
            return {}
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML file {file_path}: {str(e)}")
    
    def _deep_merge(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """
        Deep merge two dictionaries.
        
        Args:
            base: Base dictionary
            override: Override dictionary
            
        Returns:
            Merged dictionary
        """
        result = base.copy()
        
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        
        return result
    
    def _substitute_env_vars(self, config: Any) -> Any:
        """
        Recursively substitute environment variables in configuration.
        
        Args:
            config: Configuration object (dict, list, or string)
            
        Returns:
            Configuration with environment variables substituted
        """
        if isinstance(config, dict):
            return {key: self._substitute_env_vars(value) for key, value in config.items()}
        elif isinstance(config, list):
            return [self._substitute_env_vars(item) for item in config]
        elif isinstance(config, str) and config.startswith("${") and config.endswith("}"):
            # Extract environment variable name
            env_var = config[2:-1]
            default_value = None
            
            # Handle default values (e.g., ${VAR_NAME:default_value})
            if ":" in env_var:
                env_var, default_value = env_var.split(":", 1)
            
            return os.getenv(env_var, default_value)
        else:
            return config
    
    def get_config(self) -> Dict[str, Any]:
        """
        Get the merged configuration.
        
        Returns:
            Configuration dictionary
        """
        return self.config
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation.
        
        Args:
            key_path: Dot-separated key path (e.g., 'spark.config.spark.executor.memory')
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        keys = key_path.split('.')
        value = self.config
        
        try:
            for key in keys:
                value = value[key]
            return value
        except (KeyError, TypeError):
            return default
    
    def reload_config(self):
        """Reload configuration from files."""
        self.config = self._load_config()


# Global configuration instance
_config_manager: Optional[ConfigManager] = None


def get_config_manager(config_path: str = "config/config.yaml", environment: str = "dev") -> ConfigManager:
    """
    Get or create global configuration manager instance.
    
    Args:
        config_path: Path to configuration file
        environment: Environment name
        
    Returns:
        ConfigManager instance
    """
    global _config_manager
    
    if _config_manager is None:
        _config_manager = ConfigManager(config_path, environment)
    
    return _config_manager
