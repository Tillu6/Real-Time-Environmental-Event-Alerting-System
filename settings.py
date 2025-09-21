# Configuration Settings for Real-Time Environmental Alerting System

import os
from typing import Dict, Any, List
from dataclasses import dataclass
from datetime import timedelta

@dataclass
class APIConfig:
    """Configuration for external APIs"""
    # USGS Earthquake API
    USGS_BASE_URL: str = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    USGS_RATE_LIMIT: int = 100  # requests per minute
    
    # NOAA Weather API
    NOAA_BASE_URL: str = "https://api.weather.gov"
    NOAA_RATE_LIMIT: int = 300  # requests per hour
    
    # Visual Crossing Weather API (backup)
    WEATHER_API_KEY: str = os.getenv("WEATHER_API_KEY", "")
    WEATHER_BASE_URL: str = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"

@dataclass
class FabricConfig:
    """Microsoft Fabric Configuration"""
    # Fabric Workspace
    WORKSPACE_ID: str = os.getenv("FABRIC_WORKSPACE_ID", "")
    TENANT_ID: str = os.getenv("FABRIC_TENANT_ID", "")
    
    # Lakehouse Configuration
    BRONZE_LAKEHOUSE: str = "environmental_bronze"
    SILVER_LAKEHOUSE: str = "environmental_silver"
    GOLD_WAREHOUSE: str = "environmental_gold"
    
    # Data Factory Pipeline
    PIPELINE_NAME: str = "environmental-real-time-pipeline"
    
    # Power BI Configuration
    DATASET_ID: str = os.getenv("POWERBI_DATASET_ID", "")
    REPORT_ID: str = os.getenv("POWERBI_REPORT_ID", "")

@dataclass
class AlertConfig:
    """Alert and Notification Configuration"""
    # Teams Configuration
    TEAMS_WEBHOOK_URL: str = os.getenv("TEAMS_WEBHOOK_URL", "")
    
    # SMS Configuration (Azure Communication Services)
    ACS_CONNECTION_STRING: str = os.getenv("ACS_CONNECTION_STRING", "")
    SMS_FROM_NUMBER: str = os.getenv("SMS_FROM_NUMBER", "")
    
    # Alert Thresholds
    EARTHQUAKE_MAGNITUDE_THRESHOLD: float = 5.0
    SEVERE_WEATHER_THRESHOLD: float = 0.8  # severity score
    
    # Alert Recipients
    CRITICAL_RECIPIENTS: List[str] = [
        os.getenv("EMERGENCY_CONTACT_1", ""),
        os.getenv("EMERGENCY_CONTACT_2", "")
    ]

@dataclass
class ProcessingConfig:
    """Data Processing Configuration"""
    # Spark Configuration
    SPARK_APP_NAME: str = "Environmental-Event-Processor"
    SPARK_MASTER: str = "local[*]"
    
    # Batch Processing Settings
    BATCH_SIZE: int = 1000
    PROCESSING_INTERVAL: timedelta = timedelta(minutes=5)
    
    # Anomaly Detection Parameters
    ISOLATION_FOREST_CONTAMINATION: float = 0.1
    STATISTICAL_THRESHOLD_ZSCORE: float = 3.0
    
    # Data Retention
    RAW_DATA_RETENTION_DAYS: int = 90
    PROCESSED_DATA_RETENTION_DAYS: int = 365

@dataclass
class MonitoringConfig:
    """Monitoring and Logging Configuration"""
    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # Application Insights
    APPINSIGHTS_INSTRUMENTATION_KEY: str = os.getenv("APPINSIGHTS_INSTRUMENTATION_KEY", "")
    
    # Health Check Endpoints
    HEALTH_CHECK_INTERVAL: timedelta = timedelta(minutes=1)

class Settings:
    """Main Settings Class"""
    
    def __init__(self):
        self.api = APIConfig()
        self.fabric = FabricConfig()
        self.alerts = AlertConfig()
        self.processing = ProcessingConfig()
        self.monitoring = MonitoringConfig()
        
        # Environment-specific overrides
        self.environment = os.getenv("ENVIRONMENT", "development")
        
        if self.environment == "production":
            self._apply_production_config()
        elif self.environment == "staging":
            self._apply_staging_config()
    
    def _apply_production_config(self):
        """Apply production-specific configurations"""
        self.processing.SPARK_MASTER = "fabric"  # Use Fabric Spark pools
        self.monitoring.LOG_LEVEL = "WARNING"
        self.processing.PROCESSING_INTERVAL = timedelta(minutes=2)  # More frequent processing
        
    def _apply_staging_config(self):
        """Apply staging-specific configurations"""
        self.processing.SPARK_MASTER = "fabric"
        self.monitoring.LOG_LEVEL = "DEBUG"
        self.processing.PROCESSING_INTERVAL = timedelta(minutes=10)
    
    def validate_config(self) -> Dict[str, Any]:
        """Validate configuration and return any missing required settings"""
        missing_config = {}
        
        # Check required environment variables
        required_vars = [
            ("FABRIC_WORKSPACE_ID", self.fabric.WORKSPACE_ID),
            ("FABRIC_TENANT_ID", self.fabric.TENANT_ID),
            ("TEAMS_WEBHOOK_URL", self.alerts.TEAMS_WEBHOOK_URL),
        ]
        
        for var_name, var_value in required_vars:
            if not var_value:
                missing_config[var_name] = "Required but not set"
        
        return missing_config
    
    def get_connection_strings(self) -> Dict[str, str]:
        """Get all connection strings for the application"""
        return {
            "fabric_workspace": f"https://api.fabric.microsoft.com/workspaces/{self.fabric.WORKSPACE_ID}",
            "usgs_api": self.api.USGS_BASE_URL,
            "noaa_api": self.api.NOAA_BASE_URL,
            "teams_webhook": self.alerts.TEAMS_WEBHOOK_URL,
        }

# Global settings instance
settings = Settings()

# Environment validation on import
missing_config = settings.validate_config()
if missing_config and settings.environment == "production":
    raise EnvironmentError(f"Missing required configuration: {missing_config}")

# Export commonly used configurations
__all__ = [
    "settings", 
    "APIConfig", 
    "FabricConfig", 
    "AlertConfig", 
    "ProcessingConfig", 
    "MonitoringConfig"
]
