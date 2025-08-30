"""
Application settings and configuration management.

This module provides centralized configuration management using Pydantic
for type safety and validation. All environment variables and settings
are defined here with proper defaults and validation.
"""

from typing import List, Optional
from pydantic import Field, validator
from pydantic_settings import BaseSettings


class KafkaSettings(BaseSettings):
    """Kafka configuration settings."""
    
    bootstrap_servers: str = Field(default="localhost:9092", env="KAFKA_BOOTSTRAP_SERVERS")
    topic_market_data: str = Field(default="market-data", env="KAFKA_TOPIC_MARKET_DATA")
    topic_alerts: str = Field(default="alerts", env="KAFKA_TOPIC_ALERTS")
    consumer_group_id: str = Field(default="finance-data-consumer", env="KAFKA_CONSUMER_GROUP_ID")
    producer_client_id: str = Field(default="finance-data-producer", env="KAFKA_PRODUCER_CLIENT_ID")
    auto_offset_reset: str = Field(default="latest", env="KAFKA_AUTO_OFFSET_RESET")
    enable_auto_commit: bool = Field(default=True, env="KAFKA_ENABLE_AUTO_COMMIT")
    
    @validator("auto_offset_reset")
    def validate_auto_offset_reset(cls, v: str) -> str:
        """Validate auto offset reset value."""
        valid_values = ["earliest", "latest"]
        if v not in valid_values:
            raise ValueError(f"auto_offset_reset must be one of {valid_values}")
        return v


class DatabaseSettings(BaseSettings):
    """Database configuration settings."""
    
    host: str = Field(default="localhost", env="DB_HOST")  # Local connection
    port: int = Field(default=5432, env="DB_PORT")
    database: str = Field(default="postgres", env="DB_NAME")  # Use default postgres database
    username: str = Field(default="postgres", env="DB_USERNAME")
    password: str = Field(default="", env="DB_PASSWORD")  # No password
    pool_size: int = Field(default=10, env="DB_POOL_SIZE")
    max_overflow: int = Field(default=20, env="DB_MAX_OVERFLOW")
    echo: bool = Field(default=False, env="DB_ECHO")
    
    @property
    def connection_string(self) -> str:
        """Get database connection string."""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"


class SparkSettings(BaseSettings):
    """Apache Spark configuration settings."""
    
    master: str = Field(default="local[*]", env="SPARK_MASTER")
    app_name: str = Field(default="FinanceDataProcessing", env="SPARK_APP_NAME")
    checkpoint_location: str = Field(default="/tmp/spark-checkpoints", env="SPARK_CHECKPOINT_LOCATION")
    batch_duration: int = Field(default=60, env="SPARK_BATCH_DURATION")  # seconds
    max_retries: int = Field(default=3, env="SPARK_MAX_RETRIES")


class APISettings(BaseSettings):
    """Financial API configuration settings."""
    
    yahoo_finance_enabled: bool = Field(default=True, env="YAHOO_FINANCE_ENABLED")
    polygon_enabled: bool = Field(default=True, env="POLYGON_ENABLED")
    alpha_vantage_enabled: bool = Field(default=True, env="ALPHA_VANTAGE_ENABLED")
    
    # API Keys
    polygon_api_key: Optional[str] = Field(default=None, env="POLYGON_API_KEY")
    alpha_vantage_api_key: Optional[str] = Field(default=None, env="ALPHA_VANTAGE_API_KEY")
    
    # Rate limiting
    requests_per_minute: int = Field(default=60, env="API_REQUESTS_PER_MINUTE")
    retry_attempts: int = Field(default=3, env="API_RETRY_ATTEMPTS")
    retry_delay: int = Field(default=1, env="API_RETRY_DELAY")  # seconds


class MonitoringSettings(BaseSettings):
    """Monitoring and observability settings."""
    
    prometheus_port: int = Field(default=0, env="PROMETHEUS_PORT")  # Disabled to avoid conflict with Docker Prometheus
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    enable_tracing: bool = Field(default=True, env="ENABLE_TRACING")
    jaeger_host: str = Field(default="localhost", env="JAEGER_HOST")
    jaeger_port: int = Field(default=6831, env="JAEGER_PORT")
    
    @validator("log_level")
    def validate_log_level(cls, v: str) -> str:
        """Validate log level."""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(f"log_level must be one of {valid_levels}")
        return v.upper()


class SecuritySettings(BaseSettings):
    """Security configuration settings."""
    
    secret_key: str = Field(default="your-secret-key-here", env="SECRET_KEY")
    algorithm: str = Field(default="HS256", env="JWT_ALGORITHM")
    access_token_expire_minutes: int = Field(default=30, env="ACCESS_TOKEN_EXPIRE_MINUTES")
    enable_ssl: bool = Field(default=False, env="ENABLE_SSL")
    ssl_cert_file: Optional[str] = Field(default=None, env="SSL_CERT_FILE")
    ssl_key_file: Optional[str] = Field(default=None, env="SSL_KEY_FILE")


class Settings(BaseSettings):
    """Main application settings."""
    
    # Environment
    environment: str = Field(default="development", env="ENVIRONMENT")
    debug: bool = Field(default=False, env="DEBUG")
    
    # Service configuration
    service_name: str = Field(default="finance-data-platform", env="SERVICE_NAME")
    service_version: str = Field(default="1.0.0", env="SERVICE_VERSION")
    
    # Component settings
    kafka: KafkaSettings = KafkaSettings()
    database: DatabaseSettings = DatabaseSettings()
    spark: SparkSettings = SparkSettings()
    api: APISettings = APISettings()
    monitoring: MonitoringSettings = MonitoringSettings()
    security: SecuritySettings = SecuritySettings()
    
    # Trading configuration
    symbols: List[str] = Field(
        default=["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"],
        env="TRADING_SYMBOLS"
    )
    update_interval: int = Field(default=1, env="UPDATE_INTERVAL")  # seconds
    
    @validator("environment")
    def validate_environment(cls, v: str) -> str:
        """Validate environment value."""
        valid_environments = ["development", "staging", "production"]
        if v not in valid_environments:
            raise ValueError(f"environment must be one of {valid_environments}")
        return v
    
    @validator("symbols")
    def validate_symbols(cls, v: List[str]) -> List[str]:
        """Validate trading symbols."""
        if not v:
            raise ValueError("At least one trading symbol must be specified")
        return [symbol.upper() for symbol in v]
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "ignore"


# Global settings instance
settings = Settings()
