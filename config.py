"""
Configuration module for Databricks Genie Conversation API Demo
"""
import os
from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Settings(BaseSettings):
    """Application settings loaded from environment variables"""
    
    # Databricks Configuration
    databricks_host: str = Field(..., env="DATABRICKS_HOST")
    databricks_token: str = Field(..., env="DATABRICKS_TOKEN")
    genie_space_id: Optional[str] = Field(default=None, env="GENIE_SPACE_ID")
    
    # Rate Limiting Configuration
    max_retries: int = Field(default=5, env="MAX_RETRIES")
    initial_backoff: float = Field(default=1.0, env="INITIAL_BACKOFF")
    max_backoff: float = Field(default=60.0, env="MAX_BACKOFF")
    backoff_multiplier: float = Field(default=2.0, env="BACKOFF_MULTIPLIER")
    
    # Queue Configuration
    max_queue_size: int = Field(default=1000, env="MAX_QUEUE_SIZE")
    worker_threads: int = Field(default=4, env="WORKER_THREADS")
    
    # Logging Configuration
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    
    class Config:
        env_file = ".env"
        case_sensitive = False


# Global settings instance
settings = Settings()


