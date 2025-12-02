"""
Configuration settings for the FastAPI application.
"""
from pathlib import Path
from typing import Optional
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings."""
    
    # Data directory (relative to project root or absolute path)
    DATA_DIRECTORY: str = "data"
    
    # S3 Configuration
    S3_BUCKET_NAME: str
    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: str
    AWS_REGION: str = "us-east-1"
    
    # S3 Prefix (optional, for organizing files in bucket)
    S3_PREFIX: str = "inmet-data"
    
    # MLflow Configuration
    MLFLOW_TRACKING_URI: str = "http://mlflow:5000"
    MLFLOW_EXPERIMENT_NAME: str = "data-pipeline"
    
    # ThingsBoard Configuration
    THINGSBOARD_URL: str = "http://thingsboard:9090"
    THINGSBOARD_USERNAME: Optional[str] = None
    THINGSBOARD_PASSWORD: Optional[str] = None
    
    # Trendz Configuration
    TRENDZ_URL: str = "http://trendz:8888"
    
    class Config:
        env_file = ".env"
        case_sensitive = True
    
    def get_data_directory(self) -> Path:
        """
        Get the absolute path to the data directory.
        
        Returns:
            Path object pointing to the data directory
        """
        data_dir = Path(self.DATA_DIRECTORY)
        
        # If it's already an absolute path, return it
        if data_dir.is_absolute():
            return data_dir
        
        # Otherwise, resolve relative to /app (container) or project root (local)
        # In Docker: /app/src/config.py -> /app/data
        # Locally: fastapi/config.py -> ../data
        current_file = Path(__file__)
        
        # Check if we're in Docker (/app/src/) or locally (fastapi/)
        if str(current_file).startswith('/app'):
            # Docker: /app/src/config.py -> /app/data
            return Path('/app') / data_dir
        else:
            # Local: fastapi/config.py -> ../data
            project_root = current_file.parent.parent
            return project_root / data_dir


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()


settings = get_settings()

