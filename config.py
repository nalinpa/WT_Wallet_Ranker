import os
from typing import Optional

class Config:
    # Environment
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "development")
    
    # Google Cloud
    PROJECT_ID: Optional[str] = os.getenv("PROJECT_ID")
    DATASET_ID: str = os.getenv("DATASET_ID", "crypto_analysis")
    
    # Server
    PORT: int = int(os.getenv("PORT", "8080"))
    HOST: str = os.getenv("HOST", "0.0.0.0")
    
    # API Configuration
    MAX_CONCURRENT_REQUESTS: int = int(os.getenv("MAX_CONCURRENT_REQUESTS", "10"))
    REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT", "15"))
    
    # Rate Limiting
    PRICE_FETCHER_DELAY: float = float(os.getenv("PRICE_FETCHER_DELAY", "1.0"))
    
    @property
    def is_production(self) -> bool:
        return self.ENVIRONMENT == "production"
    
    @property
    def is_development(self) -> bool:
        return self.ENVIRONMENT == "development"

config = Config()