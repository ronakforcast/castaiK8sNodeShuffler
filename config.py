#!/usr/bin/env python3
"""Configuration management for CAST AI Node Manager."""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class Config:
    """Configuration class for CAST AI Node Manager."""
    
    # CAST AI Configuration
    cast_ai_api_key: str
    cast_ai_cluster_id: str
    cast_api_base_url: str = "https://api.cast.ai/v1"
    
    # Processing Configuration
    batch_size: int = 10
    drain_timeout_minutes: int = 10
    batch_wait_seconds: int = 180
    max_parallel_drains: int = 5
    
    # Retry Configuration
    max_retries: int = 3
    retry_delay_seconds: int = 30
    
    # Logging Configuration
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    @property
    def k8s_patches(self) -> str:
        return os.getenv('K8S_PATCHES', '')
    
    @classmethod
    def from_environment(cls) -> "Config":
        """Create configuration from environment variables."""
        cast_ai_api_key = os.getenv('CAST_AI_API_KEY')
        cast_ai_cluster_id = os.getenv('CAST_AI_CLUSTER_ID')
        
        if not cast_ai_api_key:
            raise ValueError("CAST_AI_API_KEY environment variable is required")
        if not cast_ai_cluster_id:
            raise ValueError("CAST_AI_CLUSTER_ID environment variable is required")
        
        return cls(
            cast_ai_api_key=cast_ai_api_key,
            cast_ai_cluster_id=cast_ai_cluster_id,
            batch_size=int(os.getenv('BATCH_SIZE', '10')),
            drain_timeout_minutes=int(os.getenv('DRAIN_TIMEOUT_MINUTES', '10')),
            batch_wait_seconds=int(os.getenv('BATCH_WAIT_SECONDS', '180')),
            max_parallel_drains=int(os.getenv('MAX_PARALLEL_DRAINS', '5')),
            max_retries=int(os.getenv('MAX_RETRIES', '3')),
            retry_delay_seconds=int(os.getenv('RETRY_DELAY_SECONDS', '30')),
            log_level=os.getenv('LOG_LEVEL', 'INFO'),
        )
    
    def validate(self) -> None:
        """Validate configuration values."""
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")
        if self.drain_timeout_minutes <= 0:
            raise ValueError("drain_timeout_minutes must be positive")
        if self.max_parallel_drains <= 0:
            raise ValueError("max_parallel_drains must be positive")
        if self.max_retries < 0:
            raise ValueError("max_retries must be non-negative")
        if self.retry_delay_seconds < 0:
            raise ValueError("retry_delay_seconds must be non-negative")