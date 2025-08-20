#!/usr/bin/env python3
"""Logging utilities for CAST AI Node Manager."""

import logging
import sys
from functools import wraps
from typing import Any, Callable, Optional
from config import Config


class NodeManagerLogger:
    """Centralized logger for node manager operations."""
    
    def __init__(self, config: Config, name: Optional[str] = None):
        self.config = config
        self.logger = logging.getLogger(name or __name__)
        self._setup_logging()
    
    def _setup_logging(self) -> None:
        """Setup logging configuration."""
        # Remove existing handlers
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)
        
        # Create console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, self.config.log_level))
        
        # Create formatter
        formatter = logging.Formatter(self.config.log_format)
        console_handler.setFormatter(formatter)
        
        # Add handler to logger
        self.logger.addHandler(console_handler)
        self.logger.setLevel(getattr(logging, self.config.log_level))
        
        # Prevent propagation to root logger
        self.logger.propagate = False
    
    def info(self, message: str, **kwargs) -> None:
        """Log info message with optional context."""
        self._log_with_context(self.logger.info, message, **kwargs)
    
    def warning(self, message: str, **kwargs) -> None:
        """Log warning message with optional context."""
        self._log_with_context(self.logger.warning, message, **kwargs)
    
    def error(self, message: str, **kwargs) -> None:
        """Log error message with optional context."""
        self._log_with_context(self.logger.error, message, **kwargs)
    
    def debug(self, message: str, **kwargs) -> None:
        """Log debug message with optional context."""
        self._log_with_context(self.logger.debug, message, **kwargs)
    
    def _log_with_context(self, log_func: Callable, message: str, **kwargs) -> None:
        """Log message with additional context."""
        if kwargs:
            context = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
            message = f"{message} | {context}"
        log_func(message)
    
    def log_operation_start(self, operation: str, **context) -> None:
        """Log the start of an operation."""
        self.info(f"Starting operation: {operation}", **context)
    
    def log_operation_success(self, operation: str, duration: Optional[float] = None, **context) -> None:
        """Log successful completion of an operation."""
        msg = f"Operation completed successfully: {operation}"
        if duration is not None:
            context['duration_seconds'] = round(duration, 2)
        self.info(msg, **context)
    
    def log_operation_failure(self, operation: str, error: Exception, **context) -> None:
        """Log failure of an operation."""
        context['error'] = str(error)
        context['error_type'] = type(error).__name__
        self.error(f"Operation failed: {operation}", **context)
    
    def log_batch_progress(self, batch_num: int, total_batches: int, batch_size: int) -> None:
        """Log batch processing progress."""
        self.info(
            f"Processing batch {batch_num}/{total_batches}",
            batch_size=batch_size,
            progress_pct=round((batch_num / total_batches) * 100, 1)
        )
    
    def log_node_action(self, action: str, node_name: str, **context) -> None:
        """Log node-specific actions."""
        self.info(f"Node action: {action}", node_name=node_name, **context)


def log_operation(operation_name: str):
    """Decorator to log operation start, success, and failure."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(self, *args, **kwargs) -> Any:
            logger = getattr(self, 'logger', None)
            if not logger:
                return func(self, *args, **kwargs)
            
            import time
            start_time = time.time()
            
            try:
                logger.log_operation_start(operation_name)
                result = func(self, *args, **kwargs)
                duration = time.time() - start_time
                logger.log_operation_success(operation_name, duration)
                return result
            except Exception as e:
                logger.log_operation_failure(operation_name, e)
                raise
        
        return wrapper
    return decorator


def retry_with_logging(max_retries: int, delay_seconds: int = 1):
    """Decorator to retry operations with logging."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(self, *args, **kwargs) -> Any:
            logger = getattr(self, 'logger', None)
            
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    if attempt > 0 and logger:
                        logger.info(f"Retry attempt {attempt}/{max_retries} for {func.__name__}")
                    
                    return func(self, *args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        if logger:
                            logger.warning(
                                f"Attempt {attempt + 1} failed for {func.__name__}: {str(e)}"
                            )
                        import time
                        time.sleep(delay_seconds)
                    else:
                        if logger:
                            logger.error(
                                f"All {max_retries + 1} attempts failed for {func.__name__}"
                            )
            
            raise last_exception
        
        return wrapper
    return decorator