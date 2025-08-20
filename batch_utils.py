#!/usr/bin/env python3
"""Batch processing utilities for node management."""

import time
from typing import List, TypeVar, Iterator, Dict, Any
from dataclasses import dataclass
from cast_utils import CastNode
from node_utils import DrainResult
from config import Config
from logger_utils import NodeManagerLogger

T = TypeVar('T')


@dataclass
class BatchResult:
    """Result of a batch processing operation."""
    batch_number: int
    total_batches: int
    items_processed: int
    successful_items: int
    failed_items: int
    duration_seconds: float
    errors: List[str]


class BatchProcessor:
    """Utility class for processing items in batches."""
    
    def __init__(self, config: Config, logger: NodeManagerLogger):
        self.config = config
        self.logger = logger
    
    def create_batches(self, items: List[T], batch_size: int = None) -> Iterator[List[T]]:
        """
        Split items into batches.
        
        Args:
            items (List[T]): Items to batch.
            batch_size (int, optional): Size of each batch. Uses config default if None.
        
        Yields:
            List[T]: Batch of items.
        """
        if batch_size is None:
            batch_size = self.config.batch_size
        
        for i in range(0, len(items), batch_size):
            yield items[i:i + batch_size]
    
    def calculate_batch_info(self, items: List[T], batch_size: int = None) -> Dict[str, int]:
        """
        Calculate batch processing information.
        
        Args:
            items (List[T]): Items to process.
            batch_size (int, optional): Size of each batch.
        
        Returns:
            Dict[str, int]: Batch information including total batches, etc.
        """
        if batch_size is None:
            batch_size = self.config.batch_size
        
        total_items = len(items)
        total_batches = (total_items + batch_size - 1) // batch_size  # Ceiling division
        
        return {
            'total_items': total_items,
            'batch_size': batch_size,
            'total_batches': total_batches,
            'estimated_duration_minutes': total_batches * (self.config.batch_wait_seconds / 60)
        }


class NodeBatchProcessor(BatchProcessor):
    """Specialized batch processor for node operations."""
    
    def __init__(self, config: Config, logger: NodeManagerLogger, k8s_manager, cast_client):
        super().__init__(config, logger)
        self.k8s_manager = k8s_manager
        self.cast_client = cast_client
    
    def process_node_batches(self, nodes: List[CastNode]) -> List[BatchResult]:
        """
        Process nodes in batches with draining and deletion.
        
        Args:
            nodes (List[CastNode]): Nodes to process.
        
        Returns:
            List[BatchResult]: Results of each batch processing.
        """
        if not nodes:
            self.logger.info("No nodes to process")
            return []
        
        batch_info = self.calculate_batch_info(nodes)
        self.logger.info(
            "Starting batch processing of nodes",
            **batch_info
        )
        
        batch_results = []
        batches = list(self.create_batches(nodes))
        
        for batch_num, batch_nodes in enumerate(batches, 1):
            batch_result = self._process_single_batch(
                batch_nodes,
                batch_num,
                len(batches)
            )
            batch_results.append(batch_result)
            
            # Wait between batches (except for the last batch)
            if batch_num < len(batches):
                self.logger.info(
                    "Waiting between batches",
                    wait_seconds=self.config.batch_wait_seconds,
                    next_batch=batch_num + 1
                )
                time.sleep(self.config.batch_wait_seconds)
        
        # Log overall summary
        total_processed = sum(r.items_processed for r in batch_results)
        total_successful = sum(r.successful_items for r in batch_results)
        total_duration = sum(r.duration_seconds for r in batch_results)
        
        self.logger.info(
            "Batch processing completed",
            total_nodes_processed=total_processed,
            successful_nodes=total_successful,
            failed_nodes=total_processed - total_successful,
            total_duration_minutes=round(total_duration / 60, 2),
            total_batches=len(batch_results)
        )
        
        return batch_results
    
    def _process_single_batch(
        self,
        batch_nodes: List[CastNode],
        batch_num: int,
        total_batches: int
    ) -> BatchResult:
        """
        Process a single batch of nodes.
        
        Args:
            batch_nodes (List[CastNode]): Nodes in this batch.
            batch_num (int): Current batch number.
            total_batches (int): Total number of batches.
        
        Returns:
            BatchResult: Result of processing this batch.
        """
        start_time = time.time()
        errors = []
        successful_drains = 0
        successful_deletions = 0
        
        self.logger.log_batch_progress(batch_num, total_batches, len(batch_nodes))
        
        # Step 1: Filter nodes that are actually managed by CAST AI
        verified_nodes = []
        for node in batch_nodes:
            if self.k8s_manager.is_cast_managed_node(node.k8s_name):
                verified_nodes.append(node)
            else:
                self.logger.warning(
                    "Node not managed by CAST AI, skipping",
                    node_name=node.k8s_name,
                    cast_id=node.cast_id
                )
                errors.append(f"Node {node.k8s_name} not managed by CAST AI")
        
        if not verified_nodes:
            return BatchResult(
                batch_number=batch_num,
                total_batches=total_batches,
                items_processed=len(batch_nodes),
                successful_items=0,
                failed_items=len(batch_nodes),
                duration_seconds=time.time() - start_time,
                errors=errors
            )
        
        # Step 2: Drain nodes in parallel
        node_names = [node.k8s_name for node in verified_nodes]
        drain_results = self.k8s_manager.drain_nodes_parallel(node_names)
        
        # Create mapping of drain results
        drain_result_map = {result.node_name: result for result in drain_results}
        
        # Step 3: Delete successfully drained nodes from CAST AI
        for node in verified_nodes:
            drain_result = drain_result_map.get(node.k8s_name)
            
            if drain_result and drain_result.success:
                successful_drains += 1
                
                try:
                    self.cast_client.delete_cast_node(node.cast_id)
                    successful_deletions += 1
                    
                    self.logger.info(
                        "Node processing completed successfully",
                        node_name=node.k8s_name,
                        cast_id=node.cast_id,
                        drain_duration=round(drain_result.duration_seconds, 2),
                        pods_drained=drain_result.pods_drained,
                        timed_out=drain_result.timed_out
                    )
                    
                except Exception as e:
                    error_msg = f"Failed to delete node {node.k8s_name} from CAST AI: {str(e)}"
                    errors.append(error_msg)
                    self.logger.error(
                        "Failed to delete node from CAST AI",
                        node_name=node.k8s_name,
                        cast_id=node.cast_id,
                        error=str(e)
                    )
            else:
                error_msg = f"Failed to drain node {node.k8s_name}"
                if drain_result and drain_result.error:
                    error_msg += f": {drain_result.error}"
                errors.append(error_msg)
                
                self.logger.error(
                    "Skipping CAST AI deletion due to drain failure",
                    node_name=node.k8s_name,
                    cast_id=node.cast_id
                )
        
        duration = time.time() - start_time
        
        batch_result = BatchResult(
            batch_number=batch_num,
            total_batches=total_batches,
            items_processed=len(batch_nodes),
            successful_items=successful_deletions,
            failed_items=len(batch_nodes) - successful_deletions,
            duration_seconds=duration,
            errors=errors
        )
        
        self.logger.info(
            "Batch processing completed",
            batch_number=batch_num,
            nodes_processed=len(batch_nodes),
            successful_drains=successful_drains,
            successful_deletions=successful_deletions,
            failed_operations=len(batch_nodes) - successful_deletions,
            duration_minutes=round(duration / 60, 2)
        )
        
        return batch_result
    
    def validate_batch_configuration(self) -> bool:
        """
        Validate batch processing configuration.
        
        Returns:
            bool: True if configuration is valid, False otherwise.
        """
        issues = []
        
        if self.config.batch_size <= 0:
            issues.append("batch_size must be positive")
        
        if self.config.max_parallel_drains <= 0:
            issues.append("max_parallel_drains must be positive")
        
        if self.config.max_parallel_drains > self.config.batch_size:
            self.logger.warning(
                "max_parallel_drains is greater than batch_size",
                max_parallel_drains=self.config.max_parallel_drains,
                batch_size=self.config.batch_size
            )
        
        if self.config.drain_timeout_minutes <= 0:
            issues.append("drain_timeout_minutes must be positive")
        
        if issues:
            for issue in issues:
                self.logger.error("Configuration validation failed", issue=issue)
            return False
        
        self.logger.info(
            "Batch configuration validated successfully",
            batch_size=self.config.batch_size,
            max_parallel_drains=self.config.max_parallel_drains,
            drain_timeout_minutes=self.config.drain_timeout_minutes,
            batch_wait_seconds=self.config.batch_wait_seconds
        )
        
        return True