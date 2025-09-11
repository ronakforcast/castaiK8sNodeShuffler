#!/usr/bin/env python3
"""CAST AI rebalancing utilities."""

import time
import requests
from typing import List, Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class RebalancingBatchResult:
    """Result of a rebalancing batch operation."""
    batch_number: int
    nodes_count: int
    plan_id: Optional[str]
    success: bool
    duration_seconds: float
    status: str
    error: Optional[str] = None


class CastAIRebalancer:
    """CAST AI rebalancing client for batch operations."""
    
    def __init__(self, cluster_id: str, api_key: str, logger, zone: Optional[str] = None, min_nodes: int = 3):
        """
        Initialize CAST AI rebalancer.
        
        Args:
            cluster_id (str): CAST AI cluster ID.
            api_key (str): CAST AI API key.
            logger: Logger instance.
            zone (str, optional): Specific zone to filter nodes.
            min_nodes (int): Minimum nodes to maintain during rebalancing.
        """
        self.cluster_id = cluster_id
        self.api_key = api_key
        self.logger = logger
        self.zone = zone
        self.min_nodes = min_nodes
        self.base_url = "https://api.cast.ai/v1"
        self.session = requests.Session()
        self.session.headers.update({
            "X-API-Key": self.api_key,
            "accept": "application/json",
            "Content-Type": "application/json"
        })
    
    def get_all_node_ids(self) -> List[str]:
        """
        Get all node IDs from CAST AI.
        
        Returns:
            List[str]: List of node IDs.
        """
        try:
            if self.zone:
                url = f"{self.base_url}/kubernetes/external-clusters/{self.cluster_id}/nodes?zone={self.zone}"
            else:
                url = f"{self.base_url}/kubernetes/external-clusters/{self.cluster_id}/nodes"
            
            self.logger.info("Fetching nodes from CAST AI", url=url, zone=self.zone)
            
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            node_ids = []
            
            # Extract node IDs from response
            if "items" in data:
                for item in data["items"]:
                    if item and "id" in item and item["id"]:
                        node_ids.append(item["id"])
            
            self.logger.info(f"Retrieved {len(node_ids)} nodes from CAST AI")
            return node_ids
            
        except requests.exceptions.RequestException as e:
            self.logger.error("Failed to fetch nodes from CAST AI", error=str(e))
            return []
        except Exception as e:
            self.logger.error("Unexpected error fetching nodes", error=str(e))
            return []
    
    def create_rebalancing_plan(self, node_ids: List[str]) -> Optional[str]:
        """
        Create a rebalancing plan for given nodes.
        
        Args:
            node_ids (List[str]): List of node IDs to rebalance.
            
        Returns:
            Optional[str]: Plan ID if successful, None otherwise.
        """
        try:
            # Build the rebalancing nodes JSON structure
            rebalancing_nodes = [{"nodeId": node_id} for node_id in node_ids]
            
            payload = {
                "minNodes": self.min_nodes,
                "rebalancingNodes": rebalancing_nodes
            }
            
            url = f"{self.base_url}/kubernetes/clusters/{self.cluster_id}/rebalancing-plans"
            
            self.logger.info(
                "Creating rebalancing plan",
                nodes_count=len(node_ids),
                min_nodes=self.min_nodes
            )
            
            response = self.session.post(url, json=payload, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            plan_id = data.get("rebalancingPlanId")
            
            if plan_id:
                self.logger.info("Rebalancing plan created successfully", plan_id=plan_id)
                return plan_id
            else:
                self.logger.error("No plan ID in response", response_data=data)
                return None
                
        except requests.exceptions.RequestException as e:
            self.logger.error("Failed to create rebalancing plan", error=str(e))
            return None
        except Exception as e:
            self.logger.error("Unexpected error creating rebalancing plan", error=str(e))
            return None
    
    def execute_rebalancing_plan(self, plan_id: str) -> bool:
        """
        Execute a rebalancing plan.
        
        Args:
            plan_id (str): Plan ID to execute.
            
        Returns:
            bool: True if execution started successfully, False otherwise.
        """
        try:
            url = f"{self.base_url}/kubernetes/clusters/{self.cluster_id}/rebalancing-plans/{plan_id}/execute"
            
            self.logger.info("Executing rebalancing plan", plan_id=plan_id)
            
            # Small delay before execution as per shell script
            time.sleep(10)
            
            response = self.session.post(url, timeout=30)
            response.raise_for_status()
            
            self.logger.info("Rebalancing plan execution started", plan_id=plan_id)
            return True
            
        except requests.exceptions.RequestException as e:
            self.logger.error("Failed to execute rebalancing plan", plan_id=plan_id, error=str(e))
            return False
        except Exception as e:
            self.logger.error("Unexpected error executing rebalancing plan", plan_id=plan_id, error=str(e))
            return False
    
    def wait_for_rebalancing_completion(self, plan_id: str, timeout_minutes: int = 40) -> bool:
        """
        Wait for rebalancing plan to complete.
        
        Args:
            plan_id (str): Plan ID to monitor.
            timeout_minutes (int): Maximum time to wait in minutes.
            
        Returns:
            bool: True if completed successfully, False if failed or timed out.
        """
        try:
            url = f"{self.base_url}/kubernetes/clusters/{self.cluster_id}/rebalancing-plans/{plan_id}"
            timeout_seconds = timeout_minutes * 60
            elapsed = 0
            check_interval = 30  # Check every 30 seconds
            
            self.logger.info(
                "Waiting for rebalancing completion",
                plan_id=plan_id,
                timeout_minutes=timeout_minutes
            )
            
            while elapsed < timeout_seconds:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                status = data.get("status", "unknown")
                
                if status == "finished":
                    self.logger.info("Rebalancing completed successfully", plan_id=plan_id)
                    return True
                elif status == "failed":
                    self.logger.error("Rebalancing failed", plan_id=plan_id, status=status)
                    return False
                else:
                    self.logger.info(
                        "Rebalancing in progress",
                        plan_id=plan_id,
                        status=status,
                        elapsed_seconds=elapsed
                    )
                
                time.sleep(check_interval)
                elapsed += check_interval
            
            self.logger.error("Rebalancing timed out", plan_id=plan_id, timeout_minutes=timeout_minutes)
            return False
            
        except requests.exceptions.RequestException as e:
            self.logger.error("Error monitoring rebalancing plan", plan_id=plan_id, error=str(e))
            return False
        except Exception as e:
            self.logger.error("Unexpected error monitoring rebalancing", plan_id=plan_id, error=str(e))
            return False
    
    def execute_batch_rebalancing(
        self,
        node_ids: List[str],
        batch_size: int,
        batch_wait_seconds: int = 30
    ) -> List[RebalancingBatchResult]:
        """
        Execute rebalancing in batches.
        
        Args:
            node_ids (List[str]): All node IDs to rebalance.
            batch_size (int): Number of nodes per batch.
            batch_wait_seconds (int): Wait time between batches.
            
        Returns:
            List[RebalancingBatchResult]: Results of each batch.
        """
        if not node_ids:
            self.logger.info("No nodes to rebalance")
            return []
        
        total_nodes = len(node_ids)
        total_batches = (total_nodes + batch_size - 1) // batch_size
        batch_results = []
        
        self.logger.info(
            "Starting batch rebalancing",
            total_nodes=total_nodes,
            batch_size=batch_size,
            total_batches=total_batches
        )
        
        for batch_num in range(1, total_batches + 1):
            start_idx = (batch_num - 1) * batch_size
            end_idx = min(start_idx + batch_size, total_nodes)
            batch_nodes = node_ids[start_idx:end_idx]
            
            batch_result = self._execute_single_rebalancing_batch(
                batch_nodes,
                batch_num,
                total_batches
            )
            batch_results.append(batch_result)
            
            # Wait between batches (except for the last batch)
            if batch_num < total_batches and batch_result.success:
                self.logger.info(
                    "Waiting between batches",
                    wait_seconds=batch_wait_seconds,
                    next_batch=batch_num + 1
                )
                time.sleep(batch_wait_seconds)
        
        # Log overall summary
        successful_batches = sum(1 for r in batch_results if r.success)
        total_duration = sum(r.duration_seconds for r in batch_results)
        
        self.logger.info(
            "Batch rebalancing completed",
            successful_batches=successful_batches,
            failed_batches=total_batches - successful_batches,
            total_duration_minutes=round(total_duration / 60, 2),
            total_batches=total_batches
        )
        
        return batch_results
    
    def _execute_single_rebalancing_batch(
        self,
        node_ids: List[str],
        batch_num: int,
        total_batches: int
    ) -> RebalancingBatchResult:
        """
        Execute rebalancing for a single batch.
        
        Args:
            node_ids (List[str]): Node IDs in this batch.
            batch_num (int): Current batch number.
            total_batches (int): Total number of batches.
            
        Returns:
            RebalancingBatchResult: Result of the batch operation.
        """
        start_time = time.time()
        
        self.logger.info(
            "Processing rebalancing batch",
            batch_number=batch_num,
            total_batches=total_batches,
            nodes_in_batch=len(node_ids)
        )
        
        # Create rebalancing plan
        plan_id = self.create_rebalancing_plan(node_ids)
        
        if not plan_id:
            return RebalancingBatchResult(
                batch_number=batch_num,
                nodes_count=len(node_ids),
                plan_id=None,
                success=False,
                duration_seconds=time.time() - start_time,
                status="plan_creation_failed",
                error="Failed to create rebalancing plan"
            )
        
        # Execute the plan
        if not self.execute_rebalancing_plan(plan_id):
            return RebalancingBatchResult(
                batch_number=batch_num,
                nodes_count=len(node_ids),
                plan_id=plan_id,
                success=False,
                duration_seconds=time.time() - start_time,
                status="execution_failed",
                error="Failed to execute rebalancing plan"
            )
        
        # Wait for completion
        success = self.wait_for_rebalancing_completion(plan_id, timeout_minutes=40)
        duration = time.time() - start_time
        
        result = RebalancingBatchResult(
            batch_number=batch_num,
            nodes_count=len(node_ids),
            plan_id=plan_id,
            success=success,
            duration_seconds=duration,
            status="completed" if success else "failed_or_timeout"
        )
        
        if success:
            self.logger.info(
                "Batch rebalancing completed successfully",
                batch_number=batch_num,
                plan_id=plan_id,
                duration_minutes=round(duration / 60, 2)
            )
        else:
            self.logger.error(
                "Batch rebalancing failed",
                batch_number=batch_num,
                plan_id=plan_id,
                duration_minutes=round(duration / 60, 2)
            )
        
        return result
    
    def close(self):
        """Close the session."""
        if self.session:
            self.session.close()