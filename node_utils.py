#!/usr/bin/env python3
"""Kubernetes node utilities."""

import time
import asyncio
from typing import List, Dict, Any, Optional, Set
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
from kubernetes import client, config as k8s_config
from kubernetes.client.rest import ApiException
from config import Config
from logger_utils import NodeManagerLogger, log_operation


@dataclass
class DrainResult:
    """Result of a node drain operation."""
    success: bool
    node_name: str
    duration_seconds: float
    pods_drained: int
    error: Optional[str] = None
    timed_out: bool = False


class KubernetesNodeManager:
    """Manager for Kubernetes node operations."""
    
    def __init__(self, config: Config, logger: NodeManagerLogger):
        self.config = config
        self.logger = logger
        self.v1 = None
        self._initialize_k8s_client()
    
    def _initialize_k8s_client(self) -> None:
        """Initialize Kubernetes client."""
        try:
            k8s_config.load_incluster_config()
            self.logger.info("Loaded in-cluster Kubernetes config")
        except k8s_config.ConfigException:
            try:
                k8s_config.load_kube_config()
                self.logger.info("Loaded local Kubernetes config")
            except k8s_config.ConfigException as e:
                self.logger.error("Could not load Kubernetes configuration", error=str(e))
                raise Exception("Could not load Kubernetes configuration") from e
        
        self.v1 = client.CoreV1Api()
        self.logger.info("Kubernetes client initialized successfully")
    
    @log_operation("check_cast_managed_node")
    def is_cast_managed_node(self, node_name: str) -> bool:
        """
        Check if a node is managed by CAST AI.
        
        Args:
            node_name (str): Name of the Kubernetes node.
        
        Returns:
            bool: True if node is managed by CAST AI, False otherwise.
        """
        try:
            node = self.v1.read_node(name=node_name)
            labels = node.metadata.labels or {}
            is_managed = labels.get('provisioner.cast.ai/managed-by') == 'cast.ai'
            
            self.logger.debug(
                "Node CAST AI management check",
                node_name=node_name,
                is_cast_managed=is_managed,
                labels=dict(labels)
            )
            
            return is_managed
            
        except ApiException as e:
            self.logger.warning("Could not check node CAST AI management", node_name=node_name, error=str(e))
            return False
    
    @log_operation("get_node_pods")
    def get_node_pods(self, node_name: str) -> List[client.V1Pod]:
        """
        Get all pods running on a specific node.
        
        Args:
            node_name (str): Name of the Kubernetes node.
        
        Returns:
            List[client.V1Pod]: List of pods on the node.
        """
        try:
            field_selector = f"spec.nodeName={node_name}"
            pods_response = self.v1.list_pod_for_all_namespaces(field_selector=field_selector)
            pods = pods_response.items
            
            # Filter out pods that don't need draining
            drainable_pods = []
            for pod in pods:
                if self._should_drain_pod(pod):
                    drainable_pods.append(pod)
            
            self.logger.info(
                "Retrieved node pods",
                node_name=node_name,
                total_pods=len(pods),
                drainable_pods=len(drainable_pods)
            )
            
            return drainable_pods
            
        except ApiException as e:
            self.logger.error("Failed to get node pods", node_name=node_name, error=str(e))
            raise
    
    def _should_drain_pod(self, pod: client.V1Pod) -> bool:
        """
        Determine if a pod should be drained.
        
        Args:
            pod (client.V1Pod): The pod to check.
        
        Returns:
            bool: True if pod should be drained, False otherwise.
        """
        # Skip completed pods
        if pod.status.phase in ['Succeeded', 'Failed']:
            return False
        
        # Skip DaemonSet pods
        if pod.metadata.owner_references:
            for owner in pod.metadata.owner_references:
                if owner.kind == 'DaemonSet':
                    return False
        
        # Skip mirror pods (static pods)
        if pod.metadata.annotations and 'kubernetes.io/config.mirror' in pod.metadata.annotations:
            return False
        
        return True
    
    @log_operation("cordon_node")
    def cordon_node(self, node_name: str) -> bool:
        """
        Cordon a node (mark as unschedulable).
        
        Args:
            node_name (str): Name of the Kubernetes node.
        
        Returns:
            bool: True if node was cordoned successfully, False otherwise.
        """
        try:
            body = {"spec": {"unschedulable": True}}
            self.v1.patch_node(name=node_name, body=body)
            
            self.logger.info("Node cordoned successfully", node_name=node_name)
            return True
            
        except ApiException as e:
            self.logger.error("Failed to cordon node", node_name=node_name, error=str(e))
            return False
    
    @log_operation("delete_pod")
    def delete_pod(self, pod: client.V1Pod, grace_period_seconds: int = 30) -> bool:
        """
        Delete a pod.
        
        Args:
            pod (client.V1Pod): The pod to delete.
            grace_period_seconds (int): Grace period for pod deletion.
        
        Returns:
            bool: True if pod was deleted successfully, False otherwise.
        """
        try:
            self.v1.delete_namespaced_pod(
                name=pod.metadata.name,
                namespace=pod.metadata.namespace,
                grace_period_seconds=grace_period_seconds
            )
            
            self.logger.debug(
                "Pod deleted",
                pod_name=pod.metadata.name,
                namespace=pod.metadata.namespace,
                grace_period=grace_period_seconds
            )
            return True
            
        except ApiException as e:
            self.logger.warning(
                "Failed to delete pod",
                pod_name=pod.metadata.name,
                namespace=pod.metadata.namespace,
                error=str(e)
            )
            return False
    
    @log_operation("wait_for_pods_termination")
    def wait_for_pods_termination(self, node_name: str, timeout_seconds: int) -> bool:
        """
        Wait for all pods on a node to terminate.
        
        Args:
            node_name (str): Name of the Kubernetes node.
            timeout_seconds (int): Maximum time to wait.
        
        Returns:
            bool: True if all pods terminated within timeout, False otherwise.
        """
        start_time = time.time()
        
        while time.time() - start_time < timeout_seconds:
            pods = self.get_node_pods(node_name)
            if not pods:
                self.logger.info("All pods terminated", node_name=node_name)
                return True
            
            remaining_pods = [f"{pod.metadata.namespace}/{pod.metadata.name}" for pod in pods]
            self.logger.debug(
                "Waiting for pods to terminate",
                node_name=node_name,
                remaining_pods_count=len(pods),
                remaining_pods=remaining_pods[:5],  # Log first 5 pods
                elapsed_seconds=round(time.time() - start_time, 1)
            )
            
            time.sleep(10)  # Check every 10 seconds
        
        # Timeout reached
        remaining_pods = self.get_node_pods(node_name)
        self.logger.warning(
            "Timeout waiting for pods to terminate",
            node_name=node_name,
            remaining_pods_count=len(remaining_pods),
            timeout_seconds=timeout_seconds
        )
        return False
    
    @log_operation("drain_node")
    def drain_node(self, node_name: str, timeout_minutes: Optional[int] = None) -> DrainResult:
        """
        Drain a node safely with timeout support.
        
        Args:
            node_name (str): Name of the Kubernetes node.
            timeout_minutes (int, optional): Timeout in minutes. Uses config default if None.
        
        Returns:
            DrainResult: Result of the drain operation.
        """
        if timeout_minutes is None:
            timeout_minutes = self.config.drain_timeout_minutes
        
        timeout_seconds = timeout_minutes * 60
        start_time = time.time()
        
        self.logger.log_node_action(
            "drain_start",
            node_name,
            timeout_minutes=timeout_minutes
        )
        
        try:
            # Step 1: Cordon the node
            if not self.cordon_node(node_name):
                return DrainResult(
                    success=False,
                    node_name=node_name,
                    duration_seconds=time.time() - start_time,
                    pods_drained=0,
                    error="Failed to cordon node"
                )
            
            # Step 2: Get pods to drain
            pods = self.get_node_pods(node_name)
            initial_pod_count = len(pods)
            
            if not pods:
                self.logger.log_node_action("drain_complete_no_pods", node_name)
                return DrainResult(
                    success=True,
                    node_name=node_name,
                    duration_seconds=time.time() - start_time,
                    pods_drained=0
                )
            
            # Step 3: Delete pods gracefully
            self.logger.info(
                "Starting graceful pod deletion",
                node_name=node_name,
                pod_count=initial_pod_count
            )
            
            for pod in pods:
                self.delete_pod(pod, grace_period_seconds=30)
            
            # Step 4: Wait for pods to terminate
            pods_terminated = self.wait_for_pods_termination(node_name, timeout_seconds)
            
            if not pods_terminated:
                # Force delete remaining pods
                self.logger.warning("Timeout reached, force deleting remaining pods", node_name=node_name)
                remaining_pods = self.get_node_pods(node_name)
                
                for pod in remaining_pods:
                    self.delete_pod(pod, grace_period_seconds=0)
                
                # Wait a bit more for force deletion
                self.wait_for_pods_termination(node_name, 60)
                
                return DrainResult(
                    success=True,
                    node_name=node_name,
                    duration_seconds=time.time() - start_time,
                    pods_drained=initial_pod_count,
                    timed_out=True
                )
            
            duration = time.time() - start_time
            self.logger.log_node_action(
                "drain_complete",
                node_name,
                pods_drained=initial_pod_count,
                duration_seconds=round(duration, 2)
            )
            
            return DrainResult(
                success=True,
                node_name=node_name,
                duration_seconds=duration,
                pods_drained=initial_pod_count
            )
            
        except Exception as e:
            duration = time.time() - start_time
            self.logger.log_node_action("drain_failed", node_name, error=str(e))
            
            return DrainResult(
                success=False,
                node_name=node_name,
                duration_seconds=duration,
                pods_drained=0,
                error=str(e)
            )
    
    def drain_nodes_parallel(self, node_names: List[str]) -> List[DrainResult]:
        """
        Drain multiple nodes in parallel.
        
        Args:
            node_names (List[str]): List of node names to drain.
        
        Returns:
            List[DrainResult]: Results of drain operations.
        """
        max_workers = min(self.config.max_parallel_drains, len(node_names))
        
        self.logger.info(
            "Starting parallel node draining",
            node_count=len(node_names),
            max_workers=max_workers,
            timeout_minutes=self.config.drain_timeout_minutes
        )
        
        results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all drain tasks
            future_to_node = {
                executor.submit(self.drain_node, node_name): node_name
                for node_name in node_names
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_node):
                node_name = future_to_node[future]
                try:
                    result = future.result()
                    results.append(result)
                    
                    if result.success:
                        self.logger.info(
                            "Node drain completed",
                            node_name=node_name,
                            duration_seconds=round(result.duration_seconds, 2),
                            pods_drained=result.pods_drained,
                            timed_out=result.timed_out
                        )
                    else:
                        self.logger.error(
                            "Node drain failed",
                            node_name=node_name,
                            error=result.error
                        )
                        
                except Exception as e:
                    self.logger.error("Unexpected error during node drain", node_name=node_name, error=str(e))
                    results.append(DrainResult(
                        success=False,
                        node_name=node_name,
                        duration_seconds=0,
                        pods_drained=0,
                        error=str(e)
                    ))
        
        success_count = sum(1 for r in results if r.success)
        self.logger.info(
            "Parallel node draining completed",
            total_nodes=len(node_names),
            successful=success_count,
            failed=len(node_names) - success_count
        )
        
        return results