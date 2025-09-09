#!/usr/bin/env python3
"""Kubernetes node utilities."""

import time
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
        self.policy_v1 = None
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
        self.policy_v1 = client.PolicyV1Api()
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
    def get_node_pods(self, node_name: str, include_daemonsets: bool = False) -> List[client.V1Pod]:
        """
        Get all pods running on a specific node.
        
        Args:
            node_name (str): Name of the Kubernetes node.
            include_daemonsets (bool): Whether to include DaemonSet pods.
        
        Returns:
            List[client.V1Pod]: List of pods on the node.
        """
        try:
            field_selector = f"spec.nodeName={node_name}"
            pods_response = self.v1.list_pod_for_all_namespaces(field_selector=field_selector)
            pods = pods_response.items
            
            # Filter pods based on drainability
            filtered_pods = []
            for pod in pods:
                if self._should_include_pod_for_drain(pod, include_daemonsets):
                    filtered_pods.append(pod)
            
            self.logger.info(
                "Retrieved node pods",
                node_name=node_name,
                total_pods=len(pods),
                drainable_pods=len(filtered_pods),
                include_daemonsets=include_daemonsets
            )
            
            return filtered_pods
            
        except ApiException as e:
            self.logger.error("Failed to get node pods", node_name=node_name, error=str(e))
            raise
    
    def _should_include_pod_for_drain(self, pod: client.V1Pod, include_daemonsets: bool = False) -> bool:
        """
        Determine if a pod should be included in drain operations.
        
        Args:
            pod (client.V1Pod): The pod to check.
            include_daemonsets (bool): Whether to include DaemonSet pods.
        
        Returns:
            bool: True if pod should be included in drain, False otherwise.
        """
        # Skip completed pods
        if pod.status.phase in ['Succeeded', 'Failed']:
            return False
        
        # Skip mirror pods (static pods)
        if pod.metadata.annotations and 'kubernetes.io/config.mirror' in pod.metadata.annotations:
            return False
        
        # Handle DaemonSet pods
        if pod.metadata.owner_references:
            for owner in pod.metadata.owner_references:
                if owner.kind == 'DaemonSet':
                    return include_daemonsets
        
        return True
    
    @log_operation("get_pod_disruption_budgets")
    def get_relevant_pod_disruption_budgets(self, pods: List[client.V1Pod]) -> List[client.V1PodDisruptionBudget]:
        """
        Get PodDisruptionBudgets that might affect the given pods.
        
        Args:
            pods (List[client.V1Pod]): Pods to check PDBs for.
        
        Returns:
            List[client.V1PodDisruptionBudget]: Relevant PDBs.
        """
        try:
            all_pdbs = self.policy_v1.list_pod_disruption_budget_for_all_namespaces()
            relevant_pdbs = []
            
            for pdb in all_pdbs.items:
                if self._pdb_affects_pods(pdb, pods):
                    relevant_pdbs.append(pdb)
                    self.logger.debug(
                        "Found relevant PDB",
                        pdb_name=pdb.metadata.name,
                        namespace=pdb.metadata.namespace,
                        min_available=pdb.spec.min_available,
                        max_unavailable=pdb.spec.max_unavailable
                    )
            
            self.logger.info(
                "Retrieved relevant PDBs",
                total_pdbs=len(all_pdbs.items),
                relevant_pdbs=len(relevant_pdbs),
                pod_count=len(pods)
            )
            
            return relevant_pdbs
            
        except ApiException as e:
            self.logger.warning("Failed to get PodDisruptionBudgets", error=str(e))
            return []
    
    def _pdb_affects_pods(self, pdb: client.V1PodDisruptionBudget, pods: List[client.V1Pod]) -> bool:
        """
        Check if a PDB affects any of the given pods.
        
        Args:
            pdb (client.V1PodDisruptionBudget): The PDB to check.
            pods (List[client.V1Pod]): Pods to check against.
        
        Returns:
            bool: True if PDB affects any of the pods.
        """
        if not pdb.spec.selector or not pdb.spec.selector.match_labels:
            return False
        
        pdb_labels = pdb.spec.selector.match_labels
        
        for pod in pods:
            if pod.metadata.namespace != pdb.metadata.namespace:
                continue
                
            pod_labels = pod.metadata.labels or {}
            
            # Check if all PDB selector labels match pod labels
            if all(pod_labels.get(key) == value for key, value in pdb_labels.items()):
                return True
        
        return False
    
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
    
    @log_operation("create_eviction")
    def create_pod_eviction(self, pod: client.V1Pod) -> bool:
        """
        Create an eviction for a pod (respects PDBs).
        
        Args:
            pod (client.V1Pod): The pod to evict.
        
        Returns:
            bool: True if eviction was created successfully, False otherwise.
        """
        try:
            eviction = client.V1Eviction(
                metadata=client.V1ObjectMeta(
                    name=pod.metadata.name,
                    namespace=pod.metadata.namespace
                ),
                delete_options=client.V1DeleteOptions()
            )
            
            self.v1.create_namespaced_pod_eviction(
                name=pod.metadata.name,
                namespace=pod.metadata.namespace,
                body=eviction
            )
            
            self.logger.debug(
                "Pod eviction created",
                pod_name=pod.metadata.name,
                namespace=pod.metadata.namespace
            )
            return True
            
        except ApiException as e:
            if e.status == 429:  # Too Many Requests - PDB blocking
                self.logger.debug(
                    "Pod eviction blocked by PDB",
                    pod_name=pod.metadata.name,
                    namespace=pod.metadata.namespace,
                    error=e.reason
                )
            else:
                self.logger.warning(
                    "Failed to create pod eviction",
                    pod_name=pod.metadata.name,
                    namespace=pod.metadata.namespace,
                    error=str(e)
                )
            return False
    
    @log_operation("force_delete_pod")
    def force_delete_pod(self, pod: client.V1Pod) -> bool:
        """
        Force delete a pod (bypasses PDBs).
        
        Args:
            pod (client.V1Pod): The pod to force delete.
        
        Returns:
            bool: True if pod was force deleted successfully, False otherwise.
        """
        try:
            self.v1.delete_namespaced_pod(
                name=pod.metadata.name,
                namespace=pod.metadata.namespace,
                grace_period_seconds=0,
                body=client.V1DeleteOptions(grace_period_seconds=0)
            )
            
            self.logger.debug(
                "Pod force deleted",
                pod_name=pod.metadata.name,
                namespace=pod.metadata.namespace
            )
            return True
            
        except ApiException as e:
            self.logger.warning(
                "Failed to force delete pod",
                pod_name=pod.metadata.name,
                namespace=pod.metadata.namespace,
                error=str(e)
            )
            return False
    
    @log_operation("evict_pods_with_pdb_respect")
    def evict_pods_respecting_pdbs(self, pods: List[client.V1Pod]) -> Dict[str, Any]:
        """
        Evict pods while respecting PodDisruptionBudgets.
        
        Args:
            pods (List[client.V1Pod]): Pods to evict.
        
        Returns:
            Dict[str, Any]: Eviction results summary.
        """
        if not pods:
            return {
                'total_pods': 0,
                'successful_evictions': 0,
                'blocked_by_pdb': 0,
                'failed_evictions': 0,
                'blocked_pods': []
            }
        
        self.logger.info("Starting pod eviction with PDB respect", pod_count=len(pods))
        
        # Get relevant PDBs
        relevant_pdbs = self.get_relevant_pod_disruption_budgets(pods)
        
        successful_evictions = 0
        blocked_by_pdb = 0
        failed_evictions = 0
        blocked_pods = []
        
        for pod in pods:
            result = self.create_pod_eviction(pod)
            if result:
                successful_evictions += 1
            else:
                # Check if it was blocked by PDB by trying to get the pod
                try:
                    current_pod = self.v1.read_namespaced_pod(
                        name=pod.metadata.name,
                        namespace=pod.metadata.namespace
                    )
                    if current_pod:
                        blocked_by_pdb += 1
                        blocked_pods.append(f"{pod.metadata.namespace}/{pod.metadata.name}")
                except ApiException:
                    # Pod might have been deleted by other means
                    failed_evictions += 1
        
        result_summary = {
            'total_pods': len(pods),
            'successful_evictions': successful_evictions,
            'blocked_by_pdb': blocked_by_pdb,
            'failed_evictions': failed_evictions,
            'blocked_pods': blocked_pods,
            'relevant_pdbs': len(relevant_pdbs)
        }
        
        self.logger.info(
            "Pod eviction completed",
            **result_summary
        )
        
        return result_summary
    
    @log_operation("wait_for_pods_termination")
    def wait_for_pods_termination(self, node_name: str, timeout_seconds: int, 
                                 initial_pod_names: Optional[Set[str]] = None) -> Dict[str, Any]:
        """
        Wait for pods on a node to terminate.
        
        Args:
            node_name (str): Name of the Kubernetes node.
            timeout_seconds (int): Maximum time to wait.
            initial_pod_names (Set[str], optional): Initial set of pod names to track.
        
        Returns:
            Dict[str, Any]: Termination status and remaining pods.
        """
        start_time = time.time()
        check_interval = 10  # Check every 10 seconds
        
        if initial_pod_names is None:
            initial_pods = self.get_node_pods(node_name, include_daemonsets=False)
            initial_pod_names = {f"{pod.metadata.namespace}/{pod.metadata.name}" for pod in initial_pods}
        
        self.logger.info(
            "Starting pod termination wait",
            node_name=node_name,
            timeout_seconds=timeout_seconds,
            initial_pod_count=len(initial_pod_names)
        )
        
        while time.time() - start_time < timeout_seconds:
            current_pods = self.get_node_pods(node_name, include_daemonsets=False)
            current_pod_names = {f"{pod.metadata.namespace}/{pod.metadata.name}" for pod in current_pods}
            
            # Check which of our original pods are still running
            remaining_tracked_pods = initial_pod_names.intersection(current_pod_names)
            
            if not remaining_tracked_pods:
                elapsed = time.time() - start_time
                self.logger.info(
                    "All tracked pods terminated",
                    node_name=node_name,
                    elapsed_seconds=round(elapsed, 1)
                )
                return {
                    'success': True,
                    'elapsed_seconds': elapsed,
                    'remaining_pods': [],
                    'remaining_pod_count': 0
                }
            
            elapsed = time.time() - start_time
            self.logger.debug(
                "Waiting for pod termination",
                node_name=node_name,
                remaining_pod_count=len(remaining_tracked_pods),
                elapsed_seconds=round(elapsed, 1),
                timeout_seconds=timeout_seconds
            )
            
            time.sleep(check_interval)
        
        # Timeout reached - get final state
        final_pods = self.get_node_pods(node_name, include_daemonsets=False)
        final_pod_names = {f"{pod.metadata.namespace}/{pod.metadata.name}" for pod in final_pods}
        remaining_tracked_pods = list(initial_pod_names.intersection(final_pod_names))
        
        self.logger.warning(
            "Timeout waiting for pod termination",
            node_name=node_name,
            timeout_seconds=timeout_seconds,
            remaining_pod_count=len(remaining_tracked_pods),
            remaining_pods=remaining_tracked_pods[:5]  # Log first 5
        )
        
        return {
            'success': False,
            'elapsed_seconds': timeout_seconds,
            'remaining_pods': remaining_tracked_pods,
            'remaining_pod_count': len(remaining_tracked_pods)
        }
    
    @log_operation("drain_node")
    def drain_node(self, node_name: str, timeout_minutes: Optional[int] = None) -> DrainResult:
        """
        Drain a node using Kubernetes-native methods (respects PDBs).
        
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
            timeout_minutes=timeout_minutes,
            method="kubernetes_native"
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
            
            # Step 2: Get pods to drain (excluding DaemonSets)
            pods = self.get_node_pods(node_name, include_daemonsets=False)
            initial_pod_count = len(pods)
            
            if not pods:
                self.logger.log_node_action("drain_complete_no_pods", node_name)
                return DrainResult(
                    success=True,
                    node_name=node_name,
                    duration_seconds=time.time() - start_time,
                    pods_drained=0
                )
            
            # Step 3: Create initial pod name set for tracking
            initial_pod_names = {f"{pod.metadata.namespace}/{pod.metadata.name}" for pod in pods}
            
            # Step 4: Evict pods respecting PDBs
            self.logger.info(
                "Starting graceful pod eviction with PDB respect",
                node_name=node_name,
                pod_count=initial_pod_count
            )
            
            eviction_result = self.evict_pods_respecting_pdbs(pods)
            
            # Step 5: Wait for pods to terminate
            termination_result = self.wait_for_pods_termination(
                node_name, 
                timeout_seconds,
                initial_pod_names
            )
            
            if termination_result['success']:
                # All pods terminated gracefully
                duration = time.time() - start_time
                self.logger.log_node_action(
                    "drain_complete_graceful",
                    node_name,
                    pods_drained=initial_pod_count,
                    duration_seconds=round(duration, 2),
                    eviction_summary=eviction_result
                )
                
                return DrainResult(
                    success=True,
                    node_name=node_name,
                    duration_seconds=duration,
                    pods_drained=initial_pod_count,
                    timed_out=False
                )
            else:
                # Timeout reached - force delete remaining pods
                remaining_pod_names = termination_result['remaining_pods']
                
                self.logger.warning(
                    "Graceful drain timeout, force deleting remaining pods",
                    node_name=node_name,
                    remaining_pod_count=len(remaining_pod_names),
                    blocked_by_pdb_count=eviction_result.get('blocked_by_pdb', 0)
                )
                
                # Get current pod objects for force deletion
                current_pods = self.get_node_pods(node_name, include_daemonsets=False)
                pods_to_force_delete = [
                    pod for pod in current_pods
                    if f"{pod.metadata.namespace}/{pod.metadata.name}" in remaining_pod_names
                ]
                
                # Force delete remaining pods
                force_deleted_count = 0
                for pod in pods_to_force_delete:
                    if self.force_delete_pod(pod):
                        force_deleted_count += 1
                
                # Wait a bit more for force deletion to complete
                final_wait_result = self.wait_for_pods_termination(node_name, 60, initial_pod_names)
                
                duration = time.time() - start_time
                self.logger.log_node_action(
                    "drain_complete_forced",
                    node_name,
                    pods_drained=initial_pod_count,
                    force_deleted_count=force_deleted_count,
                    duration_seconds=round(duration, 2),
                    final_success=final_wait_result['success']
                )
                
                return DrainResult(
                    success=True,
                    node_name=node_name,
                    duration_seconds=duration,
                    pods_drained=initial_pod_count,
                    timed_out=True
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
        Drain multiple nodes in parallel using Kubernetes-native drain.
        
        Args:
            node_names (List[str]): List of node names to drain.
        
        Returns:
            List[DrainResult]: Results of drain operations.
        """
        max_workers = min(self.config.max_parallel_drains, len(node_names))
        
        self.logger.info(
            "Starting parallel node draining with PDB respect",
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
                            timed_out=result.timed_out,
                            method="kubernetes_native"
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