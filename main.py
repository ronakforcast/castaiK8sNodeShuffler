
#!/usr/bin/env python3
"""
CAST AI Node Manager 

= A Python application to shuffles CAST AI nodes in a Kubernetes cluster.
"""

import sys
import signal
import time
from typing import Optional, List
from contextlib import contextmanager

from config import Config
from logger_utils import NodeManagerLogger
from cast_utils import CastAIClient, CastNode
from node_utils import KubernetesNodeManager
from batch_utils import NodeBatchProcessor
from rebalancing_utils import CastAIRebalancer
from kubernetes_utils import KubernetesPatcher, parse_patches_from_env
from alerts_utils import AlertManager, create_node_processing_alert, create_cast_api_alert, create_kubernetes_patch_alert


class CastAINodeManager:
    """Main application class for CAST AI node management."""
    
    def __init__(self, config: Config):
        self.config = config
        self.logger = NodeManagerLogger(config, "CastAINodeManager")
        self.cast_client: Optional[CastAIClient] = None
        self.k8s_manager: Optional[KubernetesNodeManager] = None
        self.batch_processor: Optional[NodeBatchProcessor] = None
        self.k8s_patcher: Optional[KubernetesPatcher] = None
        self.alert_manager: Optional[AlertManager] = None
        self._shutdown_requested = False
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        signal_name = signal.Signals(signum).name
        self.logger.warning(f"Received {signal_name}, initiating graceful shutdown...")
        self._shutdown_requested = True
    
    def _initialize_components(self) -> bool:
        """
        Initialize all application components.
        
        Returns:
            bool: True if initialization successful, False otherwise.
        """
        try:
            self.logger.info("Initializing application components")
            
            # Initialize alert manager
            self.alert_manager = AlertManager(
                pagerduty_key=self.config.pagerduty_integration_key,
                logger=self.logger,
                enabled=self.config.alerts_enabled
            )
            
            # Initialize CAST AI client
            self.cast_client = CastAIClient(self.config, self.logger)
            
            # Initialize Kubernetes manager
            self.k8s_manager = KubernetesNodeManager(self.config, self.logger)
            
            # Initialize Kubernetes patcher
            self.k8s_patcher = KubernetesPatcher(self.logger)
            
            # Initialize batch processor
            self.batch_processor = NodeBatchProcessor(
                self.config,
                self.logger,
                self.k8s_manager,
                self.cast_client
            )
            
            # Validate configuration
            if not self.batch_processor.validate_batch_configuration():
                raise ValueError("Invalid batch configuration")
            
            self.logger.info("All components initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error("Failed to initialize components", error=str(e))
            # Send critical alert for initialization failure
            if self.alert_manager:
                self.alert_manager.send_critical_alert(
                    "CAST AI Node Manager failed to initialize",
                    component="Initialization",
                    custom_details={"error": str(e)},
                    dedup_key="initialization-failure"
                )
            return False
    
    def _cleanup_resources(self) -> None:
        """Clean up resources."""
        self.logger.info("Cleaning up resources")
        
        if self.cast_client:
            try:
                self.cast_client.close()
            except Exception as e:
                self.logger.warning("Error closing CAST AI client", error=str(e))
        
        self.logger.info("Resource cleanup completed")
    
    @contextmanager
    def _application_context(self):
        """Context manager for application lifecycle."""
        try:
            if not self._initialize_components():
                raise RuntimeError("Failed to initialize components")
            yield
        finally:
            self._cleanup_resources()
    
    def apply_kubernetes_patches(self) -> bool:
        """
        Apply Kubernetes patches from environment configuration.
        
        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            patches = parse_patches_from_env(self.config.k8s_patches)
            if not patches:
                self.logger.info("No Kubernetes patches configured")
                return True
            
            success = self.k8s_patcher.apply_patches(patches)
            
            # Send alert if patches failed
            if not success:
                # Get failed patch details from the patcher if available
                failed_patches = [f"{p.kind}/{p.name}" for p in patches]  # Simplified - you can enhance this
                create_kubernetes_patch_alert(
                    self.alert_manager,
                    failed_patches=failed_patches,
                    total_patches=len(patches)
                )
            
            return success
        except Exception as e:
            self.logger.error("Failed to apply Kubernetes patches", error=str(e))
            self.alert_manager.send_error_alert(
                "Kubernetes patch operation failed with exception",
                component="KubernetesPatcher",
                custom_details={"error": str(e)},
                dedup_key="kubernetes-patch-exception"
            )
            return False
    
    def disable_cast_policies(self) -> bool:
        """
        Disable CAST AI policies.
        
        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            success = self.cast_client.disable_cast_policies()
            if not success:
                create_cast_api_alert(
                    self.alert_manager,
                    operation="disable_policies",
                    error="Failed to disable CAST AI policies"
                )
            return success
        except Exception as e:
            self.logger.error("Failed to disable CAST AI policies", error=str(e))
            create_cast_api_alert(
                self.alert_manager,
                operation="disable_policies",
                error=str(e)
            )
            return False
    
    def enable_cast_policies(self) -> bool:
        """
        Enable CAST AI policies.
        
        Returns:
            bool: True if successful, False otherwise.
        """
        try:
            success = self.cast_client.enable_cast_policies()
            if not success:
                create_cast_api_alert(
                    self.alert_manager,
                    operation="enable_policies",
                    error="Failed to enable CAST AI policies"
                )
            return success
        except Exception as e:
            self.logger.error("Failed to enable CAST AI policies", error=str(e))
            create_cast_api_alert(
                self.alert_manager,
                operation="enable_policies",
                error=str(e)
            )
            return False
    
    def get_cast_managed_nodes(self) -> List[CastNode]:
        """
        Get all CAST AI managed nodes that exist in Kubernetes.
        
        Returns:
            List[CastNode]: List of verified CAST AI managed nodes.
        """
        try:
            # Get nodes from CAST AI API
            all_cast_nodes = self.cast_client.get_cast_nodes()
            
            if not all_cast_nodes:
                self.logger.info("No nodes found in CAST AI")
                return []
            
            # Verify nodes exist in Kubernetes and are managed by CAST AI
            verified_nodes = []
            for node in all_cast_nodes:
                if self.k8s_manager.is_cast_managed_node(node.k8s_name):
                    verified_nodes.append(node)
                    self.logger.debug(
                        "Verified CAST AI managed node",
                        node_name=node.k8s_name,
                        cast_id=node.cast_id,
                        status=node.status,
                        instance_type=node.instance_type
                    )
                else:
                    self.logger.warning(
                        "Node exists in CAST AI but not managed in Kubernetes",
                        node_name=node.k8s_name,
                        cast_id=node.cast_id
                    )
            
            self.logger.info(
                "CAST AI managed nodes verification completed",
                total_cast_nodes=len(all_cast_nodes),
                verified_nodes=len(verified_nodes),
                unverified_nodes=len(all_cast_nodes) - len(verified_nodes)
            )
            
            return verified_nodes
            
        except Exception as e:
            self.logger.error("Failed to get CAST AI managed nodes", error=str(e))
            create_cast_api_alert(
                self.alert_manager,
                operation="get_nodes",
                error=str(e)
            )
            return []
    
    def process_nodes(self, nodes: List[CastNode]) -> bool:
        """
        Process nodes in batches.
        
        Args:
            nodes (List[CastNode]): Nodes to process.
        
        Returns:
            bool: True if processing completed successfully, False otherwise.
        """
        if not nodes:
            self.logger.info("No nodes to process")
            return True
        
        try:
            batch_results = self.batch_processor.process_node_batches(nodes)
            
            # Check if we were interrupted
            if self._shutdown_requested:
                self.logger.warning("Processing interrupted by shutdown request")
                self.alert_manager.send_warning_alert(
                    "Node processing interrupted by shutdown request",
                    component="NodeBatchProcessor",
                    dedup_key="processing-interrupted"
                )
                return False
            
            # Analyze results
            total_processed = sum(r.items_processed for r in batch_results)
            total_successful = sum(r.successful_items for r in batch_results)
            total_failed = total_processed - total_successful
            
            if total_failed > 0:
                self.logger.warning(
                    "Some nodes failed to process",
                    total_processed=total_processed,
                    successful=total_successful,
                    failed=total_failed
                )
                
                # Collect all errors for alerting
                all_errors = []
                for batch_result in batch_results:
                    if batch_result.errors:
                        for error in batch_result.errors:
                            self.logger.error(f"Batch {batch_result.batch_number} error: {error}")
                            all_errors.append(error)
                
                # Send alert for node processing failures
                create_node_processing_alert(
                    self.alert_manager,
                    failed_nodes=total_failed,
                    total_nodes=total_processed,
                    errors=all_errors
                )
                
                return False
            
            self.logger.info("All nodes processed successfully")
            # Resolve any existing node processing alerts
            self.alert_manager.resolve_alert("node-processing-failure")
            return True
            
        except Exception as e:
            self.logger.error("Failed to process nodes", error=str(e))
            self.alert_manager.send_critical_alert(
                "Node processing failed with unexpected error",
                component="NodeBatchProcessor",
                custom_details={"error": str(e)},
                dedup_key="node-processing-critical-failure"
            )
            return False
    
    # def run(self) -> int:
    #     """
    #     Main execution method.
        
    #     Returns:
    #         int: Exit code (0 for success, non-zero for failure).
    #     """
    #     start_time = time.time()
    #     self.logger.info(
    #         "Starting CAST AI Node Manager",
    #         version="2.0.0",
    #         config={
    #             'batch_size': self.config.batch_size,
    #             'drain_timeout_minutes': self.config.drain_timeout_minutes,
    #             'max_parallel_drains': self.config.max_parallel_drains,
    #             'batch_wait_seconds': self.config.batch_wait_seconds
    #         }
    #     )
        
    #     try:
    #         with self._application_context():
    #             # Check for shutdown request
    #             if self._shutdown_requested:
    #                 self.logger.info("Shutdown requested before processing started")
    #                 return 0
                
    #             # Step 0: Apply Kubernetes patches (only if configured)
    #             if self.config.k8s_patches:
    #                 self.logger.info("Step 0: Applying Kubernetes patches")
    #                 if not self.apply_kubernetes_patches():
    #                     self.logger.error("Failed to apply Kubernetes patches - STOPPING EXECUTION")
    #                     return 1
                
    #             # Check for shutdown request
    #             if self._shutdown_requested:
    #                 self.logger.info("Shutdown requested after applying patches")
    #                 return 0
                
    #             # Step 1: Disable CAST AI policies
    #             self.logger.info("Step 1: Disabling CAST AI policies")
    #             if not self.disable_cast_policies():
    #                 self.logger.error("Failed to disable CAST AI policies - STOPPING EXECUTION")
    #                 return 1
                
    #             # Check for shutdown request
    #             if self._shutdown_requested:
    #                 self.logger.info("Shutdown requested after disabling policies")
    #                 return 0
                
    #             # Step 2: Get CAST AI managed nodes
    #             self.logger.info("Step 2: Retrieving CAST AI managed nodes")
    #             nodes = self.get_cast_managed_nodes()
                
    #             if not nodes:
    #                 self.logger.info("No CAST AI managed nodes found, nothing to process")
    #                 return 0
                
    #             # Check for shutdown request
    #             if self._shutdown_requested:
    #                 self.logger.info("Shutdown requested after retrieving nodes")
    #                 return 0
                
    #             # Step 3: Process nodes in batches
    #             self.logger.info("Step 3: Processing nodes in batches")
    #             success = self.process_nodes(nodes)
                
    #             # Calculate total runtime
    #             total_duration = time.time() - start_time
                
    #             if success and not self._shutdown_requested:
    #                 self.logger.info(
    #                     "CAST AI Node Manager completed successfully",
    #                     total_duration_minutes=round(total_duration / 60, 2),
    #                     nodes_processed=len(nodes)
    #                 )
    #                 # Send success notification
    #                 self.alert_manager.send_info_alert(
    #                     f"CAST AI Node Manager completed successfully - {len(nodes)} nodes processed",
    #                     component="Main",
    #                     custom_details={
    #                         "nodes_processed": len(nodes),
    #                         "duration_minutes": round(total_duration / 60, 2)
    #                     },
    #                     dedup_key="execution-success"
    #                 )
    #                 return 0
    #             else:
    #                 self.logger.error(
    #                     "CAST AI Node Manager completed with errors",
    #                     total_duration_minutes=round(total_duration / 60, 2),
    #                     nodes_attempted=len(nodes),
    #                     shutdown_requested=self._shutdown_requested
    #                 )
    #                 return 1
                    
    #     except KeyboardInterrupt:
    #         self.logger.warning("Operation interrupted by user")
    #         self.alert_manager.send_warning_alert(
    #             "CAST AI Node Manager interrupted by user",
    #             component="Main",
    #             dedup_key="user-interruption"
    #         )
    #         return 130  # Standard exit code for Ctrl+C
    #     except Exception as e:
    #         total_duration = time.time() - start_time
    #         self.logger.error(
    #             "CAST AI Node Manager failed with unexpected error",
    #             error=str(e),
    #             error_type=type(e).__name__,
    #             total_duration_minutes=round(total_duration / 60, 2)
    #         )
    #         if self.alert_manager:
    #             self.alert_manager.send_critical_alert(
    #                 "CAST AI Node Manager failed with unexpected error",
    #                 component="Main",
    #                 custom_details={
    #                     "error": str(e),
    #                     "error_type": type(e).__name__,
    #                     "duration_minutes": round(total_duration / 60, 2)
    #                 },
    #                 dedup_key="unexpected-error"
    #             )
    #         return 1

    
    def run(self) -> int:
        """
        Main execution method.
        
        Returns:
            int: Exit code (0 for success, non-zero for failure).
        """
        start_time = time.time()
        self.logger.info(
            "Starting CAST AI Node Manager",
            version="2.0.0",
            job_type=self.config.job_type,
            config={
                'batch_size': self.config.batch_size,
                'drain_timeout_minutes': self.config.drain_timeout_minutes,
                'max_parallel_drains': self.config.max_parallel_drains,
                'batch_wait_seconds': self.config.batch_wait_seconds
            }
        )
        
        try:
            with self._application_context():
                # Check for shutdown request
                if self._shutdown_requested:
                    self.logger.info("Shutdown requested before processing started")
                    return 0
                
                # Route to appropriate job based on configuration
                if self.config.job_type == "CAST_TO_NONCAST":
                    return self._run_cast_to_noncast_job(start_time)
                elif self.config.job_type == "NONCAST_TO_CAST":
                    return self._run_noncast_to_cast_job(start_time)
                else:
                    self.logger.error(f"Unknown job type: {self.config.job_type}")
                    self.alert_manager.send_critical_alert(
                        f"Unknown job type configured: {self.config.job_type}",
                        component="Main",
                        custom_details={"job_type": self.config.job_type},
                        dedup_key="unknown-job-type"
                    )
                    return 1
                    
        except KeyboardInterrupt:
            self.logger.warning("Operation interrupted by user")
            self.alert_manager.send_warning_alert(
                "CAST AI Node Manager interrupted by user",
                component="Main",
                dedup_key="user-interruption"
            )
            return 130  # Standard exit code for Ctrl+C
        except Exception as e:
            total_duration = time.time() - start_time
            self.logger.error(
                "CAST AI Node Manager failed with unexpected error",
                error=str(e),
                error_type=type(e).__name__,
                total_duration_minutes=round(total_duration / 60, 2)
            )
            if self.alert_manager:
                self.alert_manager.send_critical_alert(
                    "CAST AI Node Manager failed with unexpected error",
                    component="Main",
                    custom_details={
                        "error": str(e),
                        "error_type": type(e).__name__,
                        "duration_minutes": round(total_duration / 60, 2)
                    },
                    dedup_key="unexpected-error"
                )
            return 1

    def _run_cast_to_noncast_job(self, start_time: float) -> int:
        """
        Execute CAST to Non-CAST job (existing functionality).
        
        Args:
            start_time (float): Job start time for duration calculation.
            
        Returns:
            int: Exit code (0 for success, non-zero for failure).
        """
        self.logger.info("Executing CAST to Non-CAST job")
        
        # Step 0: Apply Kubernetes patches (only if configured)
        if self.config.k8s_patches:
            self.logger.info("Step 0: Applying Kubernetes patches")
            if not self.apply_kubernetes_patches():
                self.logger.error("Failed to apply Kubernetes patches - STOPPING EXECUTION")
                return 1
        
        # Check for shutdown request
        if self._shutdown_requested:
            self.logger.info("Shutdown requested after applying patches")
            return 0
        
        # Step 1: Disable CAST AI policies
        self.logger.info("Step 1: Disabling CAST AI policies")
        if not self.disable_cast_policies():
            self.logger.error("Failed to disable CAST AI policies - STOPPING EXECUTION")
            return 1
        
        # Check for shutdown request
        if self._shutdown_requested:
            self.logger.info("Shutdown requested after disabling policies")
            return 0
        
        # Step 2: Get CAST AI managed nodes
        self.logger.info("Step 2: Retrieving CAST AI managed nodes")
        nodes = self.get_cast_managed_nodes()
        
        if not nodes:
            self.logger.info("No CAST AI managed nodes found, nothing to process")
            return 0
        
        # Check for shutdown request
        if self._shutdown_requested:
            self.logger.info("Shutdown requested after retrieving nodes")
            return 0
        
        # Step 3: Process nodes in batches (parallel draining)
        self.logger.info("Step 3: Processing nodes in parallel batches")
        success = self.process_nodes(nodes)
        
        # Calculate total runtime
        total_duration = time.time() - start_time
        
        if success and not self._shutdown_requested:
            self.logger.info(
                "CAST to Non-CAST job completed successfully",
                total_duration_minutes=round(total_duration / 60, 2),
                nodes_processed=len(nodes)
            )
            # Send success notification
            self.alert_manager.send_info_alert(
                f"CAST to Non-CAST job completed successfully - {len(nodes)} nodes processed",
                component="CAST_TO_NONCAST",
                custom_details={
                    "nodes_processed": len(nodes),
                    "duration_minutes": round(total_duration / 60, 2)
                },
                dedup_key="cast-to-noncast-success"
            )
            return 0
        else:
            self.logger.error(
                "CAST to Non-CAST job completed with errors",
                total_duration_minutes=round(total_duration / 60, 2),
                nodes_attempted=len(nodes),
                shutdown_requested=self._shutdown_requested
            )
            return 1

    def _run_noncast_to_cast_job(self, start_time: float) -> int:
        """
        Execute Non-CAST to CAST job (rebalancing functionality).
        
        Args:
            start_time (float): Job start time for duration calculation.
            
        Returns:
            int: Exit code (0 for success, non-zero for failure).
        """
        self.logger.info("Executing Non-CAST to CAST job (rebalancing)")
        
        # Step 0: Apply Kubernetes patches (only if configured)
        if self.config.k8s_patches:
            self.logger.info("Step 0: Applying Kubernetes patches")
            if not self.apply_kubernetes_patches():
                self.logger.error("Failed to apply Kubernetes patches - STOPPING EXECUTION")
                return 1
        
        # Check for shutdown request
        if self._shutdown_requested:
            self.logger.info("Shutdown requested after applying patches")
            return 0
        
        try:
            # Initialize rebalancer
            rebalancer = CastAIRebalancer(
                cluster_id=self.config.cast_cluster_id,
                api_key=self.config.cast_api_key,
                logger=self.logger,
                zone=self.config.rebalancing_zone,
                min_nodes=self.config.rebalancing_min_nodes
            )
            
            # Step 1: Enable CAST AI policies
            self.logger.info("Step 1: Enable CAST AI policies")
            if not self.enable_cast_policies():
                self.logger.error("Failed to Enable CAST AI policies - STOPPING EXECUTION")
                return 1
            
            # Step 2: Get all nodes from CAST AI
            self.logger.info("Step 2: Retrieving all nodes from CAST AI for rebalancing")
            all_node_ids = rebalancer.get_all_node_ids()
            
            if not all_node_ids:
                self.logger.info("No nodes found in CAST AI, nothing to rebalance")
                return 0
            
            # Check for shutdown request
            if self._shutdown_requested:
                self.logger.info("Shutdown requested after retrieving nodes")
                return 0
            
            # Step 3: Execute serial batch rebalancing
            self.logger.info("Step 3: Executing serial batch rebalancing")
            batch_results = rebalancer.execute_batch_rebalancing(
                node_ids=all_node_ids,
                batch_size=self.config.batch_size,
                batch_wait_seconds=self.config.batch_wait_seconds
            )
            
            # Analyze results
            successful_batches = sum(1 for r in batch_results if r.success)
            failed_batches = len(batch_results) - successful_batches
            
            # Check if we were interrupted
            if self._shutdown_requested:
                self.logger.warning("Rebalancing interrupted by shutdown request")
                self.alert_manager.send_warning_alert(
                    "Rebalancing interrupted by shutdown request",
                    component="NONCAST_TO_CAST",
                    dedup_key="rebalancing-interrupted"
                )
                return 0
            
            # Calculate total runtime
            total_duration = time.time() - start_time
            
            if failed_batches > 0:
                self.logger.error(
                    "Some rebalancing batches failed",
                    successful_batches=successful_batches,
                    failed_batches=failed_batches,
                    total_nodes=len(all_node_ids)
                )
                
                # Send alert for rebalancing failures
                failed_batch_details = [
                    f"Batch {r.batch_number}: {r.error or r.status}" 
                    for r in batch_results if not r.success
                ]
                
                self.alert_manager.send_error_alert(
                    f"Rebalancing failed: {failed_batches}/{len(batch_results)} batches failed",
                    component="NONCAST_TO_CAST",
                    custom_details={
                        "successful_batches": successful_batches,
                        "failed_batches": failed_batches,
                        "total_nodes": len(all_node_ids),
                        "failed_batch_details": failed_batch_details[:5]  # Limit details
                    },
                    dedup_key="rebalancing-failure"
                )
                return 1
            
            self.logger.info(
                "Non-CAST to CAST job completed successfully",
                total_duration_minutes=round(total_duration / 60, 2),
                nodes_processed=len(all_node_ids),
                batches_processed=len(batch_results)
            )
            
            # Send success notification
            self.alert_manager.send_info_alert(
                f"Non-CAST to CAST job completed successfully - {len(all_node_ids)} nodes rebalanced in {len(batch_results)} batches",
                component="NONCAST_TO_CAST",
                custom_details={
                    "nodes_processed": len(all_node_ids),
                    "batches_processed": len(batch_results),
                    "duration_minutes": round(total_duration / 60, 2)
                },
                dedup_key="noncast-to-cast-success"
            )
            return 0
            
        except Exception as e:
            total_duration = time.time() - start_time
            self.logger.error(
                "Non-CAST to CAST job failed with unexpected error",
                error=str(e),
                error_type=type(e).__name__,
                total_duration_minutes=round(total_duration / 60, 2)
            )
            
            self.alert_manager.send_critical_alert(
                "Non-CAST to CAST job failed with unexpected error",
                component="NONCAST_TO_CAST",
                custom_details={
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "duration_minutes": round(total_duration / 60, 2)
                },
                dedup_key="noncast-to-cast-critical-failure"
            )
            return 1
        finally:
            # Clean up rebalancer
            if 'rebalancer' in locals():
                rebalancer.close()

def main() -> int:
    """Main entry point."""
    try:
        # Load configuration
        config = Config.from_environment()
        config.validate()
        
        # Create and run the manager
        manager = CastAINodeManager(config)
        return manager.run()
        
    except ValueError as e:
        # Configuration errors
        print(f"Configuration error: {e}", file=sys.stderr)
        return 2
    except Exception as e:
        # Unexpected initialization errors
        print(f"Failed to initialize application: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())