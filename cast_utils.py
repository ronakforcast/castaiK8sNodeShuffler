#!/usr/bin/env python3
"""CAST AI API utilities."""

import requests
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from config import Config
from logger_utils import NodeManagerLogger, log_operation, retry_with_logging


@dataclass
class CastNode:
    """Represents a CAST AI managed node."""
    cast_id: str
    k8s_name: str
    status: Optional[str] = None
    instance_type: Optional[str] = None


class CastAIClient:
    """Client for CAST AI API operations."""
    
    def __init__(self, config: Config, logger: NodeManagerLogger):
        self.config = config
        self.logger = logger
        self.session = requests.Session()
        self.session.headers.update({
            'X-API-Key': config.cast_ai_api_key,
            'accept': 'application/json',
            'content-type': 'application/json'
        })
        self.base_url = f"{config.cast_api_base_url}/kubernetes"
        self.cluster_base_url = f"{self.base_url}/external-clusters/{config.cast_ai_cluster_id}"
    
    @log_operation("disable_cast_policies")
    @retry_with_logging(max_retries=3, delay_seconds=30)
    def disable_cast_policies(self) -> bool:
        """
        Disable CAST AI policies for the cluster.
        
        Returns:
            bool: True if policies were disabled successfully, False otherwise.
        """
        policies_url = f"{self.base_url}/clusters/{self.config.cast_ai_cluster_id}/policies"
        
        try:
            # Fetch current policies
            self.logger.info("Fetching current CAST AI policies")
            response = self.session.get(policies_url, timeout=30)
            response.raise_for_status()
            
            policies_json = response.json()
            self.logger.debug("Current policies retrieved", policies=policies_json)
            
            # Disable policies
            policies_json['enabled'] = False
            
            self.logger.info("Disabling CAST AI policies")
            response = self.session.put(policies_url, json=policies_json, timeout=30)
            response.raise_for_status()
            
            self.logger.info("CAST AI policies disabled successfully")
            return True
            
        except requests.RequestException as e:
            self.logger.error("Failed to disable CAST AI policies", error=str(e))
            raise
    
    @log_operation("get_cast_nodes")
    @retry_with_logging(max_retries=3, delay_seconds=10)
    def get_cast_nodes(self) -> List[CastNode]:
        """
        Fetch all CAST AI managed nodes.
        
        Returns:
            List[CastNode]: List of CAST AI managed nodes.
        """
        nodes_url = (
            f"{self.cluster_base_url}/nodes"
            "?nodeStatus=node_status_unspecified"
            "&lifecycleType=lifecycle_type_unspecified"
        )
        
        try:
            self.logger.info("Fetching CAST AI nodes from API")
            response = self.session.get(nodes_url, timeout=30)
            response.raise_for_status()
            
            node_data = response.json()
            cast_nodes = []
            
            for item in node_data.get('items', []):
                cast_node_id = item.get('id')
                k8s_node_name = item.get('name')
                status = item.get('status', {}).get('phase')
                instance_type = item.get('instanceType')
                
                if cast_node_id and k8s_node_name:
                    node = CastNode(
                        cast_id=cast_node_id,
                        k8s_name=k8s_node_name,
                        status=status,
                        instance_type=instance_type
                    )
                    cast_nodes.append(node)
                    
                    self.logger.debug(
                        "Found CAST AI node",
                        node_name=k8s_node_name,
                        cast_id=cast_node_id,
                        status=status,
                        instance_type=instance_type
                    )
            
            self.logger.info("CAST AI nodes retrieved", total_nodes=len(cast_nodes))
            return cast_nodes
            
        except requests.RequestException as e:
            self.logger.error("Failed to fetch CAST AI nodes from API", error=str(e))
            raise
    
    @log_operation("delete_cast_node")
    @retry_with_logging(max_retries=3, delay_seconds=30)
    def delete_cast_node(self, node_id: str, force_delete: bool = True) -> bool:
        """
        Delete a node from CAST AI.
        
        Args:
            node_id (str): CAST AI node ID.
            force_delete (bool): Whether to force delete the node.
        
        Returns:
            bool: True if node was deleted successfully, False otherwise.
        """
        delete_url = f"{self.cluster_base_url}/nodes/{node_id}"
        if force_delete:
            delete_url += "?forceDelete=true"
        
        try:
            self.logger.info("Deleting node from CAST AI", node_id=node_id, force_delete=force_delete)
            response = self.session.delete(delete_url, timeout=60)
            response.raise_for_status()
            
            self.logger.info("Node deleted from CAST AI successfully", node_id=node_id)
            return True
            
        except requests.RequestException as e:
            self.logger.error("Failed to delete node from CAST AI", node_id=node_id, error=str(e))
            raise
    
    @log_operation("get_node_details")
    def get_node_details(self, node_id: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a specific node.
        
        Args:
            node_id (str): CAST AI node ID.
        
        Returns:
            Optional[Dict[str, Any]]: Node details or None if not found.
        """
        node_url = f"{self.cluster_base_url}/nodes/{node_id}"
        
        try:
            response = self.session.get(node_url, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            self.logger.warning("Failed to get node details", node_id=node_id, error=str(e))
            return None
    
    def close(self) -> None:
        """Close the HTTP session."""
        self.session.close()
        self.logger.debug("CAST AI client session closed")