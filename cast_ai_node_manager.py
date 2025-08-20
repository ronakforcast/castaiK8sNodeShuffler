#!/usr/bin/env python3
import os
import sys
import json
import time
import base64
import requests
from kubernetes import client, config
from kubernetes.client.rest import ApiException
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CastAINodeManager:
    def __init__(self):
        self.api_key = os.getenv('CAST_AI_API_KEY')
        self.cluster_id = os.getenv('CAST_AI_CLUSTER_ID')
        self.batch_size = int(os.getenv('BATCH_SIZE', '10'))
        
        if not self.api_key or not self.cluster_id:
            raise ValueError("CAST_AI_API_KEY and CAST_AI_CLUSTER_ID environment variables are required")
        
        self.cast_api_base = f"https://api.cast.ai/v1/kubernetes/external-clusters/{self.cluster_id}"
        self.headers = {
            'X-API-Key': self.api_key,
            'accept': 'application/json',
            'content-type': 'application/json'
        }
        
        # Initialize Kubernetes client
        try:
            config.load_incluster_config()
            logger.info("Loaded in-cluster Kubernetes config")
        except config.ConfigException:
            try:
                config.load_kube_config()
                logger.info("Loaded local Kubernetes config")
            except config.ConfigException:
                raise Exception("Could not load Kubernetes configuration")
        
        self.v1 = client.CoreV1Api()

    def disable_cast_policies(self):
        """Disable CAST AI policies"""
        logger.info("Disabling CAST AI Policies...")
        
        try:
            # Fetch current policies
            policies_url = f"https://api.cast.ai/v1/kubernetes/clusters/{self.cluster_id}/policies"
            response = requests.get(policies_url, headers=self.headers)
            response.raise_for_status()
            
            policies_json = response.json()
            policies_json['enabled'] = False
            
            # Apply modified policy
            response = requests.put(policies_url, headers=self.headers, json=policies_json)
            response.raise_for_status()
            
            logger.info("CAST AI policies disabled successfully")
            return True
        except requests.RequestException as e:
            logger.error(f"Failed to disable CAST AI policies: {e}")
            return False

    def get_cast_nodes(self):
        """Fetch CAST AI managed nodes"""
        logger.info("Fetching CAST AI nodes...")
        
        try:
            nodes_url = f"{self.cast_api_base}/nodes?nodeStatus=node_status_unspecified&lifecycleType=lifecycle_type_unspecified"
            response = requests.get(nodes_url, headers=self.headers)
            response.raise_for_status()
            
            node_data = response.json()
            cast_nodes = []
            
            for item in node_data.get('items', []):
                cast_node_id = item.get('id')
                k8s_node_name = item.get('name')
                
                if cast_node_id and k8s_node_name:
                    # Verify node is managed by CAST AI
                    if self.is_cast_managed_node(k8s_node_name):
                        cast_nodes.append({
                            'cast_id': cast_node_id,
                            'k8s_name': k8s_node_name
                        })
                        logger.info(f"Found CAST AI managed node: {k8s_node_name} ({cast_node_id})")
            
            logger.info(f"Total CAST AI nodes to process: {len(cast_nodes)}")
            return cast_nodes
            
        except requests.RequestException as e:
            logger.error(f"Failed to fetch CAST AI nodes: {e}")
            return []

    def is_cast_managed_node(self, node_name):
        """Check if node is managed by CAST AI"""
        try:
            node = self.v1.read_node(name=node_name)
            labels = node.metadata.labels or {}
            return labels.get('provisioner.cast.ai/managed-by') == 'cast.ai'
        except ApiException as e:
            logger.warning(f"Could not check node {node_name}: {e}")
            return False

    def drain_node(self, node_name):
        """Drain a Kubernetes node"""
        logger.info(f"Draining node: {node_name}")
        
        try:
            # Get all pods on the node
            field_selector = f"spec.nodeName={node_name}"
            pods = self.v1.list_pod_for_all_namespaces(field_selector=field_selector)
            
            # Delete pods (excluding DaemonSets and completed pods)
            for pod in pods.items:
                if pod.metadata.owner_references:
                    for owner in pod.metadata.owner_references:
                        if owner.kind == 'DaemonSet':
                            continue  # Skip DaemonSet pods
                
                if pod.status.phase in ['Succeeded', 'Failed']:
                    continue  # Skip completed pods
                
                try:
                    self.v1.delete_namespaced_pod(
                        name=pod.metadata.name,
                        namespace=pod.metadata.namespace,
                        grace_period_seconds=0
                    )
                    logger.info(f"Deleted pod {pod.metadata.name} from namespace {pod.metadata.namespace}")
                except ApiException as e:
                    logger.warning(f"Could not delete pod {pod.metadata.name}: {e}")
            
            # Cordon the node
            try:
                body = {"spec": {"unschedulable": True}}
                self.v1.patch_node(name=node_name, body=body)
                logger.info(f"Cordoned node: {node_name}")
            except ApiException as e:
                logger.warning(f"Could not cordon node {node_name}: {e}")
            
            return True
        except Exception as e:
            logger.error(f"Failed to drain node {node_name}: {e}")
            return False

    def delete_cast_node(self, node_id):
        """Delete node from CAST AI"""
        logger.info(f"Deleting node {node_id} from CAST AI...")
        
        try:
            delete_url = f"{self.cast_api_base}/nodes/{node_id}?forceDelete=true"
            response = requests.delete(delete_url, headers=self.headers)
            response.raise_for_status()
            logger.info(f"Successfully deleted node {node_id} from CAST AI")
            return True
        except requests.RequestException as e:
            logger.error(f"Failed to delete node {node_id} from CAST AI: {e}")
            return False

    def process_nodes_in_batches(self, nodes):
        """Process nodes in batches"""
        total_nodes = len(nodes)
        
        for i in range(0, total_nodes, self.batch_size):
            batch_num = i // self.batch_size + 1
            logger.info(f"Processing batch {batch_num}...")
            
            batch_nodes = nodes[i:i + self.batch_size]
            
            for node in batch_nodes:
                node_name = node['k8s_name']
                node_id = node['cast_id']
                
                # Drain the node
                if self.drain_node(node_name):
                    # Delete from CAST AI
                    self.delete_cast_node(node_id)
                else:
                    logger.error(f"Skipping CAST AI deletion for {node_name} due to drain failure")
            
            if i + self.batch_size < total_nodes:
                logger.info("Waiting for nodes in batch to terminate...")
                time.sleep(180)  # Wait 3 minutes between batches

    def run(self):
        """Main execution method"""
        logger.info("Starting CAST AI Node Management process...")
        
        try:
            # Step 1: Disable CAST AI policies
            if not self.disable_cast_policies():
                logger.error("Failed to disable CAST AI policies, continuing anyway...")
            
            # Step 2: Get CAST AI managed nodes
            nodes = self.get_cast_nodes()
            if not nodes:
                logger.info("No CAST AI managed nodes found")
                return
            
            # Step 3: Process nodes in batches
            self.process_nodes_in_batches(nodes)
            
            logger.info("All nodes processed successfully")
            
        except Exception as e:
            logger.error(f"Script execution failed: {e}")
            sys.exit(1)

if __name__ == "__main__":
    try:
        manager = CastAINodeManager()
        manager.run()
    except Exception as e:
        logger.error(f"Failed to initialize: {e}")
        sys.exit(1)