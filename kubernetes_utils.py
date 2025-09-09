#!/usr/bin/env python3
"""Kubernetes utilities for patch operations."""

import json
import subprocess
from typing import List, Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class PatchOperation:
    """Represents a single patch operation."""
    kind: str
    name: str
    namespace: Optional[str]
    patch: Dict[str, Any]


class KubernetesPatcher:
    """Utility class for applying Kubernetes patches."""
    
    def __init__(self, logger):
        self.logger = logger
    
    def apply_patches(self, patches: List[PatchOperation]) -> bool:
        """
        Apply multiple Kubernetes patches.
        
        Args:
            patches (List[PatchOperation]): List of patch operations to apply.
        
        Returns:
            bool: True if all patches applied successfully, False otherwise.
        """
        if not patches:
            self.logger.info("No patches to apply")
            return True
        
        self.logger.info("Starting Kubernetes patch operations", total_patches=len(patches))
        
        success_count = 0
        failed_patches = []
        
        for i, patch_op in enumerate(patches, 1):
            try:
                self.logger.info(
                    "Applying patch",
                    patch_number=f"{i}/{len(patches)}",
                    kind=patch_op.kind,
                    name=patch_op.name,
                    namespace=patch_op.namespace
                )
                
                if self._apply_single_patch(patch_op):
                    success_count += 1
                    self.logger.info(
                        "Patch applied successfully",
                        kind=patch_op.kind,
                        name=patch_op.name,
                        namespace=patch_op.namespace
                    )
                else:
                    failed_patches.append(f"{patch_op.kind}/{patch_op.name}")
                    
            except Exception as e:
                failed_patches.append(f"{patch_op.kind}/{patch_op.name}")
                self.logger.error(
                    "Failed to apply patch",
                    kind=patch_op.kind,
                    name=patch_op.name,
                    namespace=patch_op.namespace,
                    error=str(e)
                )
        
        # Log summary
        if failed_patches:
            self.logger.warning(
                "Patch operations completed with failures",
                successful=success_count,
                failed=len(failed_patches),
                failed_patches=failed_patches
            )
            return False
        else:
            self.logger.info(
                "All patch operations completed successfully",
                total_patches=success_count
            )
            return True
    
    def _apply_single_patch(self, patch_op: PatchOperation) -> bool:
        """
        Apply a single Kubernetes patch using kubectl.
        
        Args:
            patch_op (PatchOperation): Patch operation to apply.
        
        Returns:
            bool: True if patch applied successfully, False otherwise.
        """
        try:
            # Build kubectl command
            cmd = [
                "kubectl", "patch",
                patch_op.kind.lower(),
                patch_op.name,
                "--type", "merge",
                "--patch", json.dumps(patch_op.patch)
            ]
            
            # Add namespace if specified
            if patch_op.namespace:
                cmd.extend(["--namespace", patch_op.namespace])
            
            # Execute the command
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                if result.stdout.strip():
                    self.logger.debug("kubectl patch output", output=result.stdout.strip())
                return True
            else:
                self.logger.error(
                    "kubectl patch command failed",
                    return_code=result.returncode,
                    stderr=result.stderr.strip(),
                    stdout=result.stdout.strip()
                )
                return False
                
        except subprocess.TimeoutExpired:
            self.logger.error("kubectl patch command timed out")
            return False
        except Exception as e:
            self.logger.error("Error executing kubectl patch", error=str(e))
            return False


def parse_patches_from_env(patches_env_var: str) -> List[PatchOperation]:
    """
    Parse patch operations from environment variable.
    
    Expected format: JSON array of objects with keys: kind, name, namespace (optional), patch
    Example: '[{"kind":"Deployment","name":"my-app","namespace":"default","patch":{"spec":{"replicas":3}}}]'
    
    Args:
        patches_env_var (str): Environment variable content containing patches.
    
    Returns:
        List[PatchOperation]: List of patch operations.
    """
    if not patches_env_var:
        return []
    
    try:
        patches_data = json.loads(patches_env_var)
        patch_operations = []
        
        for patch_data in patches_data:
            patch_op = PatchOperation(
                kind=patch_data["kind"],
                name=patch_data["name"],
                namespace=patch_data.get("namespace"),
                patch=patch_data["patch"]
            )
            patch_operations.append(patch_op)
        
        return patch_operations
        
    except (json.JSONDecodeError, KeyError) as e:
        raise ValueError(f"Invalid patches format in environment variable: {e}")