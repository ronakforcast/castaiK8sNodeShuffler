#!/usr/bin/env python3
"""Alert utilities for PagerDuty integration."""

import json
import requests
from typing import Optional, Dict, Any, List
from enum import Enum
from dataclasses import dataclass
from datetime import datetime


class AlertSeverity(Enum):
    """Alert severity levels."""
    CRITICAL = "critical"
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class AlertAction(Enum):
    """PagerDuty event actions."""
    TRIGGER = "trigger"
    ACKNOWLEDGE = "acknowledge" 
    RESOLVE = "resolve"


@dataclass
class AlertPayload:
    """PagerDuty alert payload structure."""
    summary: str
    source: str
    severity: AlertSeverity
    timestamp: Optional[str] = None
    component: Optional[str] = None
    group: Optional[str] = None
    class_type: Optional[str] = None
    custom_details: Optional[Dict[str, Any]] = None


class PagerDutyAlerter:
    """PagerDuty integration for sending alerts."""
    
    def __init__(self, integration_key: str, logger):
        """
        Initialize PagerDuty alerter.
        
        Args:
            integration_key (str): PagerDuty integration key.
            logger: Logger instance for logging.
        """
        self.integration_key = integration_key
        self.logger = logger
        self.api_url = "https://events.pagerduty.com/v2/enqueue"
        
    def send_alert(
        self,
        summary: str,
        severity: AlertSeverity = AlertSeverity.ERROR,
        source: str = "CAST AI Node Manager",
        component: Optional[str] = None,
        group: Optional[str] = None,
        class_type: Optional[str] = None,
        custom_details: Optional[Dict[str, Any]] = None,
        dedup_key: Optional[str] = None,
        action: AlertAction = AlertAction.TRIGGER
    ) -> bool:
        """
        Send alert to PagerDuty.
        
        Args:
            summary (str): Brief description of the alert.
            severity (AlertSeverity): Alert severity level.
            source (str): Source of the alert.
            component (str, optional): Component that generated the alert.
            group (str, optional): Logical grouping of the alert.
            class_type (str, optional): Class/type of the event.
            custom_details (Dict, optional): Additional details for the alert.
            dedup_key (str, optional): Deduplication key for the alert.
            action (AlertAction): Action to perform (trigger/acknowledge/resolve).
        
        Returns:
            bool: True if alert sent successfully, False otherwise.
        """
        try:
            payload = self._build_payload(
                summary=summary,
                severity=severity,
                source=source,
                component=component,
                group=group,
                class_type=class_type,
                custom_details=custom_details,
                dedup_key=dedup_key,
                action=action
            )
            
            self.logger.info(
                "Sending PagerDuty alert",
                summary=summary,
                severity=severity.value,
                action=action.value,
                dedup_key=dedup_key
            )
            
            response = requests.post(
                self.api_url,
                headers={
                    "Content-Type": "application/json"
                },
                json=payload,
                timeout=30
            )
            
            if response.status_code == 202:
                response_data = response.json()
                self.logger.info(
                    "PagerDuty alert sent successfully",
                    dedup_key=response_data.get("dedup_key"),
                    status=response_data.get("status"),
                    message=response_data.get("message")
                )
                return True
            else:
                self.logger.error(
                    "Failed to send PagerDuty alert",
                    status_code=response.status_code,
                    response=response.text
                )
                return False
                
        except requests.exceptions.RequestException as e:
            self.logger.error("Network error sending PagerDuty alert", error=str(e))
            return False
        except Exception as e:
            self.logger.error("Unexpected error sending PagerDuty alert", error=str(e))
            return False
    
    def _build_payload(
        self,
        summary: str,
        severity: AlertSeverity,
        source: str,
        component: Optional[str],
        group: Optional[str],
        class_type: Optional[str],
        custom_details: Optional[Dict[str, Any]],
        dedup_key: Optional[str],
        action: AlertAction
    ) -> Dict[str, Any]:
        """Build PagerDuty API payload."""
        
        payload_data = {
            "summary": summary,
            "source": source,
            "severity": severity.value,
            "timestamp": datetime.utcnow().isoformat() + "Z"
        }
        
        # Add optional fields
        if component:
            payload_data["component"] = component
        if group:
            payload_data["group"] = group
        if class_type:
            payload_data["class"] = class_type
        if custom_details:
            payload_data["custom_details"] = custom_details
        
        event_payload = {
            "routing_key": self.integration_key,
            "event_action": action.value,
            "payload": payload_data
        }
        
        # Add dedup_key if provided
        if dedup_key:
            event_payload["dedup_key"] = dedup_key
            
        return event_payload
    
    def trigger_alert(
        self,
        summary: str,
        severity: AlertSeverity = AlertSeverity.ERROR,
        **kwargs
    ) -> bool:
        """Convenience method to trigger an alert."""
        return self.send_alert(summary=summary, severity=severity, action=AlertAction.TRIGGER, **kwargs)
    
    def resolve_alert(self, dedup_key: str) -> bool:
        """Convenience method to resolve an alert."""
        return self.send_alert(
            summary="Alert resolved",
            dedup_key=dedup_key,
            action=AlertAction.RESOLVE
        )
    
    def acknowledge_alert(self, dedup_key: str) -> bool:
        """Convenience method to acknowledge an alert."""
        return self.send_alert(
            summary="Alert acknowledged",
            dedup_key=dedup_key,
            action=AlertAction.ACKNOWLEDGE
        )


class AlertManager:
    """High-level alert manager for CAST AI Node Manager."""
    
    def __init__(self, pagerduty_key: str, logger, enabled: bool = True):
        """
        Initialize alert manager.
        
        Args:
            pagerduty_key (str): PagerDuty integration key.
            logger: Logger instance.
            enabled (bool): Whether alerting is enabled.
        """
        self.logger = logger
        self.enabled = enabled
        self.alerter = PagerDutyAlerter(pagerduty_key, logger) if pagerduty_key else None
        
        if not self.alerter and enabled:
            self.logger.warning("PagerDuty integration key not provided, alerting disabled")
            self.enabled = False
    
    def send_critical_alert(
        self,
        summary: str,
        component: Optional[str] = None,
        custom_details: Optional[Dict[str, Any]] = None,
        dedup_key: Optional[str] = None
    ) -> bool:
        """Send critical severity alert."""
        return self._send_alert(
            summary=summary,
            severity=AlertSeverity.CRITICAL,
            component=component,
            custom_details=custom_details,
            dedup_key=dedup_key
        )
    
    def send_error_alert(
        self,
        summary: str,
        component: Optional[str] = None,
        custom_details: Optional[Dict[str, Any]] = None,
        dedup_key: Optional[str] = None
    ) -> bool:
        """Send error severity alert."""
        return self._send_alert(
            summary=summary,
            severity=AlertSeverity.ERROR,
            component=component,
            custom_details=custom_details,
            dedup_key=dedup_key
        )
    
    def send_warning_alert(
        self,
        summary: str,
        component: Optional[str] = None,
        custom_details: Optional[Dict[str, Any]] = None,
        dedup_key: Optional[str] = None
    ) -> bool:
        """Send warning severity alert."""
        return self._send_alert(
            summary=summary,
            severity=AlertSeverity.WARNING,
            component=component,
            custom_details=custom_details,
            dedup_key=dedup_key
        )
    
    def send_info_alert(
        self,
        summary: str,
        component: Optional[str] = None,
        custom_details: Optional[Dict[str, Any]] = None,
        dedup_key: Optional[str] = None
    ) -> bool:
        """Send info severity alert."""
        return self._send_alert(
            summary=summary,
            severity=AlertSeverity.INFO,
            component=component,
            custom_details=custom_details,
            dedup_key=dedup_key
        )
    
    def _send_alert(
        self,
        summary: str,
        severity: AlertSeverity,
        component: Optional[str] = None,
        custom_details: Optional[Dict[str, Any]] = None,
        dedup_key: Optional[str] = None
    ) -> bool:
        """Internal method to send alerts."""
        if not self.enabled:
            self.logger.debug("Alerting disabled, skipping alert", summary=summary)
            return True
        
        if not self.alerter:
            self.logger.warning("PagerDuty alerter not configured", summary=summary)
            return False
        
        return self.alerter.trigger_alert(
            summary=summary,
            severity=severity,
            component=component,
            custom_details=custom_details,
            dedup_key=dedup_key
        )
    
    def resolve_alert(self, dedup_key: str) -> bool:
        """Resolve an existing alert."""
        if not self.enabled or not self.alerter:
            return True
        return self.alerter.resolve_alert(dedup_key)
    
    def acknowledge_alert(self, dedup_key: str) -> bool:
        """Acknowledge an existing alert."""
        if not self.enabled or not self.alerter:
            return True
        return self.alerter.acknowledge_alert(dedup_key)


# Convenience functions for common alert scenarios
def create_node_processing_alert(
    alert_manager: AlertManager,
    failed_nodes: int,
    total_nodes: int,
    errors: List[str]
) -> bool:
    """Create alert for node processing failures."""
    return alert_manager.send_error_alert(
        summary=f"CAST AI Node Manager: {failed_nodes}/{total_nodes} nodes failed to process",
        component="NodeBatchProcessor",
        custom_details={
            "failed_nodes": failed_nodes,
            "total_nodes": total_nodes,
            "errors": errors[:5],  # Limit to first 5 errors
            "error_count": len(errors)
        },
        dedup_key="node-processing-failure"
    )


def create_cast_api_alert(
    alert_manager: AlertManager,
    operation: str,
    error: str
) -> bool:
    """Create alert for CAST AI API failures."""
    return alert_manager.send_error_alert(
        summary=f"CAST AI API Error: {operation} failed",
        component="CastAIClient",
        custom_details={
            "operation": operation,
            "error": error
        },
        dedup_key=f"cast-api-{operation}-failure"
    )


def create_kubernetes_patch_alert(
    alert_manager: AlertManager,
    failed_patches: List[str],
    total_patches: int
) -> bool:
    """Create alert for Kubernetes patch failures."""
    return alert_manager.send_warning_alert(
        summary=f"Kubernetes Patches Failed: {len(failed_patches)}/{total_patches} patches failed",
        component="KubernetesPatcher",
        custom_details={
            "failed_patches": failed_patches,
            "total_patches": total_patches
        },
        dedup_key="kubernetes-patch-failure"
    )