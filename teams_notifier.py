# Microsoft Teams and SMS Notification System
# Real-Time Environmental Event Alerting System

import json
import logging
import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import requests
from azure.communication.sms import SmsClient
from azure.core.credentials import AzureKeyCredential
from azure.identity import DefaultAzureCredential
import aiohttp

from ..config.settings import settings

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.monitoring.LOG_LEVEL),
    format=settings.monitoring.LOG_FORMAT
)
logger = logging.getLogger(__name__)

@dataclass
class NotificationMessage:
    """Data model for notification messages"""
    title: str
    message: str
    severity: str  # "Critical", "High", "Medium", "Low"
    event_type: str  # "earthquake", "weather", "anomaly"
    location: Optional[str] = None
    magnitude: Optional[float] = None
    timestamp: Optional[datetime] = None
    event_id: Optional[str] = None
    
    def to_teams_card(self) -> Dict[str, Any]:
        """Convert to Microsoft Teams adaptive card format"""
        
        # Determine color based on severity
        severity_colors = {
            "Critical": "#E74C3C",  # Red
            "High": "#F39C12",      # Orange  
            "Medium": "#F1C40F",    # Yellow
            "Low": "#27AE60"        # Green
        }
        
        # Determine icon based on event type
        event_icons = {
            "earthquake": "🌍",
            "weather": "⛈️", 
            "anomaly": "⚠️"
        }
        
        color = severity_colors.get(self.severity, "#95A5A6")
        icon = event_icons.get(self.event_type, "📢")
        
        # Build card content
        card_content = {
            "type": "message",
            "attachments": [{
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "type": "AdaptiveCard",
                    "version": "1.4",
                    "body": [
                        {
                            "type": "Container",
                            "style": "emphasis",
                            "items": [
                                {
                                    "type": "TextBlock",
                                    "text": f"{icon} {self.title}",
                                    "size": "Large",
                                    "weight": "Bolder",
                                    "color": "Attention" if self.severity in ["Critical", "High"] else "Default"
                                },
                                {
                                    "type": "TextBlock", 
                                    "text": f"**Severity:** {self.severity}",
                                    "size": "Medium",
                                    "weight": "Bolder",
                                    "color": "Attention" if self.severity == "Critical" else "Default"
                                }
                            ]
                        },
                        {
                            "type": "Container",
                            "items": [
                                {
                                    "type": "TextBlock",
                                    "text": self.message,
                                    "wrap": True,
                                    "size": "Medium"
                                }
                            ]
                        }
                    ]
                }
            }]
        }
        
        # Add optional fields
        if self.location:
            card_content["attachments"][0]["content"]["body"].append({
                "type": "FactSet",
                "facts": [
                    {
                        "title": "Location:",
                        "value": self.location
                    }
                ]
            })
            
        facts = []
        if self.magnitude:
            facts.append({
                "title": "Magnitude:",
                "value": f"{self.magnitude:.1f}"
            })
            
        if self.timestamp:
            facts.append({
                "title": "Time:",
                "value": self.timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")
            })
            
        if self.event_id:
            facts.append({
                "title": "Event ID:",
                "value": self.event_id
            })
            
        if facts:
            card_content["attachments"][0]["content"]["body"].append({
                "type": "FactSet",
                "facts": facts
            })
        
        # Add actions for critical events
        if self.severity == "Critical":
            card_content["attachments"][0]["content"]["actions"] = [
                {
                    "type": "Action.OpenUrl",
                    "title": "View Dashboard",
                    "url": "https://app.powerbi.com/groups/environmental-alerts/reports/dashboard"
                },
                {
                    "type": "Action.OpenUrl",
                    "title": "Emergency Procedures",
                    "url": "https://emergency.example.com/procedures"
                }
            ]
        
        return card_content

class TeamsNotifier:
    """Microsoft Teams notification client"""
    
    def __init__(self):
        self.webhook_url = settings.alerts.TEAMS_WEBHOOK_URL
        self.session = requests.Session()
        
        # Configure session with retry strategy
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            backoff_factor=1,
            allowed_methods=["POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        logger.info("Teams notifier initialized")
    
    def send_notification(self, message: NotificationMessage) -> bool:
        """
        Send notification to Microsoft Teams
        
        Args:
            message: NotificationMessage object
            
        Returns:
            bool: Success status
        """
        
        if not self.webhook_url:
            logger.warning("Teams webhook URL not configured")
            return False
        
        try:
            # Convert message to Teams adaptive card
            card_content = message.to_teams_card()
            
            logger.info(f"Sending Teams notification: {message.title} (Severity: {message.severity})")
            
            # Send to Teams
            response = self.session.post(
                self.webhook_url,
                json=card_content,
                headers={"Content-Type": "application/json"},
                timeout=30
            )
            
            response.raise_for_status()
            
            if response.status_code == 200:
                logger.info("Teams notification sent successfully")
                return True
            else:
                logger.warning(f"Teams notification failed with status: {response.status_code}")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error sending Teams notification: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error in Teams notification: {e}")
            return False
    
    async def send_async(self, message: NotificationMessage) -> bool:
        """Async version of send_notification"""
        
        if not self.webhook_url:
            logger.warning("Teams webhook URL not configured") 
            return False
        
        try:
            card_content = message.to_teams_card()
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.webhook_url,
                    json=card_content,
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    
                    if response.status == 200:
                        logger.info("Teams notification sent successfully (async)")
                        return True
                    else:
                        logger.warning(f"Teams notification failed with status: {response.status}")
                        return False
                        
        except Exception as e:
            logger.error(f"Error sending async Teams notification: {e}")
            return False

class SMSNotifier:
    """SMS notification client using Azure Communication Services"""
    
    def __init__(self):
        self.connection_string = settings.alerts.ACS_CONNECTION_STRING
        self.from_number = settings.alerts.SMS_FROM_NUMBER
        
        if self.connection_string:
            self.sms_client = SmsClient.from_connection_string(self.connection_string)
            logger.info("SMS notifier initialized with Azure Communication Services")
        else:
            self.sms_client = None
            logger.warning("SMS not configured - missing connection string")
    
    def send_sms(self, message: NotificationMessage, phone_numbers: List[str]) -> Dict[str, bool]:
        """
        Send SMS notifications to a list of phone numbers
        
        Args:
            message: NotificationMessage object
            phone_numbers: List of phone numbers in E.164 format (e.g., +1234567890)
            
        Returns:
            Dict mapping phone numbers to success status
        """
        
        if not self.sms_client:
            logger.warning("SMS client not initialized")
            return {num: False for num in phone_numbers}
        
        if not self.from_number:
            logger.warning("SMS from number not configured")
            return {num: False for num in phone_numbers}
        
        # Format SMS message (limit to 160 characters for basic SMS)
        sms_text = self._format_sms_message(message)
        
        results = {}
        
        for phone_number in phone_numbers:
            try:
                logger.info(f"Sending SMS to {phone_number}: {message.title}")
                
                response = self.sms_client.send(
                    from_=self.from_number,
                    to=[phone_number],
                    message=sms_text
                )
                
                # Check if SMS was sent successfully
                if response and hasattr(response, 'message_id'):
                    results[phone_number] = True
                    logger.info(f"SMS sent successfully to {phone_number}, ID: {response.message_id}")
                else:
                    results[phone_number] = False
                    logger.warning(f"SMS send failed to {phone_number}")
                    
            except Exception as e:
                logger.error(f"Error sending SMS to {phone_number}: {e}")
                results[phone_number] = False
        
        return results
    
    def _format_sms_message(self, message: NotificationMessage) -> str:
        """Format message for SMS (160 character limit)"""
        
        # Create concise SMS message
        event_icons = {
            "earthquake": "🌍",
            "weather": "⛈️",
            "anomaly": "⚠️"
        }
        
        icon = event_icons.get(message.event_type, "📢")
        
        # Build SMS text
        sms_parts = [
            f"{icon} {message.severity.upper()}",
            message.title
        ]
        
        if message.location:
            sms_parts.append(f"Location: {message.location}")
            
        if message.magnitude:
            sms_parts.append(f"Mag: {message.magnitude:.1f}")
        
        # Join parts and truncate if needed
        sms_text = " | ".join(sms_parts)
        
        if len(sms_text) > 160:
            # Truncate and add ellipsis
            sms_text = sms_text[:157] + "..."
        
        return sms_text

class NotificationCenter:
    """Central notification management system"""
    
    def __init__(self):
        self.teams = TeamsNotifier()
        self.sms = SMSNotifier()
        
        # Load recipient lists from settings
        self.critical_contacts = settings.alerts.CRITICAL_RECIPIENTS
        
        logger.info("Notification center initialized")
    
    def send_alert(
        self, 
        message: NotificationMessage,
        channels: List[str] = None,
        recipients: List[str] = None
    ) -> Dict[str, Any]:
        """
        Send alert through specified channels
        
        Args:
            message: NotificationMessage object
            channels: List of channels ("teams", "sms")
            recipients: List of phone numbers for SMS (optional)
            
        Returns:
            Dict with success status for each channel
        """
        
        if channels is None:
            # Default channels based on severity
            if message.severity == "Critical":
                channels = ["teams", "sms"]
            elif message.severity == "High":
                channels = ["teams", "sms"]
            else:
                channels = ["teams"]
        
        results = {}
        
        # Send Teams notification
        if "teams" in channels:
            try:
                teams_success = self.teams.send_notification(message)
                results["teams"] = {
                    "success": teams_success,
                    "timestamp": datetime.utcnow().isoformat()
                }
            except Exception as e:
                logger.error(f"Teams notification error: {e}")
                results["teams"] = {
                    "success": False,
                    "error": str(e),
                    "timestamp": datetime.utcnow().isoformat()
                }
        
        # Send SMS notifications
        if "sms" in channels:
            try:
                # Use provided recipients or default critical contacts
                sms_recipients = recipients or self.critical_contacts
                
                if sms_recipients:
                    sms_results = self.sms.send_sms(message, sms_recipients)
                    results["sms"] = {
                        "success": any(sms_results.values()),
                        "individual_results": sms_results,
                        "timestamp": datetime.utcnow().isoformat()
                    }
                else:
                    logger.warning("No SMS recipients configured")
                    results["sms"] = {
                        "success": False,
                        "error": "No recipients configured",
                        "timestamp": datetime.utcnow().isoformat()
                    }
                    
            except Exception as e:
                logger.error(f"SMS notification error: {e}")
                results["sms"] = {
                    "success": False,
                    "error": str(e),
                    "timestamp": datetime.utcnow().isoformat()
                }
        
        return results
    
    async def send_alert_async(
        self,
        message: NotificationMessage,
        channels: List[str] = None,
        recipients: List[str] = None
    ) -> Dict[str, Any]:
        """Async version of send_alert"""
        
        if channels is None:
            if message.severity == "Critical":
                channels = ["teams", "sms"]
            elif message.severity == "High":
                channels = ["teams", "sms"]
            else:
                channels = ["teams"]
        
        results = {}
        
        # Send notifications concurrently
        tasks = []
        
        if "teams" in channels:
            tasks.append(self._send_teams_async(message))
            
        if "sms" in channels:
            sms_recipients = recipients or self.critical_contacts
            tasks.append(self._send_sms_async(message, sms_recipients))
        
        # Wait for all tasks to complete
        if tasks:
            task_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for i, result in enumerate(task_results):
                if isinstance(result, Exception):
                    logger.error(f"Async notification task {i} failed: {result}")
                else:
                    results.update(result)
        
        return results
    
    async def _send_teams_async(self, message: NotificationMessage) -> Dict[str, Any]:
        """Send Teams notification async"""
        try:
            success = await self.teams.send_async(message)
            return {
                "teams": {
                    "success": success,
                    "timestamp": datetime.utcnow().isoformat()
                }
            }
        except Exception as e:
            return {
                "teams": {
                    "success": False,
                    "error": str(e),
                    "timestamp": datetime.utcnow().isoformat()
                }
            }
    
    async def _send_sms_async(self, message: NotificationMessage, recipients: List[str]) -> Dict[str, Any]:
        """Send SMS notifications async"""
        try:
            if recipients:
                # SMS client doesn't have async methods, so run in executor
                loop = asyncio.get_event_loop()
                sms_results = await loop.run_in_executor(
                    None, self.sms.send_sms, message, recipients
                )
                
                return {
                    "sms": {
                        "success": any(sms_results.values()),
                        "individual_results": sms_results,
                        "timestamp": datetime.utcnow().isoformat()
                    }
                }
            else:
                return {
                    "sms": {
                        "success": False,
                        "error": "No recipients provided",
                        "timestamp": datetime.utcnow().isoformat()
                    }
                }
        except Exception as e:
            return {
                "sms": {
                    "success": False,
                    "error": str(e),
                    "timestamp": datetime.utcnow().isoformat()
                }
            }

# Convenience functions for common alert types
def create_earthquake_alert(
    magnitude: float,
    location: str,
    event_time: datetime,
    event_id: str
) -> NotificationMessage:
    """Create earthquake alert notification"""
    
    # Determine severity based on magnitude
    if magnitude >= 7.0:
        severity = "Critical"
    elif magnitude >= 6.0:
        severity = "High"
    elif magnitude >= 5.0:
        severity = "Medium"
    else:
        severity = "Low"
    
    title = f"Earthquake Alert - Magnitude {magnitude:.1f}"
    message = f"A magnitude {magnitude:.1f} earthquake has been detected near {location}."
    
    return NotificationMessage(
        title=title,
        message=message,
        severity=severity,
        event_type="earthquake",
        location=location,
        magnitude=magnitude,
        timestamp=event_time,
        event_id=event_id
    )

def create_weather_alert(
    event_type: str,
    severity_level: str,
    area: str,
    headline: str
) -> NotificationMessage:
    """Create weather alert notification"""
    
    title = f"Weather Alert - {event_type}"
    message = f"{headline} affecting {area}"
    
    return NotificationMessage(
        title=title,
        message=message,
        severity=severity_level,
        event_type="weather",
        location=area,
        timestamp=datetime.utcnow()
    )

def create_anomaly_alert(
    anomaly_type: str,
    description: str,
    location: Optional[str] = None
) -> NotificationMessage:
    """Create anomaly detection alert"""
    
    title = f"Anomaly Detected - {anomaly_type}"
    
    return NotificationMessage(
        title=title,
        message=description,
        severity="Medium",
        event_type="anomaly",
        location=location,
        timestamp=datetime.utcnow()
    )

# Factory function
def create_notification_center() -> NotificationCenter:
    """Factory function to create notification center"""
    return NotificationCenter()

# Export main classes
__all__ = [
    'NotificationMessage', 'TeamsNotifier', 'SMSNotifier', 'NotificationCenter',
    'create_earthquake_alert', 'create_weather_alert', 'create_anomaly_alert',
    'create_notification_center'
]
