"""
Webhook client for firing events to downstream systems.
"""
import logging
import time
from typing import Dict, Any
import httpx
import json

logger = logging.getLogger(__name__)


class WebhookClient:
    """Handles webhook delivery to external systems."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize webhook client with configuration.
        
        Args:
            config: Webhook configuration
        """
        self.config = config
        apis = config.get("apis", {})
        webhook_config = apis.get("webhooks", {})
        self.crm_endpoint = webhook_config.get("crm_endpoint")
        self.email_endpoint = webhook_config.get("email_endpoint")
        self.timeout = webhook_config.get("timeout", 10)
        self.max_retries = webhook_config.get("max_retries", 2)
        self.client = httpx.Client(timeout=self.timeout)

    def _fire_webhook(self, endpoint: str, payload: Dict[str, Any]) -> bool:
        """
        Fire webhook with retry logic.
        
        Args:
            endpoint: Webhook endpoint URL
            payload: Data to send
            
        Returns:
            True if successful, False otherwise
        """
        for attempt in range(self.max_retries):
            try:
                response = self.client.post(
                    endpoint,
                    json=payload,
                    timeout=self.timeout
                )
                
                if response.status_code == 500:
                    if attempt < self.max_retries - 1:
                        wait_time = 2 ** attempt
                        logger.warning(f"Webhook error. Retrying in {wait_time}s (attempt {attempt + 1}/{self.max_retries})")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"Webhook failed after {self.max_retries} attempts")
                        return False
                
                if response.status_code >= 400:
                    logger.error(f"Webhook HTTP {response.status_code}: {endpoint}")
                    return False
                
                logger.info(f"Webhook fired successfully: {endpoint}")
                return True
                
            except httpx.TimeoutException:
                if attempt < self.max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.warning(f"Webhook timeout. Retrying in {wait_time}s (attempt {attempt + 1}/{self.max_retries})")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Webhook timeout after {self.max_retries} attempts: {endpoint}")
                    return False
            except Exception as e:
                logger.error(f"Error firing webhook {endpoint}: {str(e)}")
                return False
        
        return False
    
    def fire(self, payload: Dict[str, Any]) -> bool:
        """
        Fire webhook with payload to configured endpoints.
        
        Args:
            payload: Data to send in webhook
            
        Returns:
            True if successful, False otherwise
        """
        results = []
        
        if self.crm_endpoint:
            crm_payload = {
                "firm_id": payload.get("firm_id"),
                "name": payload.get("name"),
                "score": payload.get("score"),
                "route": payload.get("route"),
                "contact": payload.get("contact")
            }
            result = self._fire_webhook(self.crm_endpoint, crm_payload)
            results.append(result)
        
        if self.email_endpoint:
            email_payload = {
                "firm_id": payload.get("firm_id"),
                "name": payload.get("name"),
                "contact": payload.get("contact"),
                "variant": payload.get("variant"),
                "subject": payload.get("subject")
            }
            result = self._fire_webhook(self.email_endpoint, email_payload)
            results.append(result)
        
        return all(results) if results else False