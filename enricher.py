"""
Data enrichment service for firmographic and contact data.
"""
import logging
import time
from typing import Dict, Any, Optional
import httpx

logger = logging.getLogger(__name__)


class Enricher:
    """Handles data enrichment for firms."""

    def __init__(self, base_url: str, timeout: int = 30, max_retries: int = 3):
        """
        Initialize enricher with API configuration.

        Args:
            base_url: Base URL for enrichment API
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
        """
        self.base_url = base_url
        self.timeout = timeout
        self.max_retries = max_retries
        self.client = httpx.Client(timeout=timeout)

    def _make_request(self, method: str, endpoint: str) -> Optional[Dict[str, Any]]:
        """
        Make HTTP request with exponential backoff retry logic.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint
            
        Returns:
            Response JSON or None if all retries exhausted
        """
        url = f"{self.base_url}{endpoint}"
        
        for attempt in range(self.max_retries):
            try:
                response = self.client.request(method, url)
                
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limited. Waiting {retry_after}s before retry")
                    time.sleep(retry_after)
                    continue
                
                if response.status_code == 500:
                    if attempt < self.max_retries - 1:
                        wait_time = 2 ** attempt
                        logger.warning(f"Server error. Retrying in {wait_time}s (attempt {attempt + 1}/{self.max_retries})")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"Server error after {self.max_retries} attempts")
                        return None
                
                if response.status_code == 404:
                    logger.warning(f"Resource not found: {url}")
                    return None
                
                if response.status_code >= 400:
                    logger.error(f"HTTP {response.status_code}: {url}")
                    return None
                
                return response.json()
                
            except httpx.TimeoutException:
                if attempt < self.max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.warning(f"Timeout. Retrying in {wait_time}s (attempt {attempt + 1}/{self.max_retries})")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Timeout after {self.max_retries} attempts: {url}")
                    return None
            except Exception as e:
                logger.error(f"Error fetching {url}: {str(e)}")
                return None
        
        return None

    def fetch_firmographic(self, firm_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch firmographic data for a firm.

        Args:
            firm_id: Unique identifier for the firm

        Returns:
            Firmographic data or None if unavailable
        """
        endpoint = f"/firms/{firm_id}/firmographic"
        data = self._make_request("GET", endpoint)
        
        if data is None:
            return None
        
        normalized = {
            "firm_id": data.get("firm_id"),
            "name": data.get("name"),
            "domain": data.get("domain"),
            "country": data.get("country"),
            "region": data.get("region"),
            "practice_areas": data.get("practice_areas", []),
            "num_lawyers": data.get("num_lawyers") or data.get("lawyer_count"),
        }
        
        return normalized if normalized.get("firm_id") else None

    def fetch_contact(self, firm_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch contact information for a firm.

        Args:
            firm_id: Unique identifier for the firm

        Returns:
            Contact data or None if unavailable
        """
        endpoint = f"/firms/{firm_id}/contact"
        return self._make_request("GET", endpoint)