"""
Lead routing system for qualified prospects.
"""
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


class LeadRouter:
    """Routes qualified leads to appropriate sales representatives."""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize router with routing configuration.

        Args:
            config: Lead routing configuration
        """
        self.config = config
        self.high_priority_threshold = 0.75
        self.nurture_threshold = 0.50
        self.disqualified_threshold = 0.0

    def route(self, firm: Dict[str, Any], score: float) -> str:
        """
        Route a qualified lead based on score and firm data.

        Args:
            firm: Firm data
            score: ICP score

        Returns:
            Route category: "high_priority", "nurture", or "disqualified"
        """
        if score >= self.high_priority_threshold:
            return "high_priority"
        elif score >= self.nurture_threshold:
            return "nurture"
        else:
            return "disqualified"
