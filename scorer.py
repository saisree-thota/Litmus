"""
ICP scoring system for evaluating firm fit.
"""
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


class ICPScorer:
    """Scores firms against ideal customer profile criteria."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize scorer with ICP configuration.
        
        Args:
            config: ICP scoring configuration
        """
        self.config = config
        self.icp = config.get("icp_criteria", {})
        
    def score(self, firm: Dict[str, Any]) -> float:
        """
        Calculate ICP score for a firm.
        
        Args:
            firm: Firm data with enriched information
            
        Returns:
            ICP score between 0.0 and 1.0
        """
        scores = []
        weights = []
        
        size_score = self._score_firm_size(firm)
        if size_score is not None:
            scores.append(size_score)
            weights.append(0.4)
        
        practice_score = self._score_practice_areas(firm)
        if practice_score is not None:
            scores.append(practice_score)
            weights.append(0.35)
        
        geo_score = self._score_geography(firm)
        if geo_score is not None:
            scores.append(geo_score)
            weights.append(0.25)
        
        if not scores:
            return 0.0
        
        total_weight = sum(weights)
        if total_weight == 0:
            return 0.0
        
        weighted_sum = sum(s * w for s, w in zip(scores, weights))
        return min(1.0, max(0.0, weighted_sum / total_weight))
    
    def _score_firm_size(self, firm: Dict[str, Any]) -> float:
        """
        Score based on firm size (number of lawyers).
        
        Returns 1.0 if within range, 0.0 otherwise.
        """
        try:
            num_lawyers = firm.get("num_lawyers")
            if num_lawyers is None:
                return 0.5
            
            size_config = self.icp.get("firm_size", {})
            min_lawyers = size_config.get("min_lawyers", 50)
            max_lawyers = size_config.get("max_lawyers", 500)
            
            if min_lawyers <= num_lawyers <= max_lawyers:
                return 1.0
            
            if num_lawyers < min_lawyers:
                return max(0.0, 1.0 - (min_lawyers - num_lawyers) / min_lawyers)
            
            if num_lawyers > max_lawyers:
                return max(0.0, 1.0 - (num_lawyers - max_lawyers) / max_lawyers)
                
        except (TypeError, ValueError):
            return 0.5
        
        return 0.0
    
    def _score_practice_areas(self, firm: Dict[str, Any]) -> float:
        """
        Score based on practice areas matching preferred areas.
        
        Returns ratio of matching practice areas to total firm areas.
        """
        try:
            firm_areas = firm.get("practice_areas", [])
            if not firm_areas:
                return 0.5
            
            config = self.icp.get("practice_areas", {})
            preferred = set(config.get("preferred", []))
            
            if not preferred:
                return 0.5
            
            firm_areas_set = set(firm_areas)
            matches = len(firm_areas_set & preferred)
            score = matches / len(firm_areas_set)
            
            return score
            
        except (TypeError, ValueError):
            return 0.5
        
        return 0.0
    
    def _score_geography(self, firm: Dict[str, Any]) -> float:
        """
        Score based on geographic location (country).
        
        Returns 1.0 if in preferred regions, 0.0 otherwise.
        """
        try:
            country = firm.get("country")
            if not country:
                return 0.5
            
            config = self.icp.get("geography", {})
            preferred = config.get("preferred_regions", [])
            
            if not preferred:
                return 0.5
            
            if country in preferred:
                return 1.0
            
            return 0.0
            
        except (TypeError, ValueError):
            return 0.5
        
        return 0.0