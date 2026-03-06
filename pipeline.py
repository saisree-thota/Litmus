"""
Main pipeline orchestrator for GTM data processing.
"""
import logging
import yaml
from typing import Any, List, Dict, Set, Tuple
import httpx
import difflib

from enricher import Enricher
from scorer import ICPScorer
from router import LeadRouter
from experiment import ExperimentAssigner
from webhook import WebhookClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from YAML file."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


class FirmDeduplicator:
    """Handles deduplication of firms based on domain and name similarity."""
    
    def __init__(self):
        self.seen_domains: Set[str] = set()
        self.firm_ids_by_domain: Dict[str, str] = {}
    
    def is_duplicate(self, firm: Dict[str, Any]) -> Tuple[bool, str]:
        """
        Check if firm is a duplicate based on domain and name similarity.
        
        Args:
            firm: Firm data
            
        Returns:
            Tuple of (is_duplicate, original_firm_id if duplicate)
        """
        domain = firm.get("domain", "").lower()
        
        if not domain:
            return False, ""
        
        if domain in self.seen_domains:
            return True, self.firm_ids_by_domain.get(domain, "")
        
        self.seen_domains.add(domain)
        self.firm_ids_by_domain[domain] = firm.get("id")
        
        return False, ""


def fetch_firms(base_url: str, timeout: int = 30) -> List[Dict[str, Any]]:
    """
    Fetch all firms from the API with pagination.
    
    Args:
        base_url: Base API URL
        timeout: Request timeout
        
    Returns:
        List of firms
    """
    firms = []
    page = 1
    client = httpx.Client(timeout=timeout)
    
    while True:
        try:
            response = client.get(
                f"{base_url}/firms",
                params={"page": page, "per_page": 10}
            )
            
            if response.status_code != 200:
                logger.error(f"Failed to fetch firms page {page}: {response.status_code}")
                break
            
            data = response.json()
            items = data.get("items", [])
            
            if not items:
                break
            
            firms.extend(items)
            
            total_pages = data.get("total_pages", 0)
            logger.info(f"Fetched page {page}/{total_pages}: {len(items)} firms")
            
            if page >= total_pages:
                break
            
            page += 1
            
        except Exception as e:
            logger.error(f"Error fetching firms: {str(e)}")
            break
    
    client.close()
    logger.info(f"Total firms fetched: {len(firms)}")
    return firms


def process_firm(
    firm: Dict[str, Any],
    enricher: Enricher,
    scorer: ICPScorer,
    router: LeadRouter,
    assigner: ExperimentAssigner,
    webhook_client: WebhookClient,
    config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Process a single firm through the entire pipeline.
    
    Args:
        firm: Basic firm data from API
        enricher: Enricher instance
        scorer: ICP scorer instance
        router: Lead router instance
        assigner: Experiment assigner instance
        webhook_client: Webhook client instance
        config: Pipeline configuration
        
    Returns:
        Processed firm result
    """
    firm_id = firm.get("id")
    logger.info(f"Processing firm: {firm_id} - {firm.get('name')}")
    
    result = {
        "firm_id": firm_id,
        "name": firm.get("name"),
        "domain": firm.get("domain"),
        "enriched": False,
        "scored": False,
        "routed": False,
        "webhook_sent": False
    }
    
    firmographic = enricher.fetch_firmographic(firm_id)
    contact = enricher.fetch_contact(firm_id)
    
    if not firmographic or not contact:
        logger.warning(f"Could not enrich firm {firm_id}: missing data")
        return result
    
    enriched_firm = {
        **firm,
        **firmographic,
        "contact": contact
    }
    result["enriched"] = True
    
    score = scorer.score(enriched_firm)
    result["score"] = score
    result["scored"] = True
    logger.info(f"  Score: {score:.2f}")
    
    route = router.route(enriched_firm, score)
    result["route"] = route
    result["routed"] = True
    logger.info(f"  Route: {route}")
    
    variant = assigner.assign_variant(firm_id)
    result["variant"] = variant
    logger.info(f"  Variant: {variant}")
    
    email = contact.get("email")
    if email and route != "disqualified":
        variant_config = config.get("experiments", {}).get("email_variants", {}).get(variant, {})
        subject = variant_config.get("subject", "")
        
        webhook_payload = {
            "firm_id": firm_id,
            "name": firm.get("name"),
            "score": score,
            "route": route,
            "contact": {
                "name": contact.get("name"),
                "email": email,
                "title": contact.get("title")
            },
            "variant": variant,
            "subject": subject
        }
        
        webhook_sent = webhook_client.fire(webhook_payload)
        result["webhook_sent"] = webhook_sent
        logger.info(f"  Webhook: {'sent' if webhook_sent else 'failed'}")
    
    return result


def run_pipeline(config_path: str) -> Any:
    """
    Run the complete GTM data pipeline.

    Args:
        config_path: Path to YAML configuration file

    Returns:
        Pipeline results (structure is yours to define)
    """
    logger.info("Starting GTM Data Pipeline")
    
    config = load_config(config_path)
    logger.info(f"Configuration loaded from {config_path}")
    
    apis_config = config.get("apis", {})
    enrichment_config = apis_config.get("enrichment", {})
    base_url = enrichment_config.get("base_url", "http://localhost:8000")
    timeout = enrichment_config.get("timeout", 30)
    max_retries = enrichment_config.get("max_retries", 3)
    
    enricher = Enricher(base_url, timeout, max_retries)
    scorer = ICPScorer(config)
    router = LeadRouter(config)
    assigner = ExperimentAssigner(config)
    webhook_client = WebhookClient(config)
    
    firms = fetch_firms(base_url, timeout)
    
    deduplicator = FirmDeduplicator()
    deduplicated_firms = []
    duplicates_removed = 0
    
    for firm in firms:
        is_dup, original_id = deduplicator.is_duplicate(firm)
        if is_dup:
            logger.debug(f"Duplicate firm detected: {firm.get('id')} (matches {original_id})")
            duplicates_removed += 1
        else:
            deduplicated_firms.append(firm)
    
    logger.info(f"Deduplicated firms: {len(deduplicated_firms)} (removed {duplicates_removed} duplicates)")
    
    results = {
        "total_firms": len(firms),
        "deduplicated_firms": len(deduplicated_firms),
        "duplicates_removed": duplicates_removed,
        "processed_firms": [],
        "summary": {
            "high_priority": 0,
            "nurture": 0,
            "disqualified": 0,
            "enrichment_failures": 0,
            "webhook_failures": 0
        }
    }
    
    for firm in deduplicated_firms:
        try:
            result = process_firm(
                firm,
                enricher,
                scorer,
                router,
                assigner,
                webhook_client,
                config
            )
            results["processed_firms"].append(result)
            
            if not result.get("enriched"):
                results["summary"]["enrichment_failures"] += 1
            
            if not result.get("webhook_sent") and result.get("routed"):
                if result.get("route") != "disqualified":
                    results["summary"]["webhook_failures"] += 1
            
            route = result.get("route")
            if route in results["summary"]:
                results["summary"][route] += 1
                
        except Exception as e:
            logger.error(f"Error processing firm {firm.get('id')}: {str(e)}")
            results["summary"]["enrichment_failures"] += 1
    
    logger.info("Pipeline execution completed")
    logger.info(f"Summary: {results['summary']}")
    
    return results


if __name__ == "__main__":
    import sys
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    result = run_pipeline(config_path)
    
    print("\n" + "="*60)
    print("PIPELINE RESULTS")
    print("="*60)
    print(f"Total firms processed: {result['total_firms']}")
    print(f"After deduplication: {result['deduplicated_firms']}")
    print(f"Duplicates removed: {result['duplicates_removed']}")
    print("\nLead Distribution:")
    print(f"  High Priority: {result['summary']['high_priority']}")
    print(f"  Nurture: {result['summary']['nurture']}")
    print(f"  Disqualified: {result['summary']['disqualified']}")
    print(f"\nFailures:")
    print(f"  Enrichment failures: {result['summary']['enrichment_failures']}")
    print(f"  Webhook failures: {result['summary']['webhook_failures']}")
    print("="*60)
