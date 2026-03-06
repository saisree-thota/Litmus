# GTM Pipeline Implementation - Write-up

## What I Built

I created an end-to-end GTM pipeline that fetches law firms, enriches them with data, scores them against ideal customer profiles, routes them to sales, assigns them to A/B test variants, and sends them to downstream systems via webhooks. The pipeline handles all the messy real-world API problems gracefully.

## How the Pipeline Works

**Data Flow:**
1. Fetch firms from `/firms` API (paginated)
2. Remove duplicates (same domain = duplicate)
3. For each firm:
   - Fetch firmographic data (size, practice areas, region)
   - Fetch contact information
   - Calculate ICP score (0.0 to 1.0)
   - Route into buckets (high_priority, nurture, or disqualified)
   - Assign to A/B test variant (variant_a or variant_b)
   - Send webhooks to CRM and email systems

## Key Design Decisions

**Modular Architecture**
- Each component is independent: Enricher, Scorer, Router, ExperimentAssigner, WebhookClient
- Easy to test, modify, or swap out individual pieces
- The main Pipeline orchestrator ties everything together

**API Resilience** ([enricher.py](enricher.py))
- Handles 429 (rate limit) by waiting the `Retry-After` seconds
- Handles 500 errors with exponential backoff (1s, 2s, 4s delays)
- Handles timeouts by retrying
- Returns None gracefully instead of crashing

**Schema Flexibility** ([enricher.py](enricher.py))
- The API sometimes returns inconsistent field names (`num_lawyers` vs `lawyer_count`)
- Normalization in the enricher handles both formats, so downstream code doesn't care

**ICP Scoring** ([scorer.py](scorer.py))
- Size: 40% weight - does firm have right number of lawyers?
- Practice areas: 35% weight - how many preferred practice areas does it have?
- Geography: 25% weight - is it in a target country?
- Score ranges 0.0 to 1.0, missing data gets neutral 0.5 values

**Deduplication** ([pipeline.py](pipeline.py))
- Simple approach: same domain = same firm (skip duplicates)
- Works well for law firms where domain is unique

**A/B Test Assignment** ([experiment.py](experiment.py))
- Hash-based, so same firm ID always gets same variant (deterministic)
- Uniform distribution across variants

**Webhook Delivery** ([webhook.py](webhook.py))
- Sends to both CRM and email endpoints
- Also uses exponential backoff retry logic on failures

## What Works (All 9 Requirements ✅)

| Requirement | Status | Test Result |
|---|---|---|
| 1. Pipeline Orchestration | ✅ | Processes complete workflow |
| 2. API Integration (retries + rate limiting) | ✅ | Handles 429s, 500s, timeouts |
| 3. Data Enrichment | ✅ | Fetches firmographic + contact data |
| 4. Deduplication | ✅ | Removes domain-based duplicates |
| 5. ICP Scoring | ✅ | Scores firms 0.0-1.0 |
| 6. Lead Routing | ✅ | Routes into 3 categories |
| 7. A/B Test Assignment | ✅ | Assigns to variants deterministically |
| 8. Webhook Integration | ✅ | Fires webhooks with retries |
| 9. Error Handling & Logging | ✅ | Comprehensive logging throughout |

## Trade-offs & Future Work

**What I Did Simply:**
- Deduplication by domain only (fuzzy matching is overkill for this dataset)
- Sequential processing (async would be faster but adds complexity)
- Scoring is transparent weighted formula (ML would be better with real data)

**If I Had More Time:**
- Add async processing for 10x faster execution
- Implement fuzzy matching for name similarity
- Store results in database for historical analysis
- Add ML model trained on historical conversion data
- Implement multi-armed bandit for optimizing A/B test variants
- Add monitoring/alerting (DataDog, etc.)

## How to Run It

```bash
# Terminal 1: Start mock server
python mock_server.py

# Terminal 2: Run pipeline
python pipeline.py config.yaml
```

Watch for logs showing scores, routing decisions, and webhook deliveries.

## Configuration Driven

Everything is configurable in `config.yaml`:
- ICP scoring weights and firm size ranges
- Preferred practice areas and regions
- API timeouts and retry counts
- Webhook endpoints
- Email variant definitions

No code changes needed - just update the YAML file.

## Code Quality

✅ Type hints for clarity  
✅ Comprehensive docstrings  
✅ Single responsibility principle (each class does one thing)  
✅ Structured logging at each step  
✅ Proper error handling (doesn't swallow errors)  
✅ No commented code  
✅ Clean, readable variable names
