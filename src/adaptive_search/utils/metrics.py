"""
API Metrics Tracking Module

Module to collect and emit structured logging of API usage metrics
for the Adaptive Restaurant Search application.

This module defines the APIMetrics class, which keeps counters for:
  - total API requests made: Number of calls to the Google Places API
  - total results returned by the API: Cumulative results across all pages
  - unique results retained after deduplication: Distinct place entries

It also provides a method to log these metrics—with an efficiency ratio—
via the application's JSON-formatted logger.

Classes:
    APIMetrics: Singleton class that tracks API usage metrics throughout the application's
               runtime and provides methods to log them.

Attributes:
    api_metrics: Global instance of APIMetrics used throughout the application to
                maintain consistent counters across all search operations.

Usage:
    Import the api_metrics singleton instance and increment its counters in API-calling code:

    ```python
    from utils.metrics import api_metrics
    
    # After making an API request
    api_metrics.total_requests += 1
    
    # After receiving results
    api_metrics.results_returned += len(results)
    
    # After deduplication
    api_metrics.unique_results += len(unique_places)
    
    # Log the current metrics
    api_metrics.log_metrics()
    ```

The metrics are particularly useful for monitoring:
  - API usage efficiency (unique results per total results)
  - API quota consumption
  - Search algorithm effectiveness over time

The metrics are logged in a structured JSON format for easy integration
with log analysis tools and dashboards.

Dependencies:
    - utils.logger: For structured JSON logging
"""

from utils import logger

class APIMetrics:
    """Track API usage metrics"""
    def __init__(self):
        self.total_requests:int = 0
        self.results_returned:int = 0
        self.unique_results:int = 0
        
    def log_metrics(self):
        """Log current API metrics"""
        logger.info("API Metrics Summary", extra={
            "metrics": {
                "total_requests": self.total_requests,
                "results_returned": self.results_returned,
                "unique_results": self.unique_results,
                "efficiency_ratio": round(self.unique_results / max(1, self.results_returned), 2)
            }
        })


api_metrics = APIMetrics()