# url-validator

## Overview
The `url-validator` is doing pre-scraping checks by performing the following tasks:

1. Reads from Scrape Work SQS Queue: Processes messages containing URLs for validation.
2. Checks Cache for Recent Scrape: Uses a 24-hour expiring cache to avoid redundant scrapes. If found, stops further processing.
3. Validates URL in Metadata: Sends conditional requests (using If-None-Match with ETag) to check for content changes. Skips rescraping if content is unchanged.
4. Queries Robots.txt Service: Retrieves domain scraping rules, including any custom rules for blocking or rate limiting.
5. Manages Domain Rate Limits: Checks a 1-minute expiring cache for active scrapers on the domain. Reschedules if the threshold is exceeded; increments the counter if allowed.
6. Writes to Scrape Work Kafka Topic: Outputs messages with the URL and scrape permissions for further processing.