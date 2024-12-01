package model

import "time"

type Scrape struct {
	FullURL   string    `json:"full_url"`
	ETag      string    `json:"etag,omitempty"`
	CreatedAt time.Time `json:"timestamp"`
}

type ScrapeTask struct {
	URL               string `json:"url"`
	IsAllowedToScrape bool   `json:"allowed_to_scrape"`
}
