package worker

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	"github.com/IliaW/url-validator/config"
	"github.com/IliaW/url-validator/internal/cache"
	"github.com/IliaW/url-validator/internal/model"
	"github.com/IliaW/url-validator/internal/persistence"
)

type ValidationWorker struct {
	InputSqsChan    <-chan *string
	OutputSqsChan   chan<- *string
	OutputKafkaChan chan<- *model.ScrapeTask
	PanicChan       chan struct{}
	HttpClient      *http.Client
	Cfg             *config.Config
	Log             *slog.Logger
	Db              persistence.MetadataStorage
	Cache           cache.CachedClient
	Wg              *sync.WaitGroup
}

func (w *ValidationWorker) Run() {
	defer func() {
		if r := recover(); r != nil {
			w.Log.Error("PANIC!", slog.Any("err", r))
			w.PanicChan <- struct{}{}
		}
	}()
	defer w.Wg.Done()
	w.Log.Debug("starting validation worker...")
	substr := "http"

	for str := range w.InputSqsChan {
		// Extracting url. Expected string format: {"url": "https://example.com/page"}
		// Benchmarks show that the way is 100 times faster than Unmarshal
		index := strings.Index(*str, substr)
		if index == -1 {
			w.Log.Error("url not found.", slog.String("input", *str))
			continue
		}
		url := (*str)[index : len(*str)-1]
		w.Log.Debug("url extracted.", slog.String("url", url))

		// Check memcached if the url has been scraped (Scrape TTL: 24h. Key: url-hash)
		if w.Cache.CheckIfScraped(url) {
			continue
		}

		// Check whether the scrape in the database and etag has not changed
		if scrape := w.Db.GetLastScrape(url); scrape != nil && scrape.ETag != "" {
			req, err := http.NewRequest("GET", url, nil)
			if err != nil {
				w.Log.Error("failed to create a request. Skip etag verification. ", slog.String("url", url),
					slog.String("err", err.Error()))
				goto SkipEtagVerification
			}
			req.Header.Set("If-None-Match", scrape.ETag)

			getStatusCode := func() int {
				resp, err := w.HttpClient.Do(req)
				if err != nil {
					w.Log.Error("failed to make a request to the url.", slog.String("url", url),
						slog.String("err", err.Error()))
				}
				defer func(Body io.ReadCloser) {
					if err = Body.Close(); err != nil {
						w.Log.Warn("failed to close the response body.", slog.String("err", err.Error()))
					}
				}(resp.Body)

				return resp.StatusCode
			}

			// If 304 Not Modified, skip the scraping for the url
			if getStatusCode() == http.StatusNotModified {
				w.Log.Debug("etag has not changed. Skip scraping.", slog.String("url", url))
				continue
			}
		}
	SkipEtagVerification:
		// Request to Robots.txt service
		robotUrl := fmt.Sprintf(w.Cfg.RobotsSettings.FullURL, url)
		robotReq, err := http.NewRequest("GET", robotUrl, nil)
		if err != nil {
			w.Log.Error("failed to create a request to the robots.txt service.",
				slog.String("robotUrl", robotUrl))
		}

		// TODO: the logic may change. Robots.txt service not implemented yet
		// It will return false and will not allow scraping if an error happens
		isAllowedToScrape := func() bool {
			resp, err := w.HttpClient.Do(robotReq)
			if err != nil {
				w.Log.Error("failed to make a request to the robots service.", slog.String("robotUrl", robotUrl),
					slog.String("err", err.Error()))
				return false
			}
			defer func(Body io.ReadCloser) {
				if err = Body.Close(); err != nil {
					w.Log.Warn("failed to close the response body.", slog.String("err", err.Error()))
				}
			}(resp.Body)

			var isAllowed bool
			if err = json.NewDecoder(resp.Body).Decode(&isAllowed); err != nil {
				w.Log.Error("failed to decode the response body.", slog.String("err", err.Error()))
				return false
			}

			return isAllowed
		}

		scrapeTask := &model.ScrapeTask{
			URL:               url,
			IsAllowedToScrape: isAllowedToScrape(),
		}

		// Increment the threshold for the domain
		if err = w.Cache.IncrementThreshold(url); err != nil {
			// If the threshold is reached or an error - put the url back to the sqs
			w.OutputSqsChan <- str
			continue
		}

		w.OutputKafkaChan <- scrapeTask
	}
}
