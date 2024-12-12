package worker

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
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

	for str := range w.InputSqsChan {
		// Extracting url. Expected string format: {"url": "https://example.com/page"}
		var task model.ScrapeTask
		if err := json.Unmarshal([]byte(*str), &task); err != nil {
			w.Log.Error("failed to unmarshal the url.", slog.String("url", *str),
				slog.String("err", err.Error()))
			continue
		}

		// Check memcached if the url has been scraped (Scrape TTL: 24h. Key: url-hash)
		if w.Cache.CheckIfScraped(task.URL) {
			continue
		}

		// Check whether the scrape in the database and etag has not changed
		if scrape := w.Db.GetLastScrape(task.URL); scrape != nil && scrape.ETag != "" {
			req, err := http.NewRequest("GET", task.URL, nil)
			if err != nil {
				w.Log.Error("failed to create a request. Skip etag verification. ", slog.String("url", task.URL),
					slog.String("err", err.Error()))
				goto SkipEtagVerification
			}
			req.Header.Set("If-None-Match", scrape.ETag)

			getStatusCode := func() int {
				resp, err := w.HttpClient.Do(req)
				if err != nil {
					w.Log.Error("failed to make a request to the url.", slog.String("url", task.URL),
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
				w.Log.Debug("etag has not changed. Skip scraping.", slog.String("url", task.URL))
				continue
			}
		}
	SkipEtagVerification:
		// Request to Robots.txt service
		robotUrl := fmt.Sprintf(w.Cfg.RobotsSettings.FullURL, task.URL, w.Cfg.WorkerSettings.UserAgent)
		robotReq, err := http.NewRequest("GET", robotUrl, nil)
		if err != nil {
			w.Log.Error("failed to create a request to the robots.txt service.",
				slog.String("robotUrl", robotUrl))
			// TODO: what to do?
		}

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

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				w.Log.Error("failed to read the response body.", slog.String("err", err.Error()))
				return false
			}
			var isAllowed bool
			if err = json.Unmarshal(body, &isAllowed); err != nil {
				w.Log.Error("failed to unmarshal the response body.", slog.String("body", string(body)))
				return false
			}

			return isAllowed
		}

		task.IsAllowedToScrape = isAllowedToScrape()

		// Increment the threshold for the domain
		if err = w.Cache.IncrementThreshold(task.URL); err != nil {
			// If the threshold is reached or an error - put the url back to the sqs
			w.OutputSqsChan <- str
			continue
		}

		w.OutputKafkaChan <- &task
	}
}
