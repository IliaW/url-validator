package persistence

import (
	"database/sql"
	"log/slog"

	"github.com/IliaW/url-validator/internal/model"
)

type MetadataStorage interface {
	GetLastScrape(string) *model.Scrape
}

type MetadataRepository struct {
	db  *sql.DB
	log *slog.Logger
}

func NewMetadataRepository(db *sql.DB, log *slog.Logger) *MetadataRepository {
	return &MetadataRepository{db: db, log: log}
}

// GetLastScrape returns the last created scrape metadata for the given URL.
func (mr *MetadataRepository) GetLastScrape(url string) *model.Scrape {
	var scrapes []*model.Scrape
	rows, err := mr.db.Query("SELECT url, e_tag FROM scrape_metadata WHERE url = ? ORDER BY timestamp DESC LIMIT 1", url) // get by last timestamp
	if err != nil {
		mr.log.Error("failed to get scrape metadata from database.", slog.String("err", err.Error()))
		return nil
	}
	defer func(rows *sql.Rows) {
		err = rows.Close()
		if err != nil {
			mr.log.Error("failed to close rows.", slog.String("err", err.Error()))
		}
	}(rows)

	for rows.Next() {
		var scrape model.Scrape
		if err = rows.Scan(&scrape.FullURL, &scrape.ETag); err != nil {
			mr.log.Error("failed to scan scrape metadata from database.", slog.String("err", err.Error()))
			return nil
		}
		scrapes = append(scrapes, &scrape)
	}

	if err = rows.Err(); err != nil {
		mr.log.Error("failed to get scrape metadata from database.", slog.String("err", err.Error()))
		return nil
	}
	if len(scrapes) == 0 {
		mr.log.Debug("no scrape metadata found for the given URL.", slog.String("url", url))
		return nil
	}
	mr.log.Debug("scrapes found.", slog.Any("size", len(scrapes)))
	return scrapes[0]
}
