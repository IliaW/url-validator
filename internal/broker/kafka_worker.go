package broker

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/IliaW/url-validator/config"
	"github.com/IliaW/url-validator/internal/model"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/compress/lz4"
)

func KafkaProducer(kafkaChan <-chan *model.ScrapeTask, cfg *config.KafkaProducerConfig, log *slog.Logger,
	wg *sync.WaitGroup) {
	defer wg.Done()
	log.Info("starting kafka producer...", slog.String("topic", cfg.WriteTopicName))

	w := kafka.Writer{
		Addr:         kafka.TCP(strings.Split(cfg.Addr, ",")...),
		Topic:        cfg.WriteTopicName,
		Balancer:     &kafka.Hash{},
		MaxAttempts:  cfg.MaxAttempts,
		BatchSize:    1,                // the parameter is controlled by batch variable
		BatchTimeout: time.Millisecond, // the parameter is controlled by batchTicker variable
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		RequiredAcks: kafka.RequiredAcks(cfg.RequiredAsks),
		Async:        cfg.Async,
		Completion: func(messages []kafka.Message, err error) {
			if err != nil {
				log.Error("failed to send messages to kafka.", slog.String("err", err.Error()))
			}
		},
		Compression: kafka.Compression(new(lz4.Codec).Code()),
	}
	defer func() {
		err := w.Close()
		if err != nil {
			log.Error("failed to close kafka writer.", slog.String("err", err.Error()))
		}
	}()

	batchTicker := time.Tick(cfg.BatchTimeout)
	batch := make([]kafka.Message, 0, cfg.BatchSize)
	writeMessage := func(batch []kafka.Message) {
		ctx, cancel := context.WithTimeout(context.Background(), cfg.WriteTimeout)
		defer cancel()
		err := w.WriteMessages(ctx, batch...)
		if err != nil {
			log.Error("failed to send messages to kafka.", slog.String("err", err.Error()))
			return
		}
		log.Debug("successfully sent messages to kafka.", slog.Int("batch length", len(batch)))
	}

	for scrapeTask := range kafkaChan {
		body, err := json.Marshal(scrapeTask)
		if err != nil {
			log.Error("marshaling error.", slog.String("err", err.Error()),
				slog.Any("scrapeTask", scrapeTask))
			continue
		}
		batch = append(batch, kafka.Message{
			Key:   []byte(scrapeTask.URL),
			Value: body,
		})
		select {
		case <-batchTicker:
			writeMessage(batch)
			batch = make([]kafka.Message, 0, cfg.BatchSize)
		default:
			if len(batch) >= cfg.BatchSize {
				writeMessage(batch)
				batch = make([]kafka.Message, 0, cfg.BatchSize)
			}
		}
	}
	// Some messages may remain in the batch after kafkaChan is closed
	if len(batch) > 0 {
		log.Debug("messages in batch.", slog.Int("count", len(batch)))
		writeMessage(batch)
	}
	log.Info("stopping kafka writer.")
}
