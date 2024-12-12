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

type KafkaProducerClient struct {
	kafkaChan <-chan *model.ScrapeTask
	cfg       *config.KafkaProducerConfig
	log       *slog.Logger
	wg        *sync.WaitGroup
}

func NewKafkaProducer(kafkaChan <-chan *model.ScrapeTask, cfg *config.KafkaProducerConfig, log *slog.Logger,
	wg *sync.WaitGroup) *KafkaProducerClient {
	return &KafkaProducerClient{
		kafkaChan: kafkaChan,
		cfg:       cfg,
		log:       log,
		wg:        wg,
	}
}

func (k *KafkaProducerClient) Run() {
	defer k.wg.Done()
	k.log.Info("starting kafka producer...", slog.String("topic", k.cfg.WriteTopicName))

	w := kafka.Writer{
		Addr:         kafka.TCP(strings.Split(k.cfg.Addr, ",")...),
		Topic:        k.cfg.WriteTopicName,
		Balancer:     &kafka.Hash{},
		MaxAttempts:  k.cfg.MaxAttempts,
		BatchSize:    1,                // the parameter is controlled by batch variable
		BatchTimeout: time.Millisecond, // the parameter is controlled by batchTicker variable
		ReadTimeout:  k.cfg.ReadTimeout,
		WriteTimeout: k.cfg.WriteTimeout,
		RequiredAcks: kafka.RequiredAcks(k.cfg.RequiredAsks),
		Async:        k.cfg.Async,
		Completion: func(messages []kafka.Message, err error) {
			if err != nil {
				k.log.Error("failed to send messages to kafka.", slog.String("err", err.Error()))
			}
		},
		Compression: kafka.Compression(new(lz4.Codec).Code()),
	}
	defer func() {
		err := w.Close()
		if err != nil {
			k.log.Error("failed to close kafka writer.", slog.String("err", err.Error()))
		}
	}()

	batchTicker := time.Tick(k.cfg.BatchTimeout)
	batch := make([]kafka.Message, 0, k.cfg.BatchSize)
	writeMessage := func(batch []kafka.Message) {
		ctx, cancel := context.WithTimeout(context.Background(), k.cfg.WriteTimeout)
		defer cancel()
		err := w.WriteMessages(ctx, batch...)
		if err != nil {
			k.log.Error("failed to send messages to kafka.", slog.String("err", err.Error()))
			return
		}
		k.log.Debug("successfully sent messages to kafka.", slog.Int("batch length", len(batch)))
	}

	for scrapeTask := range k.kafkaChan {
		body, err := json.Marshal(scrapeTask)
		if err != nil {
			k.log.Error("marshaling error.", slog.String("err", err.Error()),
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
			batch = batch[:0]
		default:
			if len(batch) >= k.cfg.BatchSize {
				writeMessage(batch)
				batch = batch[:0]
			}
		}
	}
	// Some messages may remain in the batch after kafkaChan is closed
	if len(batch) > 0 {
		k.log.Debug("messages in batch.", slog.Int("count", len(batch)))
		writeMessage(batch)
	}
	k.log.Info("stopping kafka writer.")
}
