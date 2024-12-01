package aws_sqs

import (
	"context"
	"log/slog"
	"os"
	"sync"

	"github.com/IliaW/url-validator/config"
	awsCfg "github.com/aws/aws-sdk-go-v2/config"
	crd "github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type SQSWorker struct {
	client      *sqs.Client
	url         *string
	getSqsChan  chan<- *string
	sendSqsChan <-chan *string
	cfg         *config.SQSConfig
	log         *slog.Logger
	wg          *sync.WaitGroup
}

func NewSQSWorker(getSqsChan chan<- *string, sendSqsChan <-chan *string, cfg *config.SQSConfig,
	log *slog.Logger, wg *sync.WaitGroup) *SQSWorker {
	log.Info("connecting to sqs...")
	ctx := context.Background()

	sqsConfig, err := awsCfg.LoadDefaultConfig(ctx,
		awsCfg.WithCredentialsProvider(crd.NewStaticCredentialsProvider(cfg.AwsAccessKey, cfg.AwsSecretKey, "")),
		awsCfg.WithRegion(cfg.Region),
		awsCfg.WithBaseEndpoint(cfg.AwsBaseEndpoint))
	if err != nil {
		log.Error("failed to load sqs config.", slog.String("err", err.Error()))
		os.Exit(1)
	}

	client := sqs.NewFromConfig(sqsConfig)
	queueUrl, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{QueueName: &cfg.QueueName})
	if err != nil {
		log.Error("failed to get queue url.", slog.String("err", err.Error()),
			slog.String("queue_name", cfg.QueueName))
		os.Exit(1)
	}

	return &SQSWorker{
		client:      client,
		url:         queueUrl.QueueUrl,
		getSqsChan:  getSqsChan,
		sendSqsChan: sendSqsChan,
		cfg:         cfg,
		log:         log,
		wg:          wg,
	}
}

func (w *SQSWorker) SQSConsumer(ctx context.Context) {
	defer w.wg.Done()
	w.log.Info("starting sqs consumer...", slog.String("queue_url", *w.url))

	getInput := &sqs.ReceiveMessageInput{
		QueueUrl:            w.url,
		MaxNumberOfMessages: w.cfg.MaxNumberOfMessages,
		WaitTimeSeconds:     w.cfg.WaitTimeSeconds,
		VisibilityTimeout:   w.cfg.VisibilityTimeout,
	}
	deleteInput := &sqs.DeleteMessageBatchInput{
		QueueUrl: w.url,
		Entries:  nil,
	}

	for {
		select {
		case <-ctx.Done():
			w.log.Info("stopping sqs consumer...")
			close(w.getSqsChan)
			w.log.Info("close getSqsChan.")
			return
		default:
			output, err := w.client.ReceiveMessage(ctx, getInput)
			if err != nil {
				w.log.Error("failed to receive message from sqs.", slog.String("err", err.Error()))
				continue
			}
			if len(output.Messages) == 0 {
				w.log.Debug("no messages received from sqs.")
				continue
			}

			// Sending messages to getSqsChan
			entries := make([]types.DeleteMessageBatchRequestEntry, 0, len(output.Messages))
			for _, m := range output.Messages {
				w.getSqsChan <- m.Body
				entries = append(entries, types.DeleteMessageBatchRequestEntry{
					Id:            m.MessageId,
					ReceiptHandle: m.ReceiptHandle,
				})
			}
			// Deleting messages from sqs
			deleteInput.Entries = entries
			w.log.Debug("deleting messages from sqs.", slog.Int("size", len(entries)))
			_, err = w.client.DeleteMessageBatch(context.Background(), deleteInput)
			if err != nil {
				w.log.Error("failed to delete messages from sqs.", slog.String("err", err.Error()))
			}
		}
	}
}

func (w *SQSWorker) SQSProducer() {
	defer w.wg.Done()
	w.log.Info("starting sqs producer...", slog.String("queue_url", *w.url))

	ctx := context.Background()
	sendMessage := &sqs.SendMessageInput{
		QueueUrl:    w.url,
		MessageBody: nil,
	}

	// IMPORTANT: If the threshold is reached in Memcached and another message is added with the same URL
	// to empty queue, you will be stuck in a loop of getSqsMessage -> error: threshold reached -> sendSqsMessage
	// until the cache times out.
	for m := range w.sendSqsChan {
		sendMessage.MessageBody = m
		w.log.Debug("sending message back to sqs.", slog.String("message", *m))
		_, err := w.client.SendMessage(ctx, sendMessage)
		if err != nil {
			w.log.Error("failed to send message to sqs.", slog.String("message", *m),
				slog.String("err", err.Error()))
		}
	}
	w.log.Info("stopping sqs producer.")
}
