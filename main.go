package main

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IliaW/url-validator/config"
	"github.com/IliaW/url-validator/internal/aws_sqs"
	"github.com/IliaW/url-validator/internal/broker"
	cacheClient "github.com/IliaW/url-validator/internal/cache"
	"github.com/IliaW/url-validator/internal/model"
	"github.com/IliaW/url-validator/internal/persistence"
	"github.com/IliaW/url-validator/internal/worker"
	"github.com/go-sql-driver/mysql"
	"github.com/lmittmann/tint"
)

var (
	cfg          *config.Config
	log          *slog.Logger
	db           *sql.DB
	cache        cacheClient.CachedClient
	metadataRepo persistence.MetadataStorage
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg = config.MustLoad()
	log = setupLogger()
	db = setupDatabase()
	defer closeDatabase()
	metadataRepo = persistence.NewMetadataRepository(db, log)
	cache = cacheClient.NewMemcachedClient(cfg.CacheSettings, log)
	defer cache.Close()
	httpClient := setupHttpClient()
	log.Info("starting application on port "+cfg.Port, slog.String("env", cfg.Env))

	getSqsChan := make(chan *string, 100)
	sendSqsChan := make(chan *string, 10)
	kafkaChan := make(chan *model.ScrapeTask, 100)
	panicChan := make(chan struct{}, cfg.WorkerSettings.MaxWorkers)

	wg := &sync.WaitGroup{}
	wg.Add(2)
	sqs := aws_sqs.NewSQSWorker(getSqsChan, sendSqsChan, cfg.SQSSettings, log, wg)
	go sqs.SQSConsumer(ctx)
	go sqs.SQSProducer()

	workerWg := &sync.WaitGroup{}
	validationWorker := &worker.ValidationWorker{
		InputSqsChan:    getSqsChan,
		OutputSqsChan:   sendSqsChan,
		OutputKafkaChan: kafkaChan,
		PanicChan:       panicChan,
		HttpClient:      httpClient,
		Cfg:             cfg,
		Log:             log,
		Db:              metadataRepo,
		Cache:           cache,
		Wg:              workerWg,
	}
	for i := 0; i < cfg.WorkerSettings.MaxWorkers; i++ {
		workerWg.Add(1)
		go validationWorker.Run()
	}
	// Restart workers if they panic.
	go func() {
		for range panicChan {
			workerWg.Add(1)
			go validationWorker.Run()
			time.Sleep(3 * time.Minute) // timeout to avoid polluting logs if something unrecoverable happened
		}
	}()

	wg.Add(1)
	go broker.KafkaProducer(kafkaChan, cfg.KafkaSettings.Producer, log, wg)

	// Graceful shutdown.
	// 1. Stop SQS Consumer by system call.
	// 2. Close getSqsChan
	// 3. Wait till all Workers processed all messages from getSqsChan
	// 4. Close sendSqsChan, kafkaChan, and panicChan
	// 5. Wait till SQS Producer and Kafka Producer process all messages.
	// 6. Close database and memcached connections
	<-ctx.Done()
	log.Info("stopping server...")
	workerWg.Wait()
	close(sendSqsChan)
	log.Info("close sendSqsChan.")
	close(panicChan)
	log.Info("close panicChan.")
	close(kafkaChan)
	log.Info("close kafkaChan.")
	wg.Wait()
}

func setupLogger() *slog.Logger {
	resolvedLogLevel := func() slog.Level {
		envLogLevel := strings.ToLower(cfg.LogLevel)
		switch envLogLevel {
		case "info":
			return slog.LevelInfo
		case "error":
			return slog.LevelError
		default:
			return slog.LevelDebug
		}
	}

	replaceAttrs := func(groups []string, a slog.Attr) slog.Attr {
		if a.Key == slog.SourceKey {
			source := a.Value.Any().(*slog.Source)
			source.File = filepath.Base(source.File)
		}
		return a
	}

	var logger *slog.Logger
	if strings.ToLower(cfg.LogType) == "json" {
		logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			AddSource:   true,
			Level:       resolvedLogLevel(),
			ReplaceAttr: replaceAttrs}))
	} else {
		logger = slog.New(tint.NewHandler(os.Stdout, &tint.Options{
			AddSource:   true,
			Level:       resolvedLogLevel(),
			ReplaceAttr: replaceAttrs,
			NoColor:     false}))
	}

	slog.SetDefault(logger)
	logger.Debug("debug messages are enabled.")

	return logger
}

func setupDatabase() *sql.DB {
	log.Info("connecting to the database...")
	sqlCfg := mysql.Config{
		User:                 cfg.DbSettings.User,
		Passwd:               cfg.DbSettings.Password,
		Net:                  "tcp",
		Addr:                 fmt.Sprintf("%s:%s", cfg.DbSettings.Host, cfg.DbSettings.Port),
		DBName:               cfg.DbSettings.Name,
		AllowNativePasswords: true,
		ParseTime:            true,
	}
	database, err := sql.Open("mysql", sqlCfg.FormatDSN())
	if err != nil {
		log.Error("failed to establish database connection.", slog.String("err", err.Error()))
		os.Exit(1)
	}
	database.SetConnMaxLifetime(cfg.DbSettings.ConnMaxLifetime)
	database.SetMaxOpenConns(cfg.DbSettings.MaxOpenConns)
	database.SetMaxIdleConns(cfg.DbSettings.MaxIdleConns)

	maxRetry := 6
	for i := 1; i <= maxRetry; i++ {
		log.Info("ping the database.", slog.String("attempt", fmt.Sprintf("%d/%d", i, maxRetry)))
		pingErr := database.Ping()
		if pingErr != nil {
			log.Error("not responding.", slog.String("err", pingErr.Error()))
			if i == maxRetry {
				log.Error("failed to establish database connection.")
				os.Exit(1)
			}
			log.Info(fmt.Sprintf("wait %d seconds", 5*i))
			time.Sleep(time.Duration(5*i) * time.Second)
		} else {
			break
		}
	}
	log.Info("connected to the database!")

	return database
}

func closeDatabase() {
	log.Info("closing database connection.")
	err := db.Close()
	if err != nil {
		log.Error("failed to close database connection.", slog.String("err", err.Error()))
	}
}

func setupHttpClient() *http.Client {
	transport := &http.Transport{
		MaxIdleConns:        cfg.HttpClientSettings.MaxIdleConnections,
		MaxConnsPerHost:     cfg.HttpClientSettings.MaxIdleConnectionsPerHost,
		IdleConnTimeout:     cfg.HttpClientSettings.IdleConnectionTimeout,
		TLSHandshakeTimeout: cfg.HttpClientSettings.TlsHandshakeTimeout,
		DialContext: (&net.Dialer{
			Timeout:   cfg.HttpClientSettings.DialTimeout,
			KeepAlive: cfg.HttpClientSettings.DialKeepAlive,
		}).DialContext,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: cfg.HttpClientSettings.TlsInsecureSkipVerify,
		},
	}

	return &http.Client{
		Transport: transport,
		Timeout:   cfg.HttpClientSettings.RequestTimeout,
	}
}
