package config

import (
	"log/slog"
	"os"
	"path"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Env                string            `mapstructure:"env"`
	LogLevel           string            `mapstructure:"log_level"`
	LogType            string            `mapstructure:"log_type"`
	ServiceName        string            `mapstructure:"service_name"`
	Port               string            `mapstructure:"port"`
	Version            string            `mapstructure:"version"`
	GetSqsChanSize     int               `mapstructure:"get_sqs_chan_size"`
	SendSqsChanSize    int               `mapstructure:"send_sqs_chan_size"`
	KafkaChanSize      int               `mapstructure:"kafka_chan_size"`
	RestartTimeout     time.Duration     `mapstructure:"restart_timeout"`
	WorkerSettings     *WorkerConfig     `mapstructure:"worker"`
	RobotsSettings     *RobotsConfig     `mapstructure:"robots"`
	HttpClientSettings *HttpClientConfig `mapstructure:"http_client"`
	CacheSettings      *CacheConfig      `mapstructure:"cache"`
	DbSettings         *DatabaseConfig   `mapstructure:"database"`
	SQSSettings        *SQSConfig        `mapstructure:"sqs"`
	KafkaSettings      *KafkaConfig      `mapstructure:"kafka"`
}

type WorkerConfig struct {
	WorkersLimit int    `mapstructure:"workers_limit"`
	UserAgent    string `mapstructure:"user_agent"`
}

type RobotsConfig struct {
	FullURL string `mapstructure:"full_url"`
}

type HttpClientConfig struct {
	RequestTimeout            time.Duration `mapstructure:"request_timeout"`
	MaxIdleConnections        int           `mapstructure:"max_idle_connections"`
	MaxIdleConnectionsPerHost int           `mapstructure:"max_idle_connections_per_host"`
	IdleConnectionTimeout     time.Duration `mapstructure:"idle_connection_timeout"`
	TlsHandshakeTimeout       time.Duration `mapstructure:"tls_handshake_timeout"`
	DialTimeout               time.Duration `mapstructure:"dial_timeout"`
	DialKeepAlive             time.Duration `mapstructure:"dial_keep_alive"`
	TlsInsecureSkipVerify     bool          `mapstructure:"tls_insecure_skip_verify"`
}

type CacheConfig struct {
	Servers         string        `mapstructure:"servers"`
	Threshold       uint64        `mapstructure:"threshold"`
	TtlForThreshold time.Duration `mapstructure:"ttl_for_threshold"`
}

type DatabaseConfig struct {
	Host            string        `mapstructure:"host"`
	Port            string        `mapstructure:"port"`
	User            string        `mapstructure:"user"`
	Password        string        `mapstructure:"password"`
	Name            string        `mapstructure:"name"`
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`
	MaxOpenConns    int           `mapstructure:"max_open_conns"`
	MaxIdleConns    int           `mapstructure:"max_idle_conns"`
}

type SQSConfig struct {
	AwsAccessKey        string `mapstructure:"aws_access_key"`
	AwsSecretKey        string `mapstructure:"aws_secret_key"`
	AwsSessionToken     string `mapstructure:"aws_session_token"`
	AwsBaseEndpoint     string `mapstructure:"aws_base_endpoint"`
	RoleArn             string `mapstructure:"role_arn"`
	RoleSessionName     string `mapstructure:"role_session_name"`
	Region              string `mapstructure:"region"`
	QueueName           string `mapstructure:"queue_name"`
	MaxNumberOfMessages int32  `mapstructure:"max_number_of_messages"`
	WaitTimeSeconds     int32  `mapstructure:"wait_time_seconds"`
	VisibilityTimeout   int32  `mapstructure:"visibility_timeout"`
}

type KafkaConfig struct {
	Producer *KafkaProducerConfig `mapstructure:"producer"`
}

type KafkaProducerConfig struct {
	Addr           string        `mapstructure:"addr"`
	WriteTopicName string        `mapstructure:"write_topic_name"`
	MaxAttempts    int           `mapstructure:"max_attempts"`
	BatchSize      int           `mapstructure:"batch_size"`
	BatchTimeout   time.Duration `mapstructure:"batch_timeout"`
	ReadTimeout    time.Duration `mapstructure:"read_timeout"`
	WriteTimeout   time.Duration `mapstructure:"write_timeout"`
	RequiredAsks   int           `mapstructure:"required_acks"`
	Async          bool          `mapstructure:"async"`
}

func MustLoad() *Config {
	viper.AddConfigPath(path.Join("."))
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AutomaticEnv()

	err := viper.ReadInConfig()
	if err != nil {
		slog.Error("can't initialize config file.", slog.String("err", err.Error()))
		os.Exit(1)
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		slog.Error("error unmarshalling viper config.", slog.String("err", err.Error()))
		os.Exit(1)
	}

	return &cfg
}
