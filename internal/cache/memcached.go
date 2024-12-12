package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	netUrl "net/url"
	"os"
	"strings"
	"sync"

	"github.com/IliaW/url-validator/config"
	"github.com/bradfitz/gomemcache/memcache"
)

var (
	ThresholdReachedError = errors.New("threshold reached")
)

type CachedClient interface {
	CheckIfScraped(string) bool
	IncrementThreshold(string) error
	Close()
}

type MemcachedClient struct {
	client *memcache.Client
	cfg    *config.CacheConfig
	log    *slog.Logger
	mu     sync.Mutex
}

func NewMemcachedClient(cacheConfig *config.CacheConfig, log *slog.Logger) *MemcachedClient {
	log.Info("connecting to memcached...")
	ss := new(memcache.ServerList)
	servers := strings.Split(cacheConfig.Servers, ",")
	err := ss.SetServers(servers...)
	if err != nil {
		log.Error("failed to set memcached servers.", slog.String("err", err.Error()))
		os.Exit(1)
	}
	c := &MemcachedClient{
		client: memcache.NewFromSelector(ss),
		cfg:    cacheConfig,
		log:    log,
	}
	c.log.Info("pinging the memcached.")
	err = c.client.Ping()
	if err != nil {
		log.Error("connection to the memcached is failed.", slog.String("err", err.Error()))
		os.Exit(1)
	}
	c.log.Info("connected to memcached!")

	return c
}

func (mc *MemcachedClient) CheckIfScraped(url string) bool {
	key := hashURL(url)
	it, err := mc.client.Get(key)
	if err != nil {
		if errors.Is(err, memcache.ErrCacheMiss) {
			mc.log.Debug("cache not found.", slog.String("key", key))
			return false
		} else {
			mc.log.Error("failed to check if scraped.", slog.String("key", key),
				slog.String("err", err.Error()))
			return false
		}
	}
	if string(it.Value) == "" {
		mc.log.Warn("cache found but the value is empty.", slog.String("key", key),
			slog.String("value", string(it.Value)))
		return false
	}

	return true
}

func (mc *MemcachedClient) IncrementThreshold(url string) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.log.Debug("increment the threshold.")
	key := mc.generateDomainHash(url)
	value, err := mc.client.Increment(key, 0) // returns current value
	if err != nil {
		if errors.Is(err, memcache.ErrCacheMiss) {
			mc.log.Debug("cache not found. Creating new threshold.", slog.String("url", url))
			err = mc.set(key, 1, int32((mc.cfg.TtlForThreshold).Seconds()))
			if err != nil {
				mc.log.Error("failed to create new counter for the domain.", slog.String("url", url),
					slog.String("err", err.Error()))
				return err
			}
			mc.log.Debug("new counter is created.", slog.String("url", url), slog.String("key", key),
				slog.Uint64("value", 1))
			return nil
		} else {
			mc.log.Error("failed to increment the threshold.", slog.String("url", url),
				slog.String("key", key), slog.String("err", err.Error()))
			return err
		}
	}
	if value > mc.cfg.Threshold {
		mc.log.Info("threshold reached.", slog.String("url", url))
		return ThresholdReachedError
	}
	value, err = mc.client.Increment(key, 1)
	mc.log.Debug("new value is set.", slog.String("key", key), slog.Uint64("value", value))
	return nil
}

func (mc *MemcachedClient) Close() {
	mc.log.Info("closing memcached connection.")
	err := mc.client.Close()
	if err != nil {
		mc.log.Error("failed to close memcached connection.", slog.String("err", err.Error()))
	}
}

func (mc *MemcachedClient) set(key string, value any, expiration int32) error {
	byteValue, err := json.Marshal(value)
	if err != nil {
		mc.log.Error("failed to marshal value.", slog.String("err", err.Error()))
		return err
	}
	item := &memcache.Item{
		Key:        key,
		Value:      byteValue,
		Expiration: expiration,
	}

	return mc.client.Set(item)
}

func (mc *MemcachedClient) generateDomainHash(url string) string {
	u, err := netUrl.Parse(url)
	var key string
	if err != nil {
		mc.log.Error("failed to parse url. Use full url as a key.", slog.String("url", url),
			slog.String("err", err.Error()))
		key = fmt.Sprintf("%s-1m-scrapes", hashURL(url))
	} else {
		key = fmt.Sprintf("%s-1m-scrapes", hashURL(u.Host))
		mc.log.Debug("", slog.String("key:", key))
	}

	return key
}

func hashURL(url string) string {
	hash := sha256.New()
	hash.Write([]byte(url))
	return hex.EncodeToString(hash.Sum(nil))
}
