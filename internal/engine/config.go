package engine

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"time"
)

type Config struct {
	DSN, Address                                                                                    string
	Concurrency, ExecutionSlots, PoolSize, MaxExecutions, MaxRecords                                int
	MaxBodyBytes, MaxValueBytes, MaxExecutionBytes, MaxNodes, MaxEdges                              int
	MaxTries, RequestsPerSecond, RequestConcurrency                                                 int
	HandlerTimeout, ExecutionTimeout, PendingTimeout, Retention, PollInterval, DBTimeout, RetryBase time.Duration
	TestFixtures                                                                                    bool
}

func DefaultConfig() Config {
	return Config{DSN: "postgres://workflow:workflow@localhost:29432/workflow?sslmode=disable", Address: ":8000", Concurrency: 4, ExecutionSlots: 8, PoolSize: 8, MaxExecutions: 100, MaxRecords: 1000, MaxBodyBytes: 1 << 20, MaxValueBytes: 256 << 10, MaxExecutionBytes: 8 << 20, MaxNodes: 500, MaxEdges: 2000, MaxTries: 3, RequestsPerSecond: 10, RequestConcurrency: 64, HandlerTimeout: 10 * time.Second, ExecutionTimeout: 300 * time.Second, PendingTimeout: 24 * time.Hour, Retention: 24 * time.Hour, PollInterval: 25 * time.Millisecond, DBTimeout: 5 * time.Second, RetryBase: time.Second}
}

func ConfigFromEnv() (Config, error) {
	c := DefaultConfig()
	if s := os.Getenv("WF_POSTGRES_DSN"); s != "" {
		c.DSN = s
	}
	if s := os.Getenv("WF_ADDRESS"); s != "" {
		c.Address = s
	}
	ints := map[string]*int{"WF_WORKER_CONCURRENCY": &c.Concurrency, "WF_EXECUTION_SLOTS": &c.ExecutionSlots, "WF_POOL_SIZE": &c.PoolSize, "WF_MAX_EXECUTIONS": &c.MaxExecutions, "WF_MAX_RECORDS": &c.MaxRecords, "WF_MAX_BODY_BYTES": &c.MaxBodyBytes, "WF_MAX_VALUE_BYTES": &c.MaxValueBytes, "WF_MAX_EXECUTION_BYTES": &c.MaxExecutionBytes, "WF_MAX_STARTS": &c.MaxTries, "WF_REQUESTS_PER_SECOND": &c.RequestsPerSecond, "WF_REQUEST_CONCURRENCY": &c.RequestConcurrency}
	for name, p := range ints {
		if s := os.Getenv(name); s != "" {
			v, e := strconv.Atoi(s)
			if e != nil || v < 1 {
				return c, fmt.Errorf("%s must be positive", name)
			}
			*p = v
		}
	}
	durations := map[string]*time.Duration{"WF_HANDLER_TIMEOUT": &c.HandlerTimeout, "WF_EXECUTION_TIMEOUT": &c.ExecutionTimeout, "WF_PENDING_SECONDS": &c.PendingTimeout, "WF_RETENTION_SECONDS": &c.Retention, "WF_RETRY_BASE": &c.RetryBase, "WF_DB_TIMEOUT": &c.DBTimeout}
	for name, p := range durations {
		if s := os.Getenv(name); s != "" {
			v, e := strconv.ParseFloat(s, 64)
			if e != nil || v <= 0 || math.IsNaN(v) || math.IsInf(v, 0) || v > 365*86400 {
				return c, fmt.Errorf("%s must be positive", name)
			}
			*p = time.Duration(v * float64(time.Second))
		}
	}
	c.TestFixtures = os.Getenv("WF_ENABLE_TEST_FIXTURES") == "1"
	if c.HandlerTimeout > c.ExecutionTimeout || c.MaxValueBytes > c.MaxExecutionBytes || c.Concurrency > 4096 || c.PoolSize < 2 || c.PoolSize > 128 || c.MaxTries > 100 || c.RequestConcurrency > 4096 {
		return c, fmt.Errorf("inconsistent resource limits")
	}
	return c, nil
}
