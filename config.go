package kafkaClient

import (
	"crypto/tls"
	"fmt"
	"net"
	"regexp"
	"time"

	"golang.org/x/net/proxy"
)

const defaultClientID = "mertani"

var validClientID = regexp.MustCompile(`\A[A-Za-z0-9._-]+\z`)

// Config is used to pass multiple configuration options to mertani's constructors.
type Config struct {
	Net struct {
		DialTimeout  time.Duration // How long to wait for the initial connection.
		ReadTimeout  time.Duration // How long to wait for a response.
		WriteTimeout time.Duration // How long to wait for a transmit.

		// ResolveCanonicalBootstrapServers turns each bootstrap broker address
		// into a set of IPs, then does a reverse lookup on each one to get its
		// canonical hostname. This list of hostnames then replaces the
		// original address list. Similar to the `client.dns.lookup` option in
		// the JVM client, this is especially useful with GSSAPI, where it
		// allows providing an alias record instead of individual broker
		// hostnames. Defaults to false.
		ResolveCanonicalBootstrapServers bool

		TLS struct {
			Enable bool
			Config *tls.Config
		}

		KeepAlive time.Duration
		LocalAddr net.Addr
	}

	Metadata struct {
		Retry struct {
			Max         int
			Backoff     time.Duration
			BackoffFunc func(retries, maxRetries int) time.Duration
		}

		RefreshFrequency       time.Duration
		Timeout                time.Duration
		AllowAutoTopicCreation bool
	}

	Producer struct {
		MaxMessageBytes  int
		Timeout          time.Duration
		CompressionLevel int
		Idempotent       bool
		lingerMs         time.Duration

		// Transaction specify
		Transaction struct {
			ID      string
			Timeout time.Duration

			Retry struct {
				Max         int
				Backoff     time.Duration
				BackoffFunc func(retries, maxRetries int) time.Duration
			}
		}
	}

	// Consumer is the namespace for configuration related to consuming messages,
	// used by the Consumer.
	Consumer struct {
		// Group is the namespace for configuring consumer group.
		Group struct {
			Session struct {
				Timeout time.Duration
			}
			Heartbeat struct {
				Interval time.Duration
			}
			Rebalance struct {
				Strategy        BalanceStrategy
				GroupStrategies []BalanceStrategy
				Timeout         time.Duration

				Retry struct {
					Max     int
					Backoff time.Duration
				}
			}
			Member struct {
				UserData []byte
			}
			InstanceId          string
			ResetInvalidOffsets bool
		}

		Retry struct {
			Max         int
			Backoff     time.Duration
			BackoffFunc func(retries int) time.Duration
		}

		Return struct {
			Errors bool
		}

		Fetch struct {
			Min     int32
			Default int32
			Max     int32
		}

		MaxWaitTime       time.Duration
		MaxProcessingTime time.Duration
		IsolationLevel    IsolationLevel
	}

	ClientID           string
	RackID             string
	ChannelBufferSize  int
	ApiVersionsRequest bool
	Version            KafkaVersion
}

func NewConfig() *Config {
	c := &Config{}

	c.Net.DialTimeout = 30 * time.Second
	c.Net.ReadTimeout = 30 * time.Second
	c.Net.WriteTimeout = 30 * time.Second
	// c.Net.SASL.Handshake = true
	// c.Net.SASL.Version = SASLHandshakeV1

	c.Metadata.Retry.Max = 3
	c.Metadata.Retry.Backoff = 250 * time.Millisecond
	c.Metadata.RefreshFrequency = 10 * time.Minute
	c.Metadata.AllowAutoTopicCreation = true

	c.Producer.MaxMessageBytes = 1000000
	c.Producer.Timeout = 10 * time.Second
	c.Producer.CompressionLevel = CompressionLevelDefault
	c.Producer.lingerMs = time.Duration(300) * time.Millisecond

	c.Producer.Transaction.Timeout = 1 * time.Minute
	c.Producer.Transaction.Retry.Max = 50
	c.Producer.Transaction.Retry.Backoff = 100 * time.Millisecond

	c.Consumer.Fetch.Min = 1
	c.Consumer.Fetch.Default = 1024 * 1024
	c.Consumer.Retry.Backoff = 2 * time.Second
	c.Consumer.MaxWaitTime = 500 * time.Millisecond
	c.Consumer.MaxProcessingTime = 100 * time.Millisecond
	c.Consumer.Return.Errors = false

	c.Consumer.Group.Session.Timeout = 10 * time.Second
	c.Consumer.Group.Heartbeat.Interval = 3 * time.Second
	c.Consumer.Group.Rebalance.GroupStrategies = []BalanceStrategy{NewBalanceStrategyRange()}
	c.Consumer.Group.Rebalance.Timeout = 60 * time.Second
	c.Consumer.Group.Rebalance.Retry.Max = 4
	c.Consumer.Group.Rebalance.Retry.Backoff = 2 * time.Second
	c.Consumer.Group.ResetInvalidOffsets = true
	c.Consumer.Retry.Max = 3

	c.ClientID = defaultClientID
	c.ChannelBufferSize = 256
	c.ApiVersionsRequest = true
	c.Version = DefaultVersion

	return c
}

func (c *Config) Validate() error {
	// some configuration values should be warned on but not fail completely, do those first
	if !c.Net.TLS.Enable && c.Net.TLS.Config != nil {
		Logger.Println("Net.TLS is disabled but a non-nil configuration was provided.")
	}
	if c.Producer.Timeout%time.Millisecond != 0 {
		Logger.Println("Producer.Timeout only supports millisecond resolution; nanoseconds will be truncated.")
	}
	if c.Consumer.Group.Session.Timeout%time.Millisecond != 0 {
		Logger.Println("Consumer.Group.Session.Timeout only supports millisecond precision; nanoseconds will be truncated.")
	}
	if c.Consumer.Group.Heartbeat.Interval%time.Millisecond != 0 {
		Logger.Println("Consumer.Group.Heartbeat.Interval only supports millisecond precision; nanoseconds will be truncated.")
	}
	if c.Consumer.Group.Rebalance.Timeout%time.Millisecond != 0 {
		Logger.Println("Consumer.Group.Rebalance.Timeout only supports millisecond precision; nanoseconds will be truncated.")
	}
	if c.ClientID == defaultClientID {
		Logger.Println("ClientID is the default of 'mertani', you should consider setting it to something application-specific.")
	}

	// validate the Producer values
	switch {
	case c.Producer.MaxMessageBytes <= 0:
		return ConfigurationError("Producer.MaxMessageBytes must be > 0")
	// case c.Producer.RequiredAcks < -1:
	// 	return ConfigurationError("Producer.RequiredAcks must be >= -1")
	case c.Producer.Timeout <= 0:
		return ConfigurationError("Producer.Timeout must be > 0")
	}

	if c.Producer.Idempotent {
		if !c.Version.IsAtLeast(V0_11_0_0) {
			return ConfigurationError("Idempotent producer requires Version >= V0_11_0_0")
		}
	}

	if c.Producer.Transaction.ID != "" && !c.Producer.Idempotent {
		return ConfigurationError("Transactional producer requires Idempotent to be true")
	}

	// validate the Consumer values
	switch {
	case c.Consumer.Fetch.Min <= 0:
		return ConfigurationError("Consumer.Fetch.Min must be > 0")
	case c.Consumer.Fetch.Default <= 0:
		return ConfigurationError("Consumer.Fetch.Default must be > 0")
	case c.Consumer.Fetch.Max < 0:
		return ConfigurationError("Consumer.Fetch.Max must be >= 0")
	case c.Consumer.MaxWaitTime < 1*time.Millisecond:
		return ConfigurationError("Consumer.MaxWaitTime must be >= 1ms")
	case c.Consumer.MaxProcessingTime <= 0:
		return ConfigurationError("Consumer.MaxProcessingTime must be > 0")
	case c.Consumer.Retry.Backoff < 0:
		return ConfigurationError("Consumer.Retry.Backoff must be >= 0")
	case c.Consumer.IsolationLevel != ReadUncommitted && c.Consumer.IsolationLevel != ReadCommitted:
		return ConfigurationError("Consumer.IsolationLevel must be ReadUncommitted or ReadCommitted")
	}

	if c.Consumer.Group.Rebalance.Strategy != nil {
		Logger.Println("Deprecation warning: Consumer.Group.Rebalance.Strategy exists for historical compatibility" +
			" and should not be used. Please use Consumer.Group.Rebalance.GroupStrategies")
	}

	// validate IsolationLevel
	if c.Consumer.IsolationLevel == ReadCommitted && !c.Version.IsAtLeast(V0_11_0_0) {
		return ConfigurationError("ReadCommitted requires Version >= V0_11_0_0")
	}

	// validate the Consumer Group values
	switch {
	case c.Consumer.Group.Session.Timeout <= 2*time.Millisecond:
		return ConfigurationError("Consumer.Group.Session.Timeout must be >= 2ms")
	case c.Consumer.Group.Heartbeat.Interval < 1*time.Millisecond:
		return ConfigurationError("Consumer.Group.Heartbeat.Interval must be >= 1ms")
	case c.Consumer.Group.Heartbeat.Interval >= c.Consumer.Group.Session.Timeout:
		return ConfigurationError("Consumer.Group.Heartbeat.Interval must be < Consumer.Group.Session.Timeout")
	// case c.Consumer.Group.Rebalance.Strategy == nil && len(c.Consumer.Group.Rebalance.GroupStrategies) == 0:
	// 	return ConfigurationError("Consumer.Group.Rebalance.GroupStrategies or Consumer.Group.Rebalance.Strategy must not be empty")
	case c.Consumer.Group.Rebalance.Timeout <= time.Millisecond:
		return ConfigurationError("Consumer.Group.Rebalance.Timeout must be >= 1ms")
	case c.Consumer.Group.Rebalance.Retry.Max < 0:
		return ConfigurationError("Consumer.Group.Rebalance.Retry.Max must be >= 0")
	case c.Consumer.Group.Rebalance.Retry.Backoff < 0:
		return ConfigurationError("Consumer.Group.Rebalance.Retry.Backoff must be >= 0")
	}

	// for _, strategy := range c.Consumer.Group.Rebalance.GroupStrategies {
	// 	if strategy == nil {
	// 		return ConfigurationError("elements in Consumer.Group.Rebalance.Strategies must not be empty")
	// 	}
	// }

	if c.Consumer.Group.InstanceId != "" {
		if !c.Version.IsAtLeast(V2_3_0_0) {
			return ConfigurationError("Consumer.Group.InstanceId need Version >= 2.3")
		}
		// if err := validateGroupInstanceId(c.Consumer.Group.InstanceId); err != nil {
		// 	return err
		// }
	}

	// validate misc shared values
	switch {
	case c.ChannelBufferSize < 0:
		return ConfigurationError("ChannelBufferSize must be >= 0")
	}

	// only validate clientID locally for Kafka versions before KIP-190 was implemented
	if !c.Version.IsAtLeast(V1_0_0_0) && !validClientID.MatchString(c.ClientID) {
		return ConfigurationError(fmt.Sprintf("ClientID value %q is not valid for Kafka versions before 1.0.0", c.ClientID))
	}

	return nil
}

func (c *Config) getDialer() proxy.Dialer {
	return &net.Dialer{
		Timeout:   c.Net.DialTimeout,
		KeepAlive: c.Net.KeepAlive,
		LocalAddr: c.Net.LocalAddr,
	}
}
