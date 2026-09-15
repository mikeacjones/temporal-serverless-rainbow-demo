// Package config resolves the handful of environment variables that let the
// same binaries run against a local dev server or Temporal Cloud.
package config

import (
	"crypto/tls"
	"log/slog"
	"os"
	"strconv"
	"time"

	"go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
)

// Temporal is everything needed to reach a Temporal namespace.
type Temporal struct {
	Address   string
	Namespace string
	// APIKey is set for Temporal Cloud and empty for a local dev server. Its
	// presence is what switches on TLS and API-key credentials.
	APIKey string
	// DeploymentName is the Worker Deployment every order worker joins, e.g.
	// "rainbow-orders". Rollouts operate on this name.
	DeploymentName string
}

// Defaults chosen so `go run` against a local dev server needs no env vars.
const (
	defaultAddress        = "localhost:7233"
	defaultNamespace      = "default"
	defaultDeploymentName = "rainbow-orders"
)

// TemporalFromEnv reads the Temporal connection settings.
func TemporalFromEnv() Temporal {
	return Temporal{
		Address:        Env("TEMPORAL_ADDRESS", defaultAddress),
		Namespace:      Env("TEMPORAL_NAMESPACE", defaultNamespace),
		APIKey:         os.Getenv("TEMPORAL_API_KEY"),
		DeploymentName: Env("TEMPORAL_DEPLOYMENT_NAME", defaultDeploymentName),
	}
}

// ClientOptions builds SDK client options, enabling TLS and API-key
// credentials only when an API key is present.
func (t Temporal) ClientOptions(logger *slog.Logger) client.Options {
	opts := client.Options{
		HostPort:  t.Address,
		Namespace: t.Namespace,
		Logger:    sdklog.NewStructuredLogger(logger),
	}
	if t.APIKey != "" {
		opts.Credentials = client.NewAPIKeyStaticCredentials(t.APIKey)
		// Temporal Cloud requires TLS; the zero config uses the system roots.
		opts.ConnectionOptions = client.ConnectionOptions{TLS: &tls.Config{MinVersion: tls.VersionTLS12}}
	}
	return opts
}

// Cloud reports whether these settings point at Temporal Cloud.
func (t Temporal) Cloud() bool { return t.APIKey != "" }

// Env returns the environment variable or a fallback when unset or empty.
func Env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// EnvInt returns an integer environment variable or a fallback when unset or
// unparseable.
func EnvInt(key string, fallback int) int {
	v, err := strconv.Atoi(os.Getenv(key))
	if err != nil {
		return fallback
	}
	return v
}

// EnvDuration returns a duration environment variable (e.g. "5s") or a
// fallback when unset or unparseable.
func EnvDuration(key string, fallback time.Duration) time.Duration {
	d, err := time.ParseDuration(os.Getenv(key))
	if err != nil {
		return fallback
	}
	return d
}

// Logger builds the structured logger every binary uses. LOG_FORMAT=text
// gives human-readable output for local runs; the default is JSON.
func Logger() *slog.Logger {
	if Env("LOG_FORMAT", "json") == "text" {
		return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel()}))
	}
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel()}))
}

func logLevel() slog.Level {
	var l slog.Level
	if err := l.UnmarshalText([]byte(Env("LOG_LEVEL", "info"))); err != nil {
		return slog.LevelInfo
	}
	return l
}
