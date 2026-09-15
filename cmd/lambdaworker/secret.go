package main

import (
	"context"
	"fmt"
	"os"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

// resolveAPIKey makes sure TEMPORAL_API_KEY is set before the worker starts.
//
// The key is kept in Secrets Manager rather than in the function's
// environment, so it is not readable from the Lambda console and there is one
// place to rotate it. The SDK reads the credential from the environment, so
// this fetches it once at cold start and puts it there.
//
// TEMPORAL_API_KEY being set directly still wins, which keeps local testing
// of this binary simple.
func resolveAPIKey(ctx context.Context) error {
	if os.Getenv("TEMPORAL_API_KEY") != "" {
		return nil
	}

	arn := os.Getenv("TEMPORAL_API_KEY_SECRET_ARN")
	if arn == "" {
		return fmt.Errorf("set TEMPORAL_API_KEY or TEMPORAL_API_KEY_SECRET_ARN")
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}

	out, err := secretsmanager.NewFromConfig(cfg).GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: &arn,
	})
	if err != nil {
		return fmt.Errorf("read secret %s: %w", arn, err)
	}
	if out.SecretString == nil || *out.SecretString == "" {
		return fmt.Errorf("secret %s is empty", arn)
	}

	return os.Setenv("TEMPORAL_API_KEY", *out.SecretString)
}
