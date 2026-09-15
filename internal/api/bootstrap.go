package api

import (
	"context"
	"time"

	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/orders"
)

// bootstrapTimeout bounds the wait for a first version to register.
const bootstrapTimeout = 2 * time.Minute

// EnsureCurrentVersion promotes v1 at startup if nothing is Current yet.
//
// A Worker Deployment starts with no Current version, so without this the
// first order would have nowhere to go. It keys off the *published label*
// rather than registration order, because all five versions come up at once
// here and "oldest registered" would be a race.
//
// It acts only while Current is unset, so it runs once on a fresh namespace
// and then keeps out of the way — every later routing change belongs to the
// operator or the rollout coordinator.
func (s *Server) EnsureCurrentVersion(ctx context.Context) {
	deadline := time.Now().Add(bootstrapTimeout)

	for {
		state, err := s.deployment.Describe(ctx)
		switch {
		case err != nil:
			s.logger.Debug("bootstrap waiting on deployment", "err", err)

		case state.Routing.CurrentBuildID != "":
			s.logger.Info("current version already set, skipping bootstrap",
				"current", state.Routing.CurrentLabel)
			return

		default:
			for _, v := range state.Versions {
				if v.Label != string(orders.V1) {
					continue
				}
				if err := s.deployment.SetCurrent(ctx, v.BuildID); err != nil {
					s.logger.Warn("bootstrap promote failed, will retry", "buildId", v.BuildID, "err", err)
					break
				}
				s.logger.Info("bootstrapped current version", "label", v.Label, "buildId", v.BuildID)
				return
			}
		}

		if time.Now().After(deadline) {
			s.logger.Warn("no v1 worker registered in time; promote a version manually to start taking orders",
				"timeout", bootstrapTimeout)
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}
