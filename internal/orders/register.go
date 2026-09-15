package orders

import (
	"fmt"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

// Registry is the part of a worker this package needs in order to register
// itself.
//
// Declared here, as narrowly as possible, rather than taking worker.Registry:
// a Lambda serverless worker exposes these same methods but is not a full
// worker.Registry, and both kinds of worker should be able to host the order
// pipeline without either one shaping the other.
type Registry interface {
	RegisterWorkflowWithOptions(w any, options workflow.RegisterOptions)
	RegisterActivityWithOptions(a any, options activity.RegisterOptions)
}

// Register wires one version's order pipeline onto a worker.
//
// A worker process serves exactly one version. Both Workflow types are
// registered as Pinned, which is the guarantee the whole demo rests on: an
// order that starts on this worker's version runs to completion on this
// version's code, no matter what a rollout does to the routing config
// underneath it.
func Register(w Registry, v Version, profile Profile) {
	w.RegisterWorkflowWithOptions(
		func(ctx workflow.Context, in OrderInput) (OrderResult, error) {
			return Order(ctx, v, in)
		},
		workflow.RegisterOptions{
			Name:               WorkflowTypeName,
			VersioningBehavior: workflow.VersioningBehaviorPinned,
		},
	)

	w.RegisterWorkflowWithOptions(
		func(ctx workflow.Context, in OrderInput) (OrderResult, error) {
			return Gate(ctx, v, in)
		},
		workflow.RegisterOptions{
			Name:               GateWorkflowTypeName,
			VersioningBehavior: workflow.VersioningBehaviorPinned,
		},
	)

	activities := &Activities{Profile: profile}
	w.RegisterActivityWithOptions(
		activities.PerformStep,
		activity.RegisterOptions{Name: PerformStepActivityName},
	)
}

// Menu is what a customer can order. Purely cosmetic: it gives the dashboard
// something recognisable to show on each order card.
var Menu = []Item{
	{Name: "Double Double", Qty: 1},
	{Name: "Large Dark Roast", Qty: 1},
	{Name: "Boston Cream", Qty: 2},
	{Name: "Honey Dip", Qty: 1},
	{Name: "Ice Capp", Qty: 1},
	{Name: "Bagel B.E.L.T.", Qty: 1},
	{Name: "Timbits 10-pack", Qty: 1},
	{Name: "Steeped Tea", Qty: 2},
	{Name: "Breakfast Wrap", Qty: 1},
	{Name: "Chili + Bun", Qty: 1},
}

// Stores gives orders a plausible origin, so traffic looks like it comes from
// a fleet of locations rather than one firehose.
var Stores = []string{
	"Toronto — Front St",
	"Toronto — Yonge & Eg",
	"Mississauga — Square One",
	"Ottawa — Bank St",
	"Calgary — 17th Ave",
	"Vancouver — Robson",
	"Halifax — Spring Garden",
	"Montreal — Sainte-Catherine",
}

// NewOrderInput builds order number seq, varying items and store by sequence so
// the mix is deterministic and easy to reason about.
func NewOrderInput(seq int, chaos *ChaosSpec) OrderInput {
	first := Menu[seq%len(Menu)]
	second := Menu[(seq*7+3)%len(Menu)]

	items := []Item{first}
	if second.Name != first.Name {
		items = append(items, second)
	}

	return OrderInput{
		OrderID: fmt.Sprintf("ord-%06d", seq),
		Store:   Stores[seq%len(Stores)],
		Items:   items,
		Chaos:   chaos,
	}
}
