package deploy

import (
	"errors"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/sdk/converter"
)

// ErrUnknownVersion is returned when a label is not registered in the
// deployment — usually because that version's worker is not running.
var ErrUnknownVersion = errors.New("deploy: unknown version")

// decodeLabel reads the friendly label out of a version's metadata.
//
// Metadata values are Temporal payloads, so they go through the data
// converter rather than being read as plain strings.
func decodeLabel(metadata map[string]*commonpb.Payload) string {
	p, ok := metadata[MetadataKeyVersion]
	if !ok {
		return ""
	}
	var label string
	if err := converter.GetDefaultDataConverter().FromPayload(p, &label); err != nil {
		return ""
	}
	return label
}
