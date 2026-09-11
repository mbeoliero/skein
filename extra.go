package skein

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	json "encoding/json/v2"
	"uuid"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type runExtra struct {
	Traceparent string `json:"traceparent,omitempty"`
	Tracestate  string `json:"tracestate,omitempty"`
	LeaseOwner  string `json:"lease_owner,omitempty"`
}

func traceExtra(ctx context.Context) []byte {
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	// The carrier contains only validated W3C strings, so JSON encoding cannot fail.
	raw, _ := json.Marshal(runExtra{Traceparent: carrier["traceparent"], Tracestate: carrier["tracestate"]})
	return raw
}

func parseExtra(raw []byte) runExtra {
	var extra runExtra
	if err := json.Unmarshal(raw, &extra); err != nil {
		return runExtra{}
	}
	return extra
}

func restoreTrace(raw []byte) context.Context {
	extra := parseExtra(raw)
	return propagation.TraceContext{}.Extract(context.Background(), propagation.MapCarrier{
		"traceparent": extra.Traceparent,
		"tracestate":  extra.Tracestate,
	})
}

func traceId(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return ""
	}
	return sc.TraceID().String()
}

func executionId(token uuid.UUID) string {
	// Correlation must not expose the credential that authorizes settlement.
	digest := sha256.Sum256([]byte("skein/execution/" + token.String()))
	return hex.EncodeToString(digest[:])
}
