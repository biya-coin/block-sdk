package inserttrace

import (
	"context"
	"time"
)

type contextKey struct{}

const ContextKey = "github.com/skip-mev/block-sdk/v2/block/inserttrace.recorder"

type Recorder func(step string, duration time.Duration)

func WithRecorder(ctx context.Context, recorder Recorder) context.Context {
	if recorder == nil {
		return ctx
	}

	return context.WithValue(ctx, contextKey{}, recorder)
}

func Observe(ctx context.Context, step string, start time.Time) {
	duration := time.Since(start)

	if recorder, ok := ctx.Value(contextKey{}).(Recorder); ok && recorder != nil {
		recorder(step, duration)
		return
	}

	recorder, ok := ctx.Value(ContextKey).(func(string, time.Duration))
	if !ok || recorder == nil {
		return
	}

	recorder(step, duration)
}
