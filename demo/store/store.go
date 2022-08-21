package store

import "context"

type EventStore interface {
	ReadStream(ctx context.Context, streamType, streamID string) (Stream, error)
}

type Stream interface {
	Events() []Event
	Append(ctx context.Context, event interface{}) error
}

type Event interface {
	ID() string
	Revision() uint64
	Data() (interface{}, error)
}
