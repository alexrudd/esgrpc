package esdb

import (
	"context"
	"fmt"

	"github.com/alexrudd/es-demo/domain"

	"github.com/EventStore/EventStore-Client-Go/messages"
	"github.com/gofrs/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// Event represents a domain event that has been retreived from the event store.
type Event interface {
	ID() string
	Revision() uint64
	Data() proto.Message
}

type EventStore interface {
	ReadStream(ctx context.Context, stream string) ([]Event, error)
	Append(ctx context.Context, stream string, data proto.Message) error
}

// esdbEvent would be defined in the infrastructure layer and implements the
// domain.Event interface
type esdbEvent struct {
	rec *messages.RecordedEvent
}

// toEvent converts an eventstoredb RecordedEvent to an esdbEvent that
// implements the domain.Event interface.
func toEvent(rec *messages.RecordedEvent) domain.Event {
	return &esdbEvent{rec: rec}
}

// ID returns the recorded events uuid as a string.
func (e *esdbEvent) ID() string {
	return e.rec.EventID.String()
}

// Revision returns the events stream revision number.
func (e *esdbEvent) Revision() uint64 {
	return e.rec.EventNumber
}

// Data unpacks and unmarshals the event's data into the correct proto.Message
// type.
func (e *esdbEvent) Data() proto.Message {
	data := &anypb.Any{}

	err := proto.Unmarshal(e.rec.Data, data)
	if err != nil {
		panic(fmt.Sprintf("stream %s event %d: unmarshaling data wrapper: %s", e.rec.StreamID, e.rec.EventNumber, err))
	}

	msg, err := anypb.UnmarshalNew(data, proto.UnmarshalOptions{})
	if err != nil {
		panic(fmt.Sprintf("stream %s event %d: unmarshaling event: %s", e.rec.StreamID, e.rec.EventNumber, err))
	}

	return msg
}

// toProposedEvent packages a proto.Message type into a proposed event so that
// it can be appended to a stream.
func toProposedEvent(msg proto.Message) (messages.ProposedEvent, error) {
	data, err := anypb.New(msg)
	if err != nil {
		return messages.ProposedEvent{}, fmt.Errorf("wrapping data: %w", err)
	}

	b, err := proto.Marshal(data)
	if err != nil {
		return messages.ProposedEvent{}, fmt.Errorf("marshalling data: %w", err)
	}

	return messages.ProposedEvent{
		EventID:     uuid.Must(uuid.NewV4()),
		EventType:   string(msg.ProtoReflect().Descriptor().FullName()),
		ContentType: "proto3",
		Data:        b,
	}, nil
}
