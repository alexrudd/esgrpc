package esgrpc

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/EventStore/EventStore-Client-Go/protos/shared"
	api "github.com/EventStore/EventStore-Client-Go/protos/streams"
	"google.golang.org/grpc"
)

// Client for calling the EventStoreDB gRPC API.
type Client struct {
	stream api.StreamsClient
}

func NewClient(conn *grpc.ClientConn) *Client {
	return &Client{
		stream: api.NewStreamsClient(conn),
	}
}

// ReadOptions are applied to ReadReqs before calling the underlying gRPC API.
type ReadOption func(*api.ReadReq_Options) error

func (c *Client) Read(ctx context.Context, h func(*api.ReadResp_ReadEvent, error), opts ...ReadOption) error {
	readOptions := NewDefaultReadOptions()
	for _, applyOpt := range opts {
		if err := applyOpt(readOptions); err != nil {
			return fmt.Errorf("applying read option: %w", err)
		}
	}

	readCtx, cancel := context.WithCancel(ctx)

	readClient, err := c.stream.Read(readCtx, &api.ReadReq{Options: readOptions})
	if err != nil {
		cancel()
		return fmt.Errorf("calling Read rpc: %w", err)
	}

	go func() {
		defer cancel()

		for {
			resp, err := readClient.Recv()
			if err == io.EOF {
				return
			} else if err != nil {
				h(nil, err)
				return
			}

			switch content := resp.Content.(type) {
			case *api.ReadResp_Event:
				h(content.Event, nil)
			case *api.ReadResp_Confirmation:
				continue
			case *api.ReadResp_Checkpoint_:
				continue
			case *api.ReadResp_StreamNotFound_:
				h(nil, errors.New("stream not found"))
				return
			}
		}
	}()

	return nil
}

type AppendOption func(*api.AppendReq_Options) error

func (c *Client) Append(ctx context.Context, streamID string, events []*api.AppendReq_ProposedMessage, opts ...AppendOption) (*api.AppendResp_Success_, error) {
	appendOptions := &api.AppendReq_Options{
		StreamIdentifier: &shared.StreamIdentifier{
			StreamName: []byte(streamID),
		},
		ExpectedStreamRevision: &api.AppendReq_Options_Any{},
	}
	for _, applyOpt := range opts {
		if err := applyOpt(appendOptions); err != nil {
			return nil, fmt.Errorf("applying append option: %w", err)
		}
	}

	appendClient, err := c.stream.Append(ctx)
	if err != nil {
		return nil, fmt.Errorf("calling Append rpc: %w", err)
	}

	// configure append
	err = appendClient.Send(&api.AppendReq{Content: &api.AppendReq_Options_{
		Options: appendOptions,
	}})
	if err != nil {
		return nil, fmt.Errorf("configuring append operation: %w", err)
	}

	for _, event := range events {
		err = appendClient.Send(&api.AppendReq{
			Content: &api.AppendReq_ProposedMessage_{
				ProposedMessage: event,
			},
		})
		if err != nil {
			return nil, fmt.Errorf("sending event: %w", err)
		}
	}

	response, err := appendClient.CloseAndRecv()
	if err != nil {
		return nil, fmt.Errorf("closing append stream: %w", err)
	}

	switch result := response.GetResult().(type) {
	case *api.AppendResp_Success_:
		{
			return result, nil
		}
	case *api.AppendResp_WrongExpectedVersion_:
		{
			return nil, errors.New("wrong expected version")
		}
	default:
		return nil, fmt.Errorf("unexpected append response: %T", result)
	}
}
