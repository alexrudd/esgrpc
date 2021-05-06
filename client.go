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
type ReadOption func(*readConfig) error

func (c *Client) Read(ctx context.Context, opts ...ReadOption) ([]*api.ReadResp_ReadEvent, error) {
	config := newDefaultReadConfig()
	for _, applyOpt := range opts {
		if err := applyOpt(config); err != nil {
			return nil, fmt.Errorf("applying read option: %w", err)
		}
	}

	if _, isSub := config.options.CountOption.(*api.ReadReq_Options_Subscription); isSub {
		return nil, errors.New("Must specify a count limit for synchronous reads")
	}

	readClient, err := c.stream.Read(ctx, &api.ReadReq{Options: config.options})
	if err != nil {
		return nil, fmt.Errorf("calling Read rpc: %w", err)
	}

	events := []*api.ReadResp_ReadEvent{}

	for {
		// have we been cancelled?
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// read from stream
		resp, err := readClient.Recv()
		if errors.Is(err, io.EOF) {
			if config.readStopped != nil {
				config.readStopped(nil)
			}
			return events, nil
		} else if err != nil {
			if config.readStopped != nil {
				config.readStopped(err)
			}
			return nil, fmt.Errorf("reading from stream: %w", err)
		}

		switch content := resp.Content.(type) {
		case *api.ReadResp_Event:
			events = append(events, content.Event)
		case *api.ReadResp_Confirmation:
			continue
		case *api.ReadResp_Checkpoint_:
			if config.checkpointReached != nil {
				config.checkpointReached(content.Checkpoint)
			}
			continue
		case *api.ReadResp_StreamNotFound_:
			return nil, errors.New("stream does not exist") // TODO: could just return empty events slice?
		default:
			continue
		}
	}
}

type AsyncEventHandler func(context.Context, *api.ReadResp_ReadEvent)

func (c *Client) ReadASync(ctx context.Context, h AsyncEventHandler, opts ...ReadOption) error {
	config := newDefaultReadConfig()
	for _, applyOpt := range opts {
		if err := applyOpt(config); err != nil {
			return fmt.Errorf("applying read option: %w", err)
		}
	}

	readClient, err := c.stream.Read(ctx, &api.ReadReq{Options: config.options})
	if err != nil {
		return fmt.Errorf("calling Read rpc: %w", err)
	}

	go func() {
		for {
			// have we been cancelled?
			select {
			case <-ctx.Done():
				if config.readStopped != nil {
					config.readStopped(ctx.Err())
				}
			default:
			}

			resp, err := readClient.Recv()
			if err != nil {
				if config.readStopped != nil {
					config.readStopped(err)
				}
				return
			}

			switch content := resp.Content.(type) {
			case *api.ReadResp_Event:
				h(context.Background(), content.Event)
			case *api.ReadResp_Confirmation:
				continue
			case *api.ReadResp_Checkpoint_:
				if config.checkpointReached != nil {
					config.checkpointReached(content.Checkpoint)
				}
				continue
			case *api.ReadResp_StreamNotFound_:
				if config.readStopped != nil {
					config.readStopped(errors.New("stream does not exist"))
				}
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
