package esgrpc

import (
	"fmt"

	"github.com/EventStore/EventStore-Client-Go/protos/shared"
	api "github.com/EventStore/EventStore-Client-Go/protos/streams"
)

// Options
// StreamOption:
//  * Stream (ID + Revision)
//  * All (Position (Start, End, Specific))
// ReadDirection (Backwards, Forwards)
// ResolveLinks
// CountOption:
//  * Count (uint64)
//  * Subscription (Empty)
// FilterOption
//	* Filter
//    * Stream ID (Prefix slice || regex)
//    * Event (Prefix slice || regex)
//	  * Window
//	  * Interval
//  * NoFilter
// UUID:
//  * Structured
//  * String

func NewDefaultReadOptions() *api.ReadReq_Options {
	return &api.ReadReq_Options{
		StreamOption: &api.ReadReq_Options_All{
			All: &api.ReadReq_Options_AllOptions{
				AllOption: &api.ReadReq_Options_AllOptions_Start{},
			},
		},
		ReadDirection: api.ReadReq_Options_Forwards,
		ResolveLinks:  false,
		CountOption: &api.ReadReq_Options_Subscription{
			Subscription: &api.ReadReq_Options_SubscriptionOptions{},
		},
		FilterOption: &api.ReadReq_Options_NoFilter{
			NoFilter: &shared.Empty{},
		},
		UuidOption: &api.ReadReq_Options_UUIDOption{
			Content: &api.ReadReq_Options_UUIDOption_String_{
				String_: &shared.Empty{},
			},
		},
	}
}

type StreamOption func(*api.ReadReq_Options_Stream) error

func WithStreamOptions(opts ...StreamOption) ReadOption {
	return func(rr *api.ReadReq_Options) error {
		so := &api.ReadReq_Options_Stream{
			Stream: &api.ReadReq_Options_StreamOptions{},
		}

		for _, opt := range opts {
			if err := opt(so); err != nil {
				return fmt.Errorf("applying stream option: %w", err)
			}
		}

		rr.StreamOption = so

		return nil
	}
}

func ForStream(identifier string) StreamOption {
	return func(rr *api.ReadReq_Options_Stream) error {
		rr.Stream.StreamIdentifier = &shared.StreamIdentifier{
			StreamName: []byte(identifier),
		}

		return nil
	}
}

func FromRevision(rev uint64) StreamOption {
	return func(rr *api.ReadReq_Options_Stream) error {
		rr.Stream.RevisionOption = &api.ReadReq_Options_StreamOptions_Revision{
			Revision: rev,
		}

		return nil
	}
}

func FromStartRevision() StreamOption {
	return func(rr *api.ReadReq_Options_Stream) error {
		rr.Stream.RevisionOption = &api.ReadReq_Options_StreamOptions_Start{}

		return nil
	}
}

func FromEndRevision() StreamOption {
	return func(rr *api.ReadReq_Options_Stream) error {
		rr.Stream.RevisionOption = &api.ReadReq_Options_StreamOptions_End{}

		return nil
	}
}

type AllOption func(*api.ReadReq_Options_All) error

func WithAllOptions(opts ...AllOption) ReadOption {
	return func(rr *api.ReadReq_Options) error {
		so := &api.ReadReq_Options_All{
			All: &api.ReadReq_Options_AllOptions{},
		}

		for _, opt := range opts {
			if err := opt(so); err != nil {
				return fmt.Errorf("applying all stream option: %w", err)
			}
		}

		rr.StreamOption = so

		return nil
	}
}

func FromPosition(prepare, commit uint64) AllOption {
	return func(rr *api.ReadReq_Options_All) error {
		rr.All.AllOption = &api.ReadReq_Options_AllOptions_Position{
			Position: &api.ReadReq_Options_Position{
				CommitPosition:  commit,
				PreparePosition: prepare,
			},
		}

		return nil
	}
}

func FromStartPosition() AllOption {
	return func(rr *api.ReadReq_Options_All) error {
		rr.All.AllOption = &api.ReadReq_Options_AllOptions_Start{}

		return nil
	}
}

func FromEndPosition() AllOption {
	return func(rr *api.ReadReq_Options_All) error {
		rr.All.AllOption = &api.ReadReq_Options_AllOptions_End{}

		return nil
	}
}

func InBackwardsDirection() ReadOption {
	return func(rr *api.ReadReq_Options) error {
		rr.ReadDirection = api.ReadReq_Options_Backwards

		return nil
	}
}

func WithLinksResolved() ReadOption {
	return func(rr *api.ReadReq_Options) error {
		rr.ResolveLinks = true

		return nil
	}
}

func ForLimitedCount(count uint64) ReadOption {
	return func(rr *api.ReadReq_Options) error {
		rr.CountOption = &api.ReadReq_Options_Count{
			Count: count,
		}

		return nil
	}
}

type FilterOption func(*api.ReadReq_Options_Filter) error

func WithFilterOptions(opts ...FilterOption) ReadOption {
	return func(rr *api.ReadReq_Options) error {
		fo := &api.ReadReq_Options_Filter{
			Filter: &api.ReadReq_Options_FilterOptions{
				Filter: &api.ReadReq_Options_FilterOptions_StreamIdentifier{
					StreamIdentifier: &api.ReadReq_Options_FilterOptions_Expression{
						Regex: ".*",
					},
				},
				Window: &api.ReadReq_Options_FilterOptions_Max{
					Max: 32, // https://github.com/EventStore/EventStore-Client-Go/blob/700926402daf00de9453c67269c9cd318d5849fc/client/filtering/subscription_filter.go#L27
				},
				CheckpointIntervalMultiplier: 1, // https://github.com/EventStore/EventStore-Client-Go/blob/700926402daf00de9453c67269c9cd318d5849fc/client/filtering/subscription_filter.go#L28
			},
		}

		for _, opt := range opts {
			if err := opt(fo); err != nil {
				return fmt.Errorf("applying filter option: %w", err)
			}
		}

		rr.FilterOption = fo

		return nil
	}
}

func IncludeStreamPrefixes(prefixes []string) FilterOption {
	return func(rr *api.ReadReq_Options_Filter) error {
		rr.Filter.Filter = &api.ReadReq_Options_FilterOptions_StreamIdentifier{
			StreamIdentifier: &api.ReadReq_Options_FilterOptions_Expression{
				Prefix: prefixes,
			},
		}

		return nil
	}
}

func MatchStreamRegex(regex string) FilterOption {
	return func(rr *api.ReadReq_Options_Filter) error {
		rr.Filter.Filter = &api.ReadReq_Options_FilterOptions_StreamIdentifier{
			StreamIdentifier: &api.ReadReq_Options_FilterOptions_Expression{
				Regex: regex,
			},
		}

		return nil
	}
}

func IncludeEventPrefixes(prefixes []string) FilterOption {
	return func(rr *api.ReadReq_Options_Filter) error {
		rr.Filter.Filter = &api.ReadReq_Options_FilterOptions_EventType{
			EventType: &api.ReadReq_Options_FilterOptions_Expression{
				Prefix: prefixes,
			},
		}

		return nil
	}
}

func MatchEventRegex(regex string) FilterOption {
	return func(rr *api.ReadReq_Options_Filter) error {
		rr.Filter.Filter = &api.ReadReq_Options_FilterOptions_EventType{
			EventType: &api.ReadReq_Options_FilterOptions_Expression{
				Regex: regex,
			},
		}

		return nil
	}
}

func WithWindowCount() FilterOption {
	return func(rr *api.ReadReq_Options_Filter) error {
		rr.Filter.Window = &api.ReadReq_Options_FilterOptions_Count{
			Count: &shared.Empty{},
		}

		return nil
	}
}

func WithWindowMax(max uint32) FilterOption {
	return func(rr *api.ReadReq_Options_Filter) error {
		rr.Filter.Window = &api.ReadReq_Options_FilterOptions_Max{
			Max: max,
		}

		return nil
	}
}

func WithCheckpointInterval(interval uint32) FilterOption {
	return func(rr *api.ReadReq_Options_Filter) error {
		rr.Filter.CheckpointIntervalMultiplier = interval

		return nil
	}
}

func WithStructuredUUID() ReadOption {
	return func(rr *api.ReadReq_Options) error {
		rr.UuidOption = &api.ReadReq_Options_UUIDOption{
			Content: &api.ReadReq_Options_UUIDOption_Structured{
				Structured: &shared.Empty{},
			},
		}

		return nil
	}
}

func ExpectRevision(rev uint64) AppendOption {
	return func(ar *api.AppendReq_Options) error {
		ar.ExpectedStreamRevision = &api.AppendReq_Options_Revision{
			Revision: rev,
		}

		return nil
	}
}

func ExpectNoStream() AppendOption {
	return func(ar *api.AppendReq_Options) error {
		ar.ExpectedStreamRevision = &api.AppendReq_Options_NoStream{
			NoStream: &shared.Empty{},
		}

		return nil
	}
}

func ExpectStreamExists() AppendOption {
	return func(ar *api.AppendReq_Options) error {
		ar.ExpectedStreamRevision = &api.AppendReq_Options_StreamExists{
			StreamExists: &shared.Empty{},
		}

		return nil
	}
}
