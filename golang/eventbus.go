// Copyright 2023 Linkall Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vanus

import (
	// standard libraries.
	"context"
	"strings"
	"time"

	// third-party libraries.
	"google.golang.org/protobuf/types/known/wrapperspb"

	// first-party libraries.
	ctrlpb "github.com/vanus-labs/vanus/api/controller"
	"github.com/vanus-labs/vanus/api/errors"
	metapb "github.com/vanus-labs/vanus/api/meta"
	proxypb "github.com/vanus-labs/vanus/api/proxy"
)

type eventbus struct {
	controller proxypb.ControllerProxyClient
}

// Make sure eventbus implements Eventbus.
var _ Eventbus = (*eventbus)(nil)

func (eb *eventbus) LookupOffset(ctx context.Context, timestamp time.Time, opts ...EventbusOption) (*proxypb.LookupOffsetResponse, error) {
	ebOpts := newEventbusOptions(opts...)
	eventbus, err := eb.get(ctx, ebOpts)
	if err != nil {
		return nil, err
	}
	resp, err := eb.controller.LookupOffset(ctx, &proxypb.LookupOffsetRequest{
		EventbusId: eventbus.Id,
		Timestamp:  timestamp.UnixMilli(),
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (eb *eventbus) List(ctx context.Context) ([]*metapb.Eventbus, error) {
	res, err := eb.controller.ListEventbus(ctx, &ctrlpb.ListEventbusRequest{})
	if err != nil {
		return nil, err
	}
	return res.GetEventbus(), nil
}

func (eb *eventbus) Get(ctx context.Context, opts ...EventbusOption) (*metapb.Eventbus, error) {
	return eb.get(ctx, newEventbusOptions(opts...))
}

func (eb *eventbus) Create(ctx context.Context, opts ...EventbusOption) (*metapb.Eventbus, error) {
	ebOpts := newEventbusOptions(opts...)
	if ebOpts.eventbusName == "" || ebOpts.namespace == "" {
		return nil, ErrInvalidArguments
	}
	_, err := eb.get(ctx, ebOpts)
	if err != ErrEventbusNotFound {
		if err != nil {
			return nil, err
		}
		return nil, ErrEventbusExist
	}

	ns, err := eb.controller.GetNamespaceWithHumanFriendly(ctx, wrapperspb.String(ebOpts.namespace))
	if err != nil {
		return nil, err
	}

	return eb.controller.CreateEventbus(ctx, &ctrlpb.CreateEventbusRequest{
		Id:          ebOpts.eventbusID,
		Name:        ebOpts.eventbusName,
		NamespaceId: ns.Id,
		LogNumber:   1,
	})
}

func (eb *eventbus) Delete(ctx context.Context, opts ...EventbusOption) error {
	pb, err := eb.get(ctx, newEventbusOptions(opts...))
	if err == ErrEventbusNotFound {
		return nil
	}

	if err != nil {
		return err
	}

	if pb == nil {
		return nil
	}

	_, err = eb.controller.DeleteEventbus(ctx, &wrapperspb.UInt64Value{Value: pb.Id})
	if err != nil {
		return err
	}
	return nil
}

func (eb *eventbus) CheckHealth(ctx context.Context, opts ...EventbusOption) error {
	o := newEventbusOptions(opts...)
	if o.eventbusID == 0 {
		return ErrEventbusIsZero
	}
	_, err := eb.controller.ValidateEventbus(ctx, &proxypb.ValidateEventbusRequest{
		EventbusId: o.eventbusID,
	})
	return err
}

func (eb *eventbus) get(ctx context.Context, opts eventbusOptions) (*metapb.Eventbus, error) {
	if opts.eventbusID == 0 {
		if opts.namespace != "" && opts.eventbusName != "" {
			ns, err := eb.controller.GetNamespaceWithHumanFriendly(ctx, wrapperspb.String(opts.namespace))
			if err != nil {
				return nil, ErrNamespaceNotFound
			}

			eb, err := eb.controller.GetEventbusWithHumanFriendly(ctx, &ctrlpb.GetEventbusWithHumanFriendlyRequest{
				NamespaceId:  ns.Id,
				EventbusName: opts.eventbusName,
			})
			switch {
			case err == nil:
			case errors.Is(err, errors.ErrResourceNotFound):
				return nil, ErrEventbusNotFound
			case strings.Contains(err.Error(), "eventbus not found") || strings.Contains(err.Error(), "9400"):
				// Compatible with 0.7.0, and will be removed in the future.
				return nil, ErrEventbusNotFound
			}
			// TODO(james.yin): return ErrorType?
			return eb, err
		}
		return nil, ErrEventbusIsZero
	}
	eventbus, err := eb.controller.GetEventbus(ctx, wrapperspb.UInt64(opts.eventbusID))
	if err != nil {
		if errors.Is(err, errors.ErrResourceNotFound) {
			return nil, ErrEventbusNotFound
		}
		return nil, err
	}
	return eventbus, nil
}
