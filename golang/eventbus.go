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

	// third-party libraries.
	"google.golang.org/protobuf/types/known/wrapperspb"

	// first-party libraries.
	"github.com/vanus-labs/vanus/pkg/errors"
	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"
	metapb "github.com/vanus-labs/vanus/proto/pkg/meta"
	proxypb "github.com/vanus-labs/vanus/proto/pkg/proxy"
)

type eventbus struct {
	controller proxypb.ControllerProxyClient
}

// Make sure eventbus implements Eventbus.
var _ Eventbus = (*eventbus)(nil)

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

func (eb *eventbus) Create(ctx context.Context, namespace, name string) (*metapb.Eventbus, error) {
	if name == "" || namespace == "" {
		return nil, ErrInvalidArguments
	}

	opts := defaultEventbusOptions()
	opts.namespace = namespace
	opts.eventbusName = name

	_, err := eb.get(ctx, opts)
	if err != ErrEventbusNotFound {
		if err != nil {
			return nil, err
		}
		return nil, ErrEventbusExist
	}

	ns, err := eb.controller.GetNamespaceWithHumanFriendly(ctx, wrapperspb.String(opts.namespace))
	if err != nil {
		return nil, err
	}

	return eb.controller.CreateEventbus(ctx, &ctrlpb.CreateEventbusRequest{
		Name:        opts.eventbusName,
		NamespaceId: ns.Id,
		LogNumber:   1,
	})
}

func (eb *eventbus) Delete(ctx context.Context, opts ...EventbusOption) error {
	o := newEventbusOptions(opts...)
	pb, err := eb.get(ctx, o)

	if err == ErrEventbusNotFound {
		return nil
	}

	if err != nil {
		return err
	}

	if pb == nil {
		return nil
	}

	_, err = eb.controller.DeleteEventbus(ctx, &wrapperspb.UInt64Value{Value: o.eventbusID})
	if err != nil {
		return err
	}
	return nil
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
			if errors.Is(err, errors.ErrResourceNotFound) {
				return nil, ErrEventbusNotFound
			} else if err != nil && strings.Contains(err.Error(), "resource not found") {
				// Compatible with 0.7.0, and will be removed in the future.
				return nil, ErrEventbusNotFound
			}
			// TODO(james.yin): return ErrorType?
			return eb, err
		}
		return nil, ErrEventbusIsZero
	}
	return eb.controller.GetEventbus(ctx, wrapperspb.UInt64(opts.eventbusID))
}
