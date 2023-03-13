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
	"context"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"sync"

	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"
	metapb "github.com/vanus-labs/vanus/proto/pkg/meta"
	proxypb "github.com/vanus-labs/vanus/proto/pkg/proxy"
)

type eventbus struct {
	name       string
	controller proxypb.ControllerProxyClient
	meta       *metapb.Eventbus
	opt        *publishOptions
	mutex      sync.Mutex
}

func (eb *eventbus) List(ctx context.Context) ([]*metapb.Eventbus, error) {
	res, err := eb.controller.ListEventbus(ctx, &ctrlpb.ListEventbusRequest{})
	if err != nil {
		return nil, err
	}
	return res.GetEventbus(), nil
}

func (eb *eventbus) Get(ctx context.Context) (*metapb.Eventbus, error) {
	return eb.get(ctx)
}

func (eb *eventbus) Create(ctx context.Context) error {
	if eb.opt.namespace == "" || eb.opt.eventbusName == "" {
		return ErrInvalidArguments
	}

	meta, err := eb.get(ctx)
	if err != nil && err != ErrEventbusNotFound {
		return err
	}

	if meta != nil {
		return ErrEventbusExist
	}

	if err != nil && (err != ErrEventbusIsZero || err != ErrEventbusNotFound) {
		return err
	}

	ns, err := eb.controller.GetNamespaceWithHumanFriendly(ctx, wrapperspb.String(eb.opt.namespace))
	if ns != nil {
		return err
	}

	resp, err := eb.controller.CreateEventbus(ctx, &ctrlpb.CreateEventbusRequest{
		Name:        eb.name,
		NamespaceId: ns.Id,
		LogNumber:   1,
	})
	if err != nil {
		return err
	}
	eb.mutex.Lock()
	if eb.meta != nil {
		eb.meta = resp
	}
	eb.mutex.Unlock()
	return nil
}

func (eb *eventbus) Delete(ctx context.Context) error {
	pb, err := eb.get(ctx)

	if err == ErrEventbusNotFound {
		return nil
	}

	if err != nil {
		return err
	}

	if pb == nil {
		return nil
	}

	_, err = eb.controller.DeleteEventbus(ctx, &wrapperspb.UInt64Value{Value: eb.meta.Id})
	if err != nil {
		return err
	}
	return nil
}

func (eb *eventbus) get(ctx context.Context) (*metapb.Eventbus, error) {
	eb.mutex.Lock()
	defer eb.mutex.Unlock()
	if eb.meta != nil {
		return eb.meta, nil
	}
	if eb.opt.eventbusID == 0 {
		if eb.opt.namespace != "" && eb.opt.eventbusName != "" {
			ns, err := eb.controller.GetNamespaceWithHumanFriendly(ctx, wrapperspb.String(eb.opt.namespace))
			if err != nil {
				return nil, ErrNamespaceNotFound
			}
			resp, err := eb.controller.GetEventbusWithHumanFriendly(ctx, &ctrlpb.GetEventbusWithHumanFriendlyRequest{
				NamespaceId:  ns.Id,
				EventbusName: eb.opt.eventbusName,
			})
			if err != nil {
				// TODO judge resource not found
				return nil, err
			}
			eb.meta = resp
		} else {
			return nil, ErrEventbusIsZero
		}
	} else {
		resp, err := eb.controller.GetEventbus(ctx, wrapperspb.UInt64(eb.opt.eventbusID))
		if err != nil {
			return nil, err
		}
		eb.meta = resp
	}
	return eb.meta, nil
}
