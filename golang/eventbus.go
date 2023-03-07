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

	"google.golang.org/protobuf/types/known/emptypb"

	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"
	metapb "github.com/vanus-labs/vanus/proto/pkg/meta"
	proxypb "github.com/vanus-labs/vanus/proto/pkg/proxy"
)

type eventbus struct {
	name       string
	controller proxypb.ControllerProxyClient
}

func (eb *eventbus) List(ctx context.Context) ([]*metapb.EventBus, error) {
	res, err := eb.controller.ListEventBus(context.Background(), &emptypb.Empty{})
	if err != nil {
		return nil, err
	}
	return res.GetEventbus(), nil
}

func (eb *eventbus) Get(ctx context.Context) (*metapb.EventBus, error) {
	return eb.controller.GetEventBus(context.Background(), &metapb.EventBus{Name: eb.name})
}

func (eb *eventbus) Create(ctx context.Context) error {
	_, err := eb.controller.CreateEventBus(context.Background(), &ctrlpb.CreateEventBusRequest{
		Name:      eb.name,
		LogNumber: 1,
	})
	if err != nil {
		return err
	}
	return nil
}

func (eb *eventbus) Delete(ctx context.Context) error {
	_, err := eb.controller.DeleteEventBus(context.Background(), &metapb.EventBus{Name: eb.name})
	if err != nil {
		return err
	}
	return nil
}
