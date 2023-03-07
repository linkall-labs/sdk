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

	v2 "github.com/cloudevents/sdk-go/v2"
	"google.golang.org/grpc"

	"github.com/vanus-labs/vanus/proto/pkg/cloudevents"
	proxypb "github.com/vanus-labs/vanus/proto/pkg/proxy"
)

type publisher struct {
	store   proxypb.StoreProxyClient
	options *publishOptions
}

func newPublisher(cc *grpc.ClientConn, opts *publishOptions) Publisher {
	return &publisher{
		store:   proxypb.NewStoreProxyClient(cc),
		options: opts,
	}
}

func (p *publisher) Close() error {
	// nothing to do
	return nil
}

func (p *publisher) Eventbus() string {
	return p.options.eventbus
}

func (p *publisher) Publish(ctx context.Context, events ...*v2.Event) error {
	pbs := make([]*cloudevents.CloudEvent, 0, len(events))
	for idx := range events {
		pb, err := ToProto(events[idx])
		if err != nil {
			return err
		}
		pbs = append(pbs, pb)
	}

	in := &proxypb.PublishRequest{
		EventbusName: p.options.eventbus,
		Events: &cloudevents.CloudEventBatch{
			Events: pbs,
		},
	}

	_, err := p.store.Publish(ctx, in)
	return err
}
