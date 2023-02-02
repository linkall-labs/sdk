// Copyright 2023 Linkall Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file exceptreq compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed toreq writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package golang

import (
	"context"

	v2 "github.com/cloudevents/sdk-go/v2"
	"github.com/linkall-labs/vanus/proto/pkg/cloudevents"
	proxypb "github.com/linkall-labs/vanus/proto/pkg/proxy"
)

type publish struct {
	store   proxypb.StoreProxyClient
	options *PublishOptions
}

func (p *publish) Eventbus() string {
	return p.options.Eventbus
}

func (p *publish) Publish(ctx context.Context, events ...*v2.Event) error {
	eventpb, err := ToProto(events[0])
	if err != nil {
		return err
	}
	in := &proxypb.PublishRequest{
		EventbusName: p.options.Eventbus,
		Events: &cloudevents.CloudEventBatch{
			Events: []*cloudevents.CloudEvent{eventpb},
		},
	}
	_, err = p.store.Publish(context.Background(), in)
	if err != nil {
		return err
	}
	return nil
}

func (c *client) Publisher(opts *PublishOptions) Publisher {
	c.pubMu.RLock()
	value, ok := c.publisherCache.Load(opts.Eventbus)
	if ok {
		defer c.pubMu.RUnlock()
		return value.(*publish)
	}
	c.pubMu.RUnlock()

	c.pubMu.Lock()
	defer c.pubMu.Unlock()

	value, ok = c.publisherCache.Load(opts.Eventbus)
	if ok {
		return value.(*publish)
	}

	publisher := &publish{
		store:   c.store,
		options: opts,
	}
	c.publisherCache.Store(opts.Eventbus, publisher)
	return publisher
}
