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
	"sync"

	v2 "github.com/cloudevents/sdk-go/v2"
	"github.com/linkall-labs/vanus/observability/log"
	proxypb "github.com/linkall-labs/vanus/proto/pkg/proxy"
)

type ackCallback func(result bool) error

type message struct {
	event *v2.Event
	ack   ackCallback
}

func newMessage(cb ackCallback, e *v2.Event) Message {
	return &message{
		event: e,
		ack:   cb,
	}
}

func (m *message) GetEvent() *v2.Event {
	return m.event
}

func (m *message) Success() error {
	return m.ack(true)
}

func (m *message) Failed(err error) error {
	return m.ack(false)
}

type subscribe struct {
	store           proxypb.StoreProxyClient
	options         *SubscribeOptions
	subscribeStream proxypb.StoreProxy_SubscribeClient
	ackStream       proxypb.StoreProxy_AckClient
	messagec        chan Message
	state           streamState
	mu              sync.Mutex
}

func newSubscribe(
	c *client,
	opts *SubscribeOptions,
) (*subscribe, error) {
	in := &proxypb.SubscribeRequest{
		SubscriptionId: opts.SubscriptionID,
	}
	subscribeStream, err := c.store.Subscribe(context.Background(), in)
	if err != nil {
		return nil, err
	}

	ackStream, err := c.store.Ack(context.Background())
	if err != nil {
		subscribeStream.CloseSend()
		return nil, err
	}

	ch := make(chan Message, 32)
	return &subscribe{
		store:           c.store,
		options:         opts,
		subscribeStream: subscribeStream,
		ackStream:       ackStream,
		messagec:        ch,
		state:           stateInitialized,
	}, nil
}

func (s *subscribe) release() {
	s.subscribeStream.CloseSend()
	s.subscribeStream = nil
	s.ackStream.CloseSend()
	s.ackStream = nil
	close(s.messagec)
	s.state = stateClosed
}

func (s *subscribe) SubscriptionID() string {
	return s.options.SubscriptionID
}

func (s *subscribe) Subscribe(ctx context.Context) (<-chan Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == stateRunning {
		return s.messagec, nil
	}
	go func() {
		for {
			resp, err := s.subscribeStream.Recv()
			if err != nil {
				s.release()
				return
			}
			log.Info(context.Background(), "subscribe stream recv", map[string]interface{}{
				log.KeyError: err,
				"resp":       resp,
			})
			ackFunc := func(result bool) error {
				req := &proxypb.AckRequest{
					SequenceId:     resp.SequenceId,
					SubscriptionId: s.options.SubscriptionID,
					Success:        result,
				}
				err = s.ackStream.Send(req)
				if err != nil {
					s.release()
					return err
				}
				return nil
			}
			if batch := resp.GetEvents(); batch != nil {
				if eventpbs := batch.GetEvents(); len(eventpbs) > 0 {
					for _, eventpb := range eventpbs {
						event, err2 := FromProto(eventpb)
						if err2 != nil {
							// TODO(jiangkai): check err
							continue
						}
						s.messagec <- newMessage(ackFunc, event)
					}
				}
			}
		}
	}()
	s.state = stateRunning
	return s.messagec, nil
}

func (c *client) Subscriber(opts *SubscribeOptions) (Subscriber, error) {
	c.subMu.RLock()
	value, ok := c.subscriberCache.Load(opts.SubscriptionID)
	if ok {
		defer c.subMu.RUnlock()
		return value.(*subscribe), nil
	}
	c.subMu.RUnlock()

	c.subMu.Lock()
	defer c.subMu.Unlock()

	value, ok = c.subscriberCache.Load(opts.SubscriptionID)
	if ok {
		return value.(*subscribe), nil
	}

	subscribe, err := newSubscribe(c, opts)
	if err != nil {
		return nil, err
	}
	c.subscriberCache.Store(opts.SubscriptionID, subscribe)
	return subscribe, nil
}
