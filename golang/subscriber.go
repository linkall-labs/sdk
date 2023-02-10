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
	"sync"

	v2 "github.com/cloudevents/sdk-go/v2"
	proxypb "github.com/linkall-labs/vanus/proto/pkg/proxy"
	"google.golang.org/grpc"
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
	options         *subscribeOptions
	subscribeStream proxypb.StoreProxy_SubscribeClient
	ackStream       proxypb.StoreProxy_AckClient
	messageC        chan Message
	state           streamState
	mu              sync.Mutex
	closeOnce       sync.Once
	handler         func(ctx context.Context, msgs ...Message) error
	closeC          chan struct{}
}

func (s *subscribe) Listen(handler func(ctx context.Context, msgs ...Message) error) error {
	in := &proxypb.SubscribeRequest{
		SubscriptionId: s.SubscriptionID(),
	}
	var err error
	ctx := context.Background()
	s.subscribeStream, err = s.store.Subscribe(ctx, in)
	if err != nil {
		_ = s.Close()
		return err
	}
	s.ackStream, err = s.store.Ack(ctx)
	if err != nil {
		_ = s.Close()
		return err
	}
	s.handler = handler
	go s.startReceive()
	barrier := make(chan struct{}, s.options.parallelism)
	for idx := 0; idx < s.options.parallelism; idx++ {
		barrier <- struct{}{}
	}
	for {
		select {
		case <-s.closeC:
			// TODO log
			goto CLOSE
		case msg := <-s.messageC:
			<-barrier
			l := len(s.messageC)
			var msgs []Message
			msgs = append(msgs, msg)
			for idx := 0; idx < l && idx < s.options.batchSize; idx++ {
				msgs = append(msgs, <-s.messageC)
			}
			go func() {
				// TODO(wenfeng) How to process error?
				_ = s.handler(context.Background(), msgs...)
				barrier <- struct{}{}
			}()
		}
	}
CLOSE:
	close(barrier)
	return nil
}

func (s *subscribe) Close() error {
	s.closeOnce.Do(func() {
		_ = s.subscribeStream.CloseSend()
		s.subscribeStream = nil
		_ = s.ackStream.CloseSend()
		s.ackStream = nil
		close(s.messageC)
		close(s.closeC)
		s.state = stateClosed
	})
	return nil
}

func newSubscriber(cc *grpc.ClientConn, opts *subscribeOptions) (Subscriber, error) {
	ch := make(chan Message, 32)
	return &subscribe{
		store:    proxypb.NewStoreProxyClient(cc),
		options:  opts,
		messageC: ch,
		closeC:   make(chan struct{}),
		state:    stateInitialized,
	}, nil
}

func (s *subscribe) SubscriptionID() string {
	return s.options.subscriptionID
}

func (s *subscribe) startReceive() {
	for {
		resp, err := s.subscribeStream.Recv()
		if err != nil {
			_ = s.Close() // TODO(wenfeng) how to process error?
			break
		}

		ackFunc := func(result bool) error {
			req := &proxypb.AckRequest{
				SequenceId:     resp.SequenceId,
				SubscriptionId: s.options.subscriptionID,
				Success:        result,
			}
			err = s.ackStream.Send(req)
			if err != nil {
				_ = s.Close()
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
					s.messageC <- newMessage(ackFunc, event)
				}
			}
		}
	}
}
