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
	"fmt"
	"github.com/linkall-labs/vanus/proto/pkg/cloudevents"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/types/known/emptypb"
	"net"
	"sync"

	v2 "github.com/cloudevents/sdk-go/v2"
	proxypb "github.com/linkall-labs/vanus/proto/pkg/proxy"
	"google.golang.org/grpc"
)

type ackCallback func(err error)

type message struct {
	event   *v2.Event
	ack     ackCallback
	ackFlag atomic.Bool
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

func (m *message) Success() {
	if m.ackFlag.CAS(false, true) {
		m.ack(nil)
	}
}

func (m *message) Failed(err error) {
	if m.ackFlag.CAS(false, true) {
		m.ack(err)
	}
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
	s.handler = handler
	go func() {
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
				if msg == nil {
					goto CLOSE // TODO(wenfeng) how to process if msg is nil?
				}
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
	}()
	return s.startReceive()
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

func newSubscriber(cc *grpc.ClientConn, opts *subscribeOptions) Subscriber {
	return &subscribe{
		store:    proxypb.NewStoreProxyClient(cc),
		options:  opts,
		messageC: make(chan Message, 32),
		closeC:   make(chan struct{}),
		state:    stateInitialized,
	}
}

func (s *subscribe) SubscriptionID() string {
	return s.options.subscriptionID
}

func (s *subscribe) Send(ctx context.Context, event *cloudevents.BatchEvent) (*emptypb.Empty, error) {
	ch := make(chan error, 1)
	s.processCloudEvents(event.Events, func(err error) {
		ch <- err
	})
	err := <-ch
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (s *subscribe) startReceive() error {
	if !s.options.activeMode {
		srv := grpc.NewServer()
		cloudevents.RegisterCloudEventsServer(srv, s)
		listen, err := net.Listen("tcp", fmt.Sprintf(":%d", s.options.port))
		if err != nil {
			return err
		}
		return srv.Serve(listen)
	} else {
		ctx := context.Background()
		var err error
		in := &proxypb.SubscribeRequest{
			SubscriptionId: s.SubscriptionID(),
		}
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
		for {
			select {
			case <-s.closeC:
			default:
				resp, err := s.subscribeStream.Recv()
				if err != nil {
					return s.Close()
				}

				ackFunc := func(err error) {
					req := &proxypb.AckRequest{
						SequenceId:     resp.SequenceId,
						SubscriptionId: s.options.subscriptionID,
						Success:        err == nil,
					}
					_err := s.ackStream.Send(req)
					if _err != nil {
						_ = s.Close()
					}
				}
				if batch := resp.GetEvents(); batch != nil {
					s.processCloudEvents(batch, ackFunc)
				} else {
					ackFunc(nil)
				}
			}
		}
	}
}

func (s *subscribe) processCloudEvents(batch *cloudevents.CloudEventBatch, cb ackCallback) {
	size := atomic.NewInt32(int32(len(batch.GetEvents())))
	_ackFunc := func(err error) {
		if err == nil {
			if size.Dec() == 0 {
				cb(nil)
			}
		} else {
			cb(err)
		}
	}
	if events := batch.GetEvents(); len(events) > 0 {
		for _, e := range events {
			event, err2 := FromProto(e)
			if err2 != nil {
				// TODO(JiangKai): check err
				continue
			}
			s.messageC <- newMessage(_ackFunc, event)
		}
	}
}
