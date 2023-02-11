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
	"errors"
	"sync"

	proxypb "github.com/linkall-labs/vanus/proto/pkg/proxy"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ClientOptions struct {
	Endpoint string
}

type streamState string

var (
	stateInitialized streamState = "initialized"
	stateRunning     streamState = "running"
	stateClosed      streamState = "closed"
)

type client struct {
	endpoint        string
	controller      proxypb.ControllerProxyClient
	subscriberCache sync.Map
	publisherCache  sync.Map
	subMu           sync.RWMutex
	pubMu           sync.RWMutex
	conn            *grpc.ClientConn
}

func Connect(options *ClientOptions) (Client, error) {
	if options.Endpoint == "" {
		//log.Error(context.Background(), "endpoint is required for client", nil)
		return nil, errors.New("endpoint is required for client")
	}
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpc.Dial(options.Endpoint, opts...)
	if err != nil {
		//log.Error(context.Background(), "grpc dial error", map[string]interface{}{
		//	log.KeyError: err,
		//})
		return nil, err
	}
	return &client{
		conn:       conn,
		endpoint:   options.Endpoint,
		controller: proxypb.NewControllerProxyClient(conn),
	}, nil
}

func (c *client) Disconnect() error {
	return nil
}

func (c *client) Close() error {
	return nil
}

func (c *client) Publisher(opts ...PublishOption) Publisher {
	defaultOpts := defaultPublishOptions()
	for _, apply := range opts {
		apply(defaultOpts)
	}

	value, ok := c.publisherCache.Load(defaultOpts.eventbus)
	if ok {
		return value.(Publisher)
	}

	// TODO(wenfeng) use connection pool
	publisher := newPublisher(c.conn, defaultOpts)
	value, _ = c.publisherCache.LoadOrStore(defaultOpts.eventbus, publisher)
	return value.(Publisher)
}

func (c *client) Subscriber(opts ...SubscribeOption) Subscriber {
	defaultOptions := defaultSubscribeOptions()
	for _, apply := range opts {
		apply(defaultOptions)
	}

	value, ok := c.subscriberCache.Load(defaultOptions.subscriptionID)
	if ok {
		return value.(Subscriber)
	}

	subscribe := newSubscriber(c.conn, defaultOptions)

	value, _ = c.subscriberCache.LoadOrStore(defaultOptions.subscriptionID, subscribe)
	return value.(Subscriber)
}
