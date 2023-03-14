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
	"fmt"
	"time"
)

type Protocol int

const (
	ProtocolHTTP Protocol = iota
	ProtocolGRPC

	defaultListenPort   = 8080
	defaultMaxBatchSize = 16
	defaultParallelism  = 4
)

type EventbusOption func(opt *eventbusOptions)

type eventbusOptions struct {
	namespace    string
	eventbusName string
	eventbusID   uint64
}

func newEventbusOptions(options ...EventbusOption) eventbusOptions {
	opts := defaultEventbusOptions()

	for _, apply := range options {
		apply(&opts)
	}
	return opts
}

func (o eventbusOptions) key() string {
	return fmt.Sprintf("%s_%s_%d", o.namespace, o.eventbusName, o.eventbusID)
}

func defaultEventbusOptions() eventbusOptions {
	return eventbusOptions{}
}

func WithEventbus(namespace, name string) EventbusOption {
	return func(opt *eventbusOptions) {
		opt.namespace = namespace
		opt.eventbusName = name
	}
}

func WithEventbusID(id uint64) EventbusOption {
	return func(opt *eventbusOptions) {
		opt.eventbusID = id
	}
}

type SubscriptionOption func(opt *subscriptionOptions)

type subscriptionOptions struct {
	subscriptionID         ID
	batchSize              int
	port                   int
	activeMode             bool
	protocol               Protocol
	order                  bool
	parallelism            int
	consumeTimeoutPerBatch time.Duration
}

func newSubscriptionOptions(opts ...SubscriptionOption) subscriptionOptions {
	o := defaultSubscribeOptions()
	for _, apply := range opts {
		apply(&o)
	}
	return o
}

func (o subscriptionOptions) key() string {
	return fmt.Sprintf("%d", o.subscriptionID)
}

func defaultSubscribeOptions() subscriptionOptions {
	return subscriptionOptions{
		batchSize:   defaultMaxBatchSize,
		port:        defaultListenPort,
		protocol:    ProtocolGRPC,
		parallelism: defaultParallelism,
	}
}

func WithSubscriptionID(id ID) SubscriptionOption {
	return func(opt *subscriptionOptions) {
		opt.subscriptionID = id
	}
}

func WithMaxBatchSize(size int) SubscriptionOption {
	return func(opt *subscriptionOptions) {
		opt.batchSize = size
	}
}

func WithActiveMode(is bool) SubscriptionOption {
	return func(opt *subscriptionOptions) {
		opt.activeMode = true
	}
}

func WithListenPort(port int) SubscriptionOption {
	return func(opt *subscriptionOptions) {
		opt.port = port
	}
}

func WithProtocol(p Protocol) SubscriptionOption {
	return func(opt *subscriptionOptions) {
		opt.protocol = p
	}
}

func WithOrder(is bool) SubscriptionOption {
	return func(opt *subscriptionOptions) {
		opt.order = is
	}
}

func WithParallelism(n int) SubscriptionOption {
	return func(opt *subscriptionOptions) {
		opt.parallelism = n
	}
}

func WithConsumeTimeout(t time.Duration) SubscriptionOption {
	return func(opt *subscriptionOptions) {
		opt.consumeTimeoutPerBatch = t
	}
}
