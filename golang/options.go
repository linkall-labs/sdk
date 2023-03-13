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

type EventbusOption func(opt *publishOptions)

type publishOptions struct {
	namespace    string
	eventbusName string
	eventbusID   uint64
}

func (o publishOptions) key() string {
	return fmt.Sprintf("%s_%s_%d", o.namespace, o.eventbusName, o.eventbusID)
}

func defaultPublishOptions() *publishOptions {
	return &publishOptions{}
}

func WithEventbus(namespace, name string) EventbusOption {
	return func(opt *publishOptions) {
		opt.namespace = namespace
		opt.eventbusName = name
	}
}

func WithEventbusID(id uint64) EventbusOption {
	return func(opt *publishOptions) {
		opt.eventbusID = id
	}
}

type SubscriptionOption func(opt *subscribeOptions)

type subscribeOptions struct {
	subscriptionID         ID
	batchSize              int
	port                   int
	activeMode             bool
	protocol               Protocol
	order                  bool
	parallelism            int
	consumeTimeoutPerBatch time.Duration
}

func (o subscribeOptions) key() string {
	return fmt.Sprintf("%d", o.subscriptionID)
}

func defaultSubscribeOptions() *subscribeOptions {
	return &subscribeOptions{
		batchSize:   defaultMaxBatchSize,
		port:        defaultListenPort,
		protocol:    ProtocolGRPC,
		parallelism: defaultParallelism,
	}
}

func WithSubscriptionID(id ID) SubscriptionOption {
	return func(opt *subscribeOptions) {
		opt.subscriptionID = id
	}
}

func WithMaxBatchSize(size int) SubscriptionOption {
	return func(opt *subscribeOptions) {
		opt.batchSize = size
	}
}

func WithActiveMode(is bool) SubscriptionOption {
	return func(opt *subscribeOptions) {
		opt.activeMode = true
	}
}

func WithListenPort(port int) SubscriptionOption {
	return func(opt *subscribeOptions) {
		opt.port = port
	}
}

func WithProtocol(p Protocol) SubscriptionOption {
	return func(opt *subscribeOptions) {
		opt.protocol = p
	}
}

func WithOrder(is bool) SubscriptionOption {
	return func(opt *subscribeOptions) {
		opt.order = is
	}
}

func WithParallelism(n int) SubscriptionOption {
	return func(opt *subscribeOptions) {
		opt.parallelism = n
	}
}

func WithConsumeTimeout(t time.Duration) SubscriptionOption {
	return func(opt *subscribeOptions) {
		opt.consumeTimeoutPerBatch = t
	}
}
