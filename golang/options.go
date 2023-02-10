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

import "time"

type Protocol int

const (
	ProtocolHTTP Protocol = iota
	ProtocolGRPC

	defaultListenPort   = 8080
	defaultMaxBatchSize = 16
	defaultParallelism  = 4
)

type publishOptions struct {
	eventbus string
}

func defaultPublishOptions() *publishOptions {
	return &publishOptions{}
}

type subscribeOptions struct {
	subscriptionID         string
	batchSize              int
	port                   int
	activeMode             bool
	protocol               Protocol
	order                  bool
	parallelism            int
	consumeTimeoutPerBatch time.Duration
}

func defaultSubscribeOptions() *subscribeOptions {
	return &subscribeOptions{
		batchSize:   defaultMaxBatchSize,
		port:        defaultListenPort,
		protocol:    ProtocolGRPC,
		parallelism: defaultParallelism,
	}
}

type PublishOption func(opt *publishOptions)

type SubscribeOption func(opt *subscribeOptions)

func WithEventbus(eb string) PublishOption {
	return func(opt *publishOptions) {
		opt.eventbus = eb
	}
}

func WithSubscriptionID(id string) SubscribeOption {
	return func(opt *subscribeOptions) {
		opt.subscriptionID = id
	}
}

func WithMaxBatchSize(size int) SubscribeOption {
	return func(opt *subscribeOptions) {
		opt.batchSize = size
	}
}

func WithActiveMode(is bool) SubscribeOption {
	return func(opt *subscribeOptions) {
		opt.activeMode = true
	}
}

func WithListenPort(port int) SubscribeOption {
	return func(opt *subscribeOptions) {
		opt.port = port
	}
}

func WithProtocol(p Protocol) SubscribeOption {
	return func(opt *subscribeOptions) {
		opt.protocol = p
	}
}

func WithOrder(is bool) SubscribeOption {
	return func(opt *subscribeOptions) {
		opt.order = is
	}
}

func WithParallelism(n int) SubscribeOption {
	return func(opt *subscribeOptions) {
		opt.parallelism = n
	}
}

func WithConsumeTimeout(t time.Duration) SubscribeOption {
	return func(opt *subscribeOptions) {
		opt.consumeTimeoutPerBatch = t
	}
}
