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

package golang

import (
	"context"

	v2 "github.com/cloudevents/sdk-go/v2"
	metapb "github.com/linkall-labs/vanus/proto/pkg/meta"
)

type Client interface {
	Publisher(opts *PublishOptions) Publisher
	Subscriber(opts *SubscribeOptions) (Subscriber, error)
	Controller() Controller
	Disconnect() error
}

type Publisher interface {
	Eventbus() string
	Publish(ctx context.Context, events ...*v2.Event) error
}

type Subscriber interface {
	SubscriptionID() string
	Subscribe(ctx context.Context) (<-chan Message, error)
}

type Message interface {
	GetEvent() *v2.Event
	Success() error
	Failed(err error) error
}

type PublishOptions struct {
	Eventbus string
}

type SubscribeOptions struct {
	SubscriptionID string
}

type Controller interface {
	Eventbus(name string) Eventbus
	Subscription(id string) Subscription
}

type Eventbus interface {
	List() ([]*metapb.EventBus, error)
	Get() (*metapb.EventBus, error)
	Create() error
	Delete() error
}

type Subscription interface {
	List() ([]*metapb.Subscription, error)
	Get() (*metapb.Subscription, error)
	Create() error
	Delete() error
	Pause() error
	Resume() error
}
