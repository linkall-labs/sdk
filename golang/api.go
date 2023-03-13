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
	"io"
	"strconv"

	v2 "github.com/cloudevents/sdk-go/v2"

	metapb "github.com/vanus-labs/vanus/proto/pkg/meta"
)

type Client interface {
	Publisher(opts ...EventbusOption) Publisher
	Subscriber(opts ...SubscriptionOption) Subscriber
	Controller() Controller
	Disconnect() error
}

type Publisher interface {
	io.Closer
	Eventbus() string
	Publish(ctx context.Context, events ...*v2.Event) error
}

type Subscriber interface {
	io.Closer
	SubscriptionID() ID
	Listen(handler func(ctx context.Context, msgs ...Message) error) error
}

type Message interface {
	GetEvent() *v2.Event
	Success()
	Failed(err error)
}

type Controller interface {
	Eventbus(opts ...EventbusOption) Eventbus
	Subscription(opts ...SubscriptionOption) Subscription
}

type Eventbus interface {
	List(ctx context.Context) ([]*metapb.Eventbus, error)
	Get(ctx context.Context) (*metapb.Eventbus, error)
	Create(ctx context.Context) error
	Delete(ctx context.Context) error
}

type Subscription interface {
	List(ctx context.Context) ([]*metapb.Subscription, error)
	Get(ctx context.Context) (*metapb.Subscription, error)
	Create(ctx context.Context) error
	Delete(ctx context.Context) error
	Pause(ctx context.Context) error
	Resume(ctx context.Context) error
}

type ID uint64

func (id ID) Hex() string {
	return fmt.Sprintf("%016X", id)
}

func NewID(id uint64) ID {
	return ID(id)
}

func NewIDFromHex(id string) (ID, error) {
	i, err := strconv.ParseInt(id, 16, 64)
	return ID(i), err
}
