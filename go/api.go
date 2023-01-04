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
	v2 "github.com/cloudevents/sdk-go/v2"
)

type Client interface {
	Send(eventbusName string, events ...*v2.Event) error
	Subscribe(subscriptionID uint64) (<-chan Message, error)
	Close() error
}

type Message interface {
	GetEvent() *v2.Event
	Success() error
	Failed(err error) error
}
