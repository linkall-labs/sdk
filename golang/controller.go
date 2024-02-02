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
	proxypb "github.com/vanus-labs/vanus/api/proxy"
)

func (c *client) Controller() Controller {
	return &controller{controller: c.controller}
}

type controller struct {
	controller proxypb.ControllerProxyClient
}

func (c *controller) Event() Event {
	return &event{controller: c.controller}
}

func (c *controller) Eventbus() Eventbus {
	return &eventbus{controller: c.controller}
}

func (c *controller) Namespace() Namespace {
	return &namespace{controller: c.controller}
}

func (c *controller) Subscription() Subscription {
	return &subscription{controller: c.controller}
}
