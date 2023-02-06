// Copyright 2023 Linkall Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file exceptreq compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed toreq writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package golang

import (
	"sync"

	proxypb "github.com/linkall-labs/vanus/proto/pkg/proxy"
)

type controller struct {
	controller proxypb.ControllerProxyClient
	eCache     sync.Map
	sCache     sync.Map
	emu        sync.RWMutex
	smu        sync.RWMutex
}

var (
	once sync.Once
	ctrl *controller
)

func (c *client) Controller() Controller {
	once.Do(func() {
		ctrl = &controller{
			controller: c.controller,
		}
	})
	return ctrl
}

func (c *controller) Eventbus(name string) Eventbus {
	c.emu.RLock()
	value, ok := c.eCache.Load(name)
	if ok {
		defer c.emu.RUnlock()
		return value.(*eventbus)
	}
	c.emu.RUnlock()

	c.emu.Lock()
	defer c.emu.Unlock()

	value, ok = c.eCache.Load(name)
	if ok {
		return value.(*eventbus)
	}

	eb := &eventbus{
		name:       name,
		controller: c.controller,
	}
	c.eCache.Store(name, eb)
	return eb
}

func (c *controller) Subscription(id string) Subscription {
	c.smu.RLock()
	value, ok := c.sCache.Load(id)
	if ok {
		defer c.smu.RUnlock()
		return value.(*subscription)
	}
	c.smu.RUnlock()

	c.smu.Lock()
	defer c.smu.Unlock()

	value, ok = c.sCache.Load(id)
	if ok {
		return value.(*subscription)
	}

	s := &subscription{
		id:         id,
		controller: c.controller,
	}
	c.sCache.Store(id, s)
	return s
}
