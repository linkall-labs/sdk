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
	// standard libraries.
	"context"

	// third-party libraries.

	// first-party libraries.
	proxypb "github.com/vanus-labs/vanus/proto/pkg/proxy"
)

type event struct {
	controller proxypb.ControllerProxyClient
}

// Make sure eventbus implements Eventbus.
var _ Event = (*event)(nil)

func (e *event) Get(ctx context.Context, opts ...EventOption) (*proxypb.GetEventResponse, error) {
	o := newEventOptions(opts...)
	if o.eventbusID == 0 {
		return nil, ErrEventbusIsZero
	}
	if o.eventID == "" && o.number == 0 {
		return nil, ErrInvalidArguments
	}
	return e.controller.GetEvent(ctx, &proxypb.GetEventRequest{
		EventbusId: o.eventbusID,
		EventId:    o.eventID,
		Offset:     o.offset,
		Number:     o.number,
	})
}
