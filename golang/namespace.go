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
	"google.golang.org/protobuf/types/known/wrapperspb"

	// first-party libraries.
	metapb "github.com/vanus-labs/vanus/proto/pkg/meta"
	proxypb "github.com/vanus-labs/vanus/proto/pkg/proxy"
)

type namespace struct {
	controller proxypb.ControllerProxyClient
}

// Make sure namespace implements Namespace.
var _ Namespace = (*namespace)(nil)

func (ns *namespace) Get(ctx context.Context, name string) (*metapb.Namespace, error) {
	nsRef, err := ns.controller.GetNamespaceWithHumanFriendly(ctx, wrapperspb.String(name))
	if err != nil {
		return nil, err
	}
	return nsRef, nil
}
