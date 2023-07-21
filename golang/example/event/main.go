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

package main

import (
	"context"
	"fmt"

	vanus "github.com/vanus-labs/sdk/golang"
)

func main() {
	opts := &vanus.ClientOptions{
		Endpoint: "172.17.0.2:30001",
		Token:    "admin",
	}

	c, err := vanus.Connect(opts)
	if err != nil {
		panic("failed to connect to Vanus cluster, error: " + err.Error())
	}

	eb, err := c.Controller().Eventbus().Get(context.Background(), vanus.WithEventbus("default", "quick-start"))
	if err != nil {
		panic(err)
	}

	es, err := c.Controller().Event().Get(context.Background(), vanus.WithBatchEvents(eb.Id, 0, 1))
	if err != nil {
		panic(err)
	}
	fmt.Printf("event data: %+v\n", es.Events[0].String())
}
