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

	v2 "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"
	"github.com/linkall-labs/sdk/golang"
)

func main() {
	opts := &vanus.ClientOptions{
		Endpoint: "172.17.0.2:30001",
	}

	c, err := vanus.Connect(opts)
	if err != nil {
		panic("failed to connect to Vanus cluster, error: " + err.Error())
	}

	p := c.Publisher(vanus.WithEventbus("quick-start"))

	event := v2.NewEvent()
	event.SetID(uuid.New().String())
	event.SetSource("example-source")
	event.SetType("example-type")
	_ = event.SetData(v2.ApplicationJSON, map[string]string{"hello": "world"})
	err = p.Publish(context.Background(), &event)
	if err != nil {
		fmt.Printf("publish event failed, err: %s\n", err.Error())
		return
	}
	fmt.Printf("publish event success\n")
}
