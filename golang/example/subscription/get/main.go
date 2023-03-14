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

package main

import (
	"context"
	"fmt"

	"github.com/vanus-labs/sdk/golang"
)

func main() {
	opts := &vanus.ClientOptions{
		Endpoint: "172.17.0.2:30001",
	}
	c, err := vanus.Connect(opts)
	if err != nil {
		panic("connect error")
	}

	id, err := vanus.NewIDFromHex("0000002689000012")
	if err != nil {
		panic("invalid id")
	}
	res, err := c.Controller().Subscription().Get(context.Background(), vanus.WithSubscriptionID(id))
	if err != nil {
		panic("get subscription error")
	}
	fmt.Printf("get subscription success, subscription: %s\n", res.String())
}
