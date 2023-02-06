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

	client "github.com/linkall-labs/sdk/golang"
)

func main() {
	ctx := context.Background()
	opts := &client.ClientOptions{
		Endpoint: "172.17.0.2:30001",
	}
	c, err := client.Connect(opts)
	if err != nil {
		panic("connect error")
	}

	s, err := c.Subscriber(&client.SubscribeOptions{
		SubscriptionID: "0000002689000012",
	})
	if err != nil {
		panic("new subscriber error")
	}

	messagec, err := s.Subscribe(ctx)
	if err != nil {
		fmt.Printf("subscribe failed, err: %s\n", err.Error())
		return
	}
	for {
		select {
		case msg, ok := <-messagec:
			if !ok {
				fmt.Println("messagec closed, exit.")
				return
			}
			fmt.Printf("received a message, event: %s\n", msg.GetEvent().String())
		case <-ctx.Done():
			fmt.Println("ctx exit")
			return
		}
	}
}
