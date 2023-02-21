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
	"io"
	"time"
)

func main() {
	opts := &vanus.ClientOptions{
		Endpoint: "172.17.0.2:30001",
	}

	c, err := vanus.Connect(opts)
	if err != nil {
		panic("failed to connect to Vanus cluster, error: " + err.Error())
	}

	s := c.Subscriber(
		vanus.WithSubscriptionID("your_subscription_id"),
		vanus.WithListenPort(18080),
		vanus.WithProtocol(vanus.ProtocolGRPC),
		vanus.WithParallelism(8),
		vanus.WithConsumeTimeout(time.Second),
	)
	defer func() {
		_ = s.Close()
	}()

	err = s.Listen(func(ctx context.Context, msgs ...vanus.Message) error {
		for _, msg := range msgs {
			fmt.Printf("received a message, event: %s\n", msg.GetEvent().String())
			msg.Success()
		}
		return nil
	})

	if err != io.EOF {
		fmt.Printf("subscribe failed, err: %s\n", err.Error())
		return
	}
}
