package main

import (
	"context"
	"fmt"

	gocli "github.com/linkall-labs/sdk/golang"
)

func main() {
	cfg := gocli.Config{
		Endpoint: "172.17.0.2:30001",
	}
	c := gocli.New(&cfg)

	ctx := context.Background()

	messagec, err := c.Subscribe("000018FD28000011")
	if err != nil {
		fmt.Printf("subscribe failed, err: %s\n", err.Error())
		return
	}
	for {
		select {
		case msg := <-messagec:
			fmt.Printf("received a message, event: %s\n", msg.GetEvent().String())
		case <-ctx.Done():
			fmt.Println("ctx exit")
			return
		}
	}
	fmt.Printf("send event success\n")
}
