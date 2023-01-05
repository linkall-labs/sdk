package main

import (
	"fmt"

	v2 "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"
	gocli "github.com/linkall-labs/sdk/golang"
)

func main() {
	cfg := gocli.Config{
		Endpoint: "172.17.0.2:30001",
	}
	c := gocli.New(&cfg)

	event := v2.NewEvent()
	event.SetID(uuid.New().String())
	event.SetSource("event-source")
	event.SetType("event-type")
	event.SetData(v2.ApplicationJSON, map[string]string{"hello": "world"})
	err := c.Send("quick-start", &event)
	if err != nil {
		fmt.Printf("send event failed, err: %s\n", err.Error())
		return
	}
	fmt.Printf("send event success\n")
}
