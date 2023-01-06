package golang

import (
	"context"
	"sync"

	v2 "github.com/cloudevents/sdk-go/v2"
	vanuspb "github.com/linkall-labs/sdk/proto/pkg/vanus"
	"github.com/linkall-labs/vanus/proto/pkg/cloudevents"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Config struct {
	Endpoint string
}

type streamState string

var (
	stateRunning streamState = "running"
	stateClosed  streamState = "closed"
)

type streamCache struct {
	subscribeStream vanuspb.Client_SubscribeClient
	ackStream       vanuspb.Client_AckClient
	messagec        chan Message
	state           streamState
}

func newStreamCache(
	subscribeStream vanuspb.Client_SubscribeClient, ackStream vanuspb.Client_AckClient, ch chan Message,
) *streamCache {
	return &streamCache{
		subscribeStream: subscribeStream,
		ackStream:       ackStream,
		messagec:        ch,
		state:           stateRunning,
	}
}

func (sc *streamCache) release() {
	sc.subscribeStream.CloseSend()
	sc.subscribeStream = nil
	sc.ackStream.CloseSend()
	sc.ackStream = nil
	close(sc.messagec)
	sc.state = stateClosed
}

type client struct {
	Endpoint    string
	proxy       vanuspb.ClientClient
	streamCache sync.Map
	mu          sync.Mutex
}

func New(cfg *Config) Client {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpc.Dial(cfg.Endpoint, opts...)
	if err != nil {
		return nil
	}
	return &client{
		Endpoint: cfg.Endpoint,
		proxy:    vanuspb.NewClientClient(conn),
	}
}

func (c *client) Send(eventbusName string, events ...*v2.Event) error {
	eventpb, err := ToProto(events[0])
	if err != nil {
		return err
	}
	in := &vanuspb.PublishRequest{
		EventbusName: eventbusName,
		Events: &cloudevents.CloudEventBatch{
			Events: []*cloudevents.CloudEvent{eventpb},
		},
	}
	_, err = c.proxy.Publish(context.Background(), in)
	if err != nil {
		return err
	}
	return nil
}

func (c *client) Subscribe(subscriptionID string) (<-chan Message, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	value, ok := c.streamCache.Load(subscriptionID)
	if ok && value.(*streamCache).state == stateRunning {
		return value.(*streamCache).messagec, nil
	}

	in := &vanuspb.SubscribeRequest{
		SubscriptionId: subscriptionID,
	}
	stream, err := c.proxy.Subscribe(context.Background(), in)
	if err != nil {
		return nil, err
	}

	ackStream, err := c.proxy.Ack(context.Background())
	if err != nil {
		stream.CloseSend()
		return nil, err
	}

	messageC := make(chan Message, 32)
	cache := newStreamCache(stream, ackStream, messageC)
	c.streamCache.Store(subscriptionID, cache)

	go func(cache *streamCache) {
		for {
			resp, err := cache.subscribeStream.Recv()
			if err != nil {
				cache.release()
				return
			}
			ackFunc := func(result bool) error {
				req := &vanuspb.AckRequest{
					SequenceId:     resp.SequenceId,
					SubscriptionId: subscriptionID,
					Success:        result,
				}
				err = cache.ackStream.Send(req)
				if err != nil {
					cache.release()
					return err
				}
				return nil
			}
			if batch := resp.GetEvents(); batch != nil {
				if eventpbs := batch.GetEvents(); len(eventpbs) > 0 {
					for _, eventpb := range eventpbs {
						event, err2 := FromProto(eventpb)
						if err2 != nil {
							// TODO(jiangkai): check err
							continue
						}
						cache.messagec <- newMessage(ackFunc, event)
					}
				}
			}
		}
	}(cache)
	return messageC, nil
}

func (c *client) Close() error {
	return nil
}

type ackCallback func(result bool) error

type message struct {
	event *v2.Event
	ack   ackCallback
}

func newMessage(cb ackCallback, e *v2.Event) Message {
	return &message{
		event: e,
		ack:   cb,
	}
}

func (m *message) GetEvent() *v2.Event {
	return m.event
}

func (m *message) Success() error {
	return m.ack(true)
}

func (m *message) Failed(err error) error {
	return m.ack(false)
}
