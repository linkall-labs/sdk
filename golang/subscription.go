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

package golang

import (
	"context"

	ctrlpb "github.com/linkall-labs/vanus/proto/pkg/controller"
	metapb "github.com/linkall-labs/vanus/proto/pkg/meta"
	proxypb "github.com/linkall-labs/vanus/proto/pkg/proxy"
)

type subscription struct {
	id         string
	controller proxypb.ControllerProxyClient
}

func (s *subscription) List() ([]*metapb.Subscription, error) {
	res, err := s.controller.ListSubscription(context.Background(), &ctrlpb.ListSubscriptionRequest{})
	if err != nil {
		return nil, err
	}
	return res.GetSubscription(), nil
}

func (s *subscription) Get() (*metapb.Subscription, error) {
	id, err := NewIDFromString(s.id)
	if err != nil {
		return nil, err
	}
	return s.controller.GetSubscription(context.Background(), &ctrlpb.GetSubscriptionRequest{Id: id})
}

func (s *subscription) Create() error {
	_, err := s.controller.CreateSubscription(context.Background(), &ctrlpb.CreateSubscriptionRequest{
		Subscription: &ctrlpb.SubscriptionRequest{},
	})
	if err != nil {
		return err
	}
	return nil
}

func (s *subscription) Delete() error {
	id, err := NewIDFromString(s.id)
	if err != nil {
		return err
	}
	_, err = s.controller.DeleteSubscription(context.Background(), &ctrlpb.DeleteSubscriptionRequest{Id: id})
	if err != nil {
		return err
	}
	return nil
}

// func (s *subscription) Update() error {
// 	id, err := NewIDFromString(s.id)
// 	if err != nil {
// 		return err
// 	}
// 	_, err = s.controller.UpdateSubscription(context.Background(), &ctrlpb.UpdateSubscriptionRequest{
// 		Id:           id,
// 		Subscription: &ctrlpb.SubscriptionRequest{},
// 	})
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }

func (s *subscription) Pause() error {
	id, err := NewIDFromString(s.id)
	if err != nil {
		return err
	}
	_, err = s.controller.DisableSubscription(context.Background(), &ctrlpb.DisableSubscriptionRequest{
		Id: id,
	})
	if err != nil {
		return err
	}
	return nil
}

func (s *subscription) Resume() error {
	id, err := NewIDFromString(s.id)
	if err != nil {
		return err
	}
	_, err = s.controller.ResumeSubscription(context.Background(), &ctrlpb.ResumeSubscriptionRequest{
		Id: id,
	})
	if err != nil {
		return err
	}
	return nil
}
