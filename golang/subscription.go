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
	"context"

	ctrlpb "github.com/vanus-labs/vanus/proto/pkg/controller"
	metapb "github.com/vanus-labs/vanus/proto/pkg/meta"
	proxypb "github.com/vanus-labs/vanus/proto/pkg/proxy"
)

type subscription struct {
	id         string
	controller proxypb.ControllerProxyClient
	meta       *metapb.Subscription
	opt        *subscribeOptions
}

func (s *subscription) List(ctx context.Context) ([]*metapb.Subscription, error) {
	res, err := s.controller.ListSubscription(ctx, &ctrlpb.ListSubscriptionRequest{})
	if err != nil {
		return nil, err
	}
	return res.GetSubscription(), nil
}

func (s *subscription) Get(ctx context.Context) (*metapb.Subscription, error) {
	id, err := NewIDFromString(s.id)
	if err != nil {
		return nil, err
	}
	return s.controller.GetSubscription(ctx, &ctrlpb.GetSubscriptionRequest{Id: id})
}

func (s *subscription) Create(ctx context.Context) error {
	_, err := s.controller.CreateSubscription(ctx, &ctrlpb.CreateSubscriptionRequest{
		Subscription: &ctrlpb.SubscriptionRequest{},
	})
	if err != nil {
		return err
	}
	return nil
}

func (s *subscription) Delete(ctx context.Context) error {
	id, err := NewIDFromString(s.id)
	if err != nil {
		return err
	}
	_, err = s.controller.DeleteSubscription(ctx, &ctrlpb.DeleteSubscriptionRequest{
		Id: id,
	})
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
// 	_, err = s.controller.UpdateSubscription(ctx, &ctrlpb.UpdateSubscriptionRequest{
// 		Id:           id,
// 		Subscription: &ctrlpb.SubscriptionRequest{},
// 	})
// 	if err != nil {
// 		return err
// 	}
// 	return nil
// }

func (s *subscription) Pause(ctx context.Context) error {
	id, err := NewIDFromString(s.id)
	if err != nil {
		return err
	}
	_, err = s.controller.DisableSubscription(ctx, &ctrlpb.DisableSubscriptionRequest{
		Id: id,
	})
	if err != nil {
		return err
	}
	return nil
}

func (s *subscription) Resume(ctx context.Context) error {
	id, err := NewIDFromString(s.id)
	if err != nil {
		return err
	}
	_, err = s.controller.ResumeSubscription(ctx, &ctrlpb.ResumeSubscriptionRequest{
		Id: id,
	})
	if err != nil {
		return err
	}
	return nil
}
