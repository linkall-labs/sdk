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

var (
	_ Subscription = &subscription{}
)

type subscription struct {
	controller proxypb.ControllerProxyClient
}

func (s *subscription) List(ctx context.Context) ([]*metapb.Subscription, error) {
	res, err := s.controller.ListSubscription(ctx, &ctrlpb.ListSubscriptionRequest{})
	if err != nil {
		return nil, err
	}
	return res.GetSubscription(), nil
}

func (s *subscription) Get(ctx context.Context, opts ...SubscriptionOption) (*metapb.Subscription, error) {

	o := newSubscriptionOptions(opts...)

	if o.subscriptionID == 0 {
		return nil, ErrSubscriptionIDIsZero
	}

	return s.controller.GetSubscription(ctx, &ctrlpb.GetSubscriptionRequest{Id: uint64(o.subscriptionID)})
}

func (s *subscription) Create(ctx context.Context, request *ctrlpb.SubscriptionRequest) (*metapb.Subscription, error) {
	return s.controller.CreateSubscription(ctx, &ctrlpb.CreateSubscriptionRequest{
		Subscription: request,
	})
}

func (s *subscription) Update(ctx context.Context,
	request *ctrlpb.UpdateSubscriptionRequest) (*metapb.Subscription, error) {
	return s.controller.UpdateSubscription(ctx, request)
}

func (s *subscription) Delete(ctx context.Context, opts ...SubscriptionOption) error {
	o := newSubscriptionOptions(opts...)

	if o.subscriptionID == 0 {
		return ErrSubscriptionIDIsZero
	}
	_, err := s.controller.DeleteSubscription(ctx, &ctrlpb.DeleteSubscriptionRequest{
		Id: uint64(o.subscriptionID),
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

func (s *subscription) Pause(ctx context.Context, opts ...SubscriptionOption) error {
	o := newSubscriptionOptions(opts...)

	if o.subscriptionID == 0 {
		return ErrSubscriptionIDIsZero
	}
	_, err := s.controller.DisableSubscription(ctx, &ctrlpb.DisableSubscriptionRequest{
		Id: uint64(o.subscriptionID),
	})
	if err != nil {
		return err
	}
	return nil
}

func (s *subscription) Resume(ctx context.Context, opts ...SubscriptionOption) error {
	o := newSubscriptionOptions(opts...)

	if o.subscriptionID == 0 {
		return ErrSubscriptionIDIsZero
	}
	_, err := s.controller.ResumeSubscription(ctx, &ctrlpb.ResumeSubscriptionRequest{
		Id: uint64(o.subscriptionID),
	})
	if err != nil {
		return err
	}
	return nil
}
