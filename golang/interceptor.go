package vanus

import (
	"context"

	"google.golang.org/grpc"

	"github.com/vanus-labs/vanus/pkg/errors"
)

func UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req interface{}, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		err := invoker(ctx, method, req, reply, cc, opts...)
		if et, ok := errors.FromError(err); ok {
			return et
		}
		return err
	}
}
