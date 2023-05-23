package vanus

import (
	// standard libraries.
	"context"

	// third-party libraries.
	"google.golang.org/grpc"

	// first-party libraries.
	"github.com/vanus-labs/vanus/pkg/errors"
)

func UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req interface{}, reply interface{},
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
	) error {
		err := invoker(ctx, method, req, reply, cc, opts...)
		if et, ok := errors.FromError(err); ok && et != nil {
			return et
		}
		return err
	}
}
