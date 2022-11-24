package service

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/sourcegraph/jsonrpc2"
	"tbx.at/salmonide"
)

type PingService interface {
	Ping(ctx context.Context, network, address string) error
}

type pingServiceHandler struct{}

// Handle implements jsonrpc2.Handler
func (*pingServiceHandler) Handle(ctx context.Context, conn *jsonrpc2.Conn, request *jsonrpc2.Request) {
	if !request.Notif {
		conn.ReplyWithError(ctx, request.ID, salmonide.NewMethodNotFoundError(request.Method))
	}
}

var _ jsonrpc2.Handler = (*pingServiceHandler)(nil)

type PingServiceImpl struct{}

// Ping implements PingService
func (*PingServiceImpl) Ping(ctx context.Context, network string, address string) error {
	netConn, err := net.Dial(network, address)
	if err != nil {
		return err
	}
	defer netConn.Close()

	stream := jsonrpc2.NewBufferedStream(netConn, jsonrpc2.VarintObjectCodec{})
	conn := jsonrpc2.NewConn(ctx, stream, &pingServiceHandler{})
	defer conn.Close()

	pingCtx, cancelPingCtx := context.WithTimeout(ctx, 5*time.Second)
	defer cancelPingCtx()
	var response string
	if err := conn.Call(pingCtx, salmonide.MethodPing, nil, &response); err != nil {
		return err
	}

	if response != "pong" {
		return errors.New("response to ping wasn't \"pong\"")
	}
	return nil
}

var _ PingService = (*PingServiceImpl)(nil)
