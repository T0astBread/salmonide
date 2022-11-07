package main

import (
	"context"
	"net"

	"tbx.at/salmonide"
	"tbx.at/salmonide/coordinator"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	listenAddr, err := net.ResolveTCPAddr("tcp4", "127.0.0.1:3001")
	if err != nil {
		panic(err)
	}
	listener, err := net.ListenTCP("tcp4", listenAddr)
	if err != nil {
		panic(err)
	}

	c := coordinator.NewCoordinator()
	if err := salmonide.ListenNet(ctx, listener, c); err != nil {
		panic(err)
	}
}
