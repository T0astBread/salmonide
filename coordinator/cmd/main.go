package main

import (
	"context"
	"net"

	"tbx.at/salmonide"
	"tbx.at/salmonide/coordinator"
	"tbx.at/salmonide/service"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, cleanup, err := salmonide.OpenDB(ctx, "db.sqlite", coordinator.DBMigrations)
	if cleanup != nil {
		defer cleanup()
	}
	if err != nil {
		panic(err)
	}

	services := service.Services{
		DB: db,
	}

	listenAddr, err := net.ResolveTCPAddr("tcp4", "127.0.0.1:3001")
	if err != nil {
		panic(err)
	}
	listener, err := net.ListenTCP("tcp4", listenAddr)
	if err != nil {
		panic(err)
	}

	c, err := coordinator.NewCoordinator(ctx, services)
	if err != nil {
		panic(err)
	}

	if err := salmonide.ListenNet(ctx, listener, c); err != nil {
		panic(err)
	}
}
