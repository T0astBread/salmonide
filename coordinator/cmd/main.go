package main

import (
	"context"
	"net"
	"os"
	"path/filepath"

	"golang.org/x/sync/errgroup"
	"tbx.at/salmonide"
	"tbx.at/salmonide/coordinator"
	"tbx.at/salmonide/service"
)

func main() {
	bctx, cancel := context.WithCancel(context.Background())
	eg, ctx := errgroup.WithContext(bctx)
	defer eg.Wait()
	defer cancel()

	dbDir := filepath.Join("/tmp", "salmonide")
	if err := os.MkdirAll(dbDir, 0700); err != nil {
		panic(err)
	}

	db, cleanup, err := salmonide.OpenDB(ctx, filepath.Join(dbDir, "db.sqlite"), coordinator.DBMigrations)
	if cleanup != nil {
		defer cleanup()
	}
	if err != nil {
		panic(err)
	}

	services := service.Services{
		DB:        db,
		LogWriter: os.Stderr,
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
	defer c.Close()
	eg.Go(func() error {
		return c.ProcessMessages(ctx)
	})

	if err := salmonide.ListenNet(ctx, listener, c); err != nil {
		panic(err)
	}

	if err := eg.Wait(); err != nil {
		panic(err)
	}
}
