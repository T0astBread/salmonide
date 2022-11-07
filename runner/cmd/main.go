package main

import (
	"context"
	"log"
	"net"
	"os"
	"path/filepath"

	"golang.org/x/sync/errgroup"
	"tbx.at/salmonide"
	"tbx.at/salmonide/runner"
	"tbx.at/salmonide/service"
)

const (
	coordinatorNetwork = "tcp"
	coordinatorAddress = "127.0.0.1:3001"
)

func main() {
	bctx, cancel := context.WithCancel(context.Background())
	eg, ctx := errgroup.WithContext(bctx)
	defer eg.Wait()

	services := service.Services{
		DiskFile:    &service.DiskFileServiceImpl{},
		Qemu:        &service.QemuServiceImpl{},
		TokenFile:   &service.TokenFileServiceImpl{},
		WorkerProxy: &service.WorkerProxyServiceImpl{},
	}

	coordinatorConn, err := net.Dial(coordinatorNetwork, coordinatorAddress)
	if err != nil {
		panic(err)
	}
	defer coordinatorConn.Close()

	eg.Go(func() error {
		err := services.WorkerProxy.Run(ctx, coordinatorNetwork, coordinatorAddress)
		log.Printf("error proxying connections between workers and coordinator: %s\n", err)
		return err
	})

	listenAddr, err := net.ResolveTCPAddr("tcp4", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	listener, err := net.ListenTCP("tcp4", listenAddr)
	if err != nil {
		panic(err)
	}
	defer listener.Close()

	cwd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	cwd = filepath.Join(cwd, "testdata")
	diskDir := filepath.Join(cwd, "disks")
	imageDir := filepath.Join(cwd, "images")
	tokenDir := filepath.Join(cwd, "tokens")

	r := runner.NewRunner(services, diskDir, imageDir, tokenDir)
	eg.Go(func() error {
		return salmonide.ListenNet(ctx, listener, r)
	})

	coord, err := r.ConnectPeer(ctx, coordinatorConn)
	if err != nil {
		panic(err)
	}
	defer coord.Conn.Close()

	<-coord.Conn.DisconnectNotify()
	cancel()

	if err := r.GetDisconnectError(coord.ID); err != nil {
		panic(err)
	}

	if err := listener.Close(); err != nil {
		panic(err)
	}

	if err := eg.Wait(); err != nil {
		panic(err)
	}
}
