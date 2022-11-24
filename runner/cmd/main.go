package main

import (
	"context"
	"log"
	"net"
	"os"
	"path/filepath"

	"golang.org/x/sync/errgroup"
	"tbx.at/salmonide/runner"
	"tbx.at/salmonide/service"
)

const (
	coordinatorNetwork = "tcp"
	coordinatorAddress = "127.0.0.1:3001"
)

func main() {
	eg, ctx := errgroup.WithContext(context.Background())
	defer eg.Wait()

	services := service.Services{
		LogWriter: os.Stderr,

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
		if err != nil {
			log.Printf("error proxying connections between workers and coordinator: %s\n", err)
		}
		return err
	})

	// cwd, err := os.Getwd()
	// if err != nil {
	// 	panic(err)
	// }
	// cwd = filepath.Join(cwd, "testdata")
	cwd := filepath.Join("/tmp", "salmonide")
	diskDir := filepath.Join(cwd, "disks")
	imageDir := filepath.Join(cwd, "images")
	tokenDir := filepath.Join(cwd, "tokens")

	r := runner.NewRunner(services, diskDir, imageDir, tokenDir)
	defer r.Close()
	eg.Go(func() error {
		return r.ProcessMessages(ctx)
	})

	r.ConnectPeer(ctx, coordinatorConn, nil)

	if err := eg.Wait(); err != nil {
		panic(err)
	}
}
