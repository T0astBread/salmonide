package main

import (
	"context"
	"log"
	"net"
	"os"
	"time"

	"golang.org/x/sync/errgroup"
	"tbx.at/salmonide/service"
	"tbx.at/salmonide/worker"
)

const (
	coordinatorNetwork = "tcp4"
	coordinatorAddress = "10.0.2.2:3000"
)

func main() {
	services := service.Services{
		LogWriter: os.Stderr,

		Exec:      &service.ExecServiceImpl{},
		Ping:      &service.PingServiceImpl{},
		Sentinel:  &service.SentinelServiceImpl{},
		Shutdown:  &service.ShutdownServiceImpl{},
		TokenFile: &service.TokenFileServiceImpl{},
	}

	bctx, cancel := context.WithCancel(context.Background())
	eg, ctx := errgroup.WithContext(bctx)
	defer eg.Wait()

	sentinelExists, err := services.Sentinel.SentinelExists()
	if err != nil {
		panic(err)
	}
	if sentinelExists {
		log.Println("Found sentinel file - exiting")
		return
	}

	if err := services.Sentinel.WriteSentinel(); err != nil {
		panic(err)
	}

	defer services.Shutdown.Shutdown(false)

	for i := 0; ; i++ {
		if err := services.Ping.Ping(ctx, coordinatorNetwork, coordinatorAddress); err != nil {
			log.Printf("error pinging coordinator: %s\n", err.Error())
			if i >= 10 {
				panic(err)
			}
			log.Println("retrying coordinator ping in 1s...")
			time.Sleep(1 * time.Second)
		} else {
			break
		}
	}
	log.Println("pinged coordinator")

	jobID, err := services.TokenFile.Read()
	if err != nil {
		panic(err)
	}
	log.Println("read token file; job ID is", jobID)

	coordinatorConn, err := net.Dial(coordinatorNetwork, coordinatorAddress)
	if err != nil {
		panic(err)
	}
	defer coordinatorConn.Close()
	log.Println("opened network connection to coordinator")


	w := worker.NewWorker(services, jobID)
	defer w.Close()
	eg.Go(func() error {
		return w.ProcessMessages(ctx)
	})
	log.Println("started message processing loop")

	w.ConnectPeer(ctx, coordinatorConn, cancel)

	if err := eg.Wait(); err != nil {
		panic(err)
	}
	log.Println("message processing loop exited")

	if err := services.Shutdown.Shutdown(true); err != nil {
		panic(err)
	}
}
