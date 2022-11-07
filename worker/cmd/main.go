package main

import (
	"context"
	"log"
	"net"
	"time"

	"golang.org/x/sync/errgroup"
	"tbx.at/salmonide"
	"tbx.at/salmonide/service"
	"tbx.at/salmonide/worker"
)

const (
	coordinatorNetwork = "tcp4"
	coordinatorAddress = "10.0.2.2:3000"
)

func main() {
	services := service.Services{
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

	jobID, err := services.TokenFile.Read()
	if err != nil {
		panic(err)
	}

	listenAddr, err := net.ResolveTCPAddr("tcp4", "127.0.0.1:3001")
	if err != nil {
		panic(err)
	}
	listener, err := net.ListenTCP("tcp4", listenAddr)
	if err != nil {
		panic(err)
	}
	defer listener.Close()

	w := worker.NewWorker(services, jobID)

	eg.Go(func() error {
		return salmonide.ListenNet(ctx, listener, w)
	})

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

	coordinatorConn, err := net.Dial(coordinatorNetwork, coordinatorAddress)
	if err != nil {
		panic(err)
	}
	defer coordinatorConn.Close()
	coordinator, err := w.ConnectPeer(ctx, coordinatorConn)
	if err != nil {
		panic(err)
	}
	defer coordinator.Conn.Close()

	// var closeCoordinatorConn func() error
	// var coordinator salmonide.Peer
	// for i := 0; ; i++ {
	// 	coord, cleanup, err := func() (salmonide.Peer, func() error, error) {
	// 		conn, err := net.Dial("tcp4", "10.0.2.2:3000")
	// 		if err != nil {
	// 			return salmonide.Peer{}, nil, err
	// 		}
	// 		coord, err := w.ConnectPeer(ctx, conn)
	// 		if err != nil {
	// 			return salmonide.Peer{}, conn.Close, err
	// 		}
	// 		return coord, func() error {
	// 			if err := coord.Conn.Close(); err != nil {
	// 				return err
	// 			}
	// 			return conn.Close()
	// 		}, nil
	// 	}()
	// 	if err != nil {
	// 		log.Printf("error connecting to coordinator: %s\n", err.Error())
	// 		if cleanup != nil {
	// 			cleanup()
	// 		}
	// 		if i >= 10 {
	// 			panic(err)
	// 		}
	// 		log.Println("retrying coordinator connection in 1s...")
	// 		time.Sleep(1 * time.Second)
	// 	}
	// 	closeCoordinatorConn = cleanup
	// 	coordinator = coord
	// 	defer cleanup()
	// 	break
	// }

	var job salmonide.Job
	if err := coordinator.Conn.Call(ctx, salmonide.MethodCoordinatorStartJob, jobID, &job); err != nil {
		panic(err)
	}

	if err := services.Exec.Run(ctx, coordinator.Conn, job); err != nil {
		panic(err)
	}

	if err := coordinator.Conn.Close(); err != nil {
		panic(err)
	}

	<-coordinator.Conn.DisconnectNotify()
	cancel()

	if err := w.GetDisconnectError(coordinator.ID); err != nil {
		panic(err)
	}

	if err := listener.Close(); err != nil {
		panic(err)
	}

	if err := eg.Wait(); err != nil {
		panic(err)
	}

	if err := services.Shutdown.Shutdown(true); err != nil {
		panic(err)
	}
}
