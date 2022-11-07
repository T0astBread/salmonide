package service

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	"golang.org/x/sync/errgroup"
)

type WorkerProxyService interface {
	Run(ctx context.Context, coordinatorNetwork, coordinatorAddress string) error
}

type WorkerProxyServiceImpl struct{}

// Run implements WorkerProxyService
func (*WorkerProxyServiceImpl) Run(ctx context.Context, coordinatorNetwork, coordinatorAddress string) error {
	listenAddr, err := net.ResolveTCPAddr("tcp4", "127.0.0.1:3000")
	if err != nil {
		return err
	}

	listener, err := net.ListenTCP("tcp4", listenAddr)
	if err != nil {
		if strings.Contains(err.Error(), "address already in use") {
			return nil
		}
		return err
	}
	defer listener.Close()

	eg, egCtx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		<-egCtx.Done()
		return listener.Close()
	})

	eg.Go(func() error {
		for {
			srcConn, err := listener.Accept()
			if err != nil {
				if ctx.Err() == nil {
					return err
				}
				return nil
			}
			defer srcConn.Close()

			dstConn, err := net.Dial(coordinatorNetwork, coordinatorAddress)
			if err != nil {
				return err
			}
			defer dstConn.Close()

			copiers, cctx := errgroup.WithContext(ctx)

			copiers.Go(func() error {
				_, err := io.Copy(dstConn, srcConn)
				if cctx.Err() == nil {
					return err
				}
				return nil
			})
			copiers.Go(func() error {
				_, err := io.Copy(srcConn, dstConn)
				if cctx.Err() == nil {
					return err
				}
				return nil
			})

			go func() {
				if err := copiers.Wait(); err != nil {
					fmt.Fprintf(os.Stderr, "error proxying worker connection: %s", err.Error())
				}
				_ = srcConn.Close()
				_ = dstConn.Close()
			}()
		}
	})

	return eg.Wait()
}

var _ WorkerProxyService = (*WorkerProxyServiceImpl)(nil)
