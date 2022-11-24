package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/sourcegraph/jsonrpc2"
	"tbx.at/salmonide"
	"tbx.at/salmonide/service"
)

type worker struct {
	*salmonide.Actor
	services service.Services

	job salmonide.Job
}

func NewWorker(services service.Services, jobID salmonide.JobID) *salmonide.Actor {
	name := fmt.Sprintf("worker-%d", time.Now().Nanosecond())

	w := &worker{
		services: services,

		job: salmonide.Job{
			ID: jobID,
		},
	}
	w.Actor = salmonide.NewActor(services.LogWriter, name, salmonide.ActorTypeWorker, w.onConnect, nil, w.onDisconnect, w.handleRequest)

	return w.Actor
}

func (w *worker) onConnect(ctx context.Context, peer *salmonide.Peer) error {
	if err := peer.Conn.Call(ctx, salmonide.MethodCoordinatorStartJob, w.job.ID, nil); err != nil {
		return err
	}

	var job salmonide.Job
	if err := peer.Conn.Call(ctx, salmonide.MethodCoordinatorJob, w.job.ID, &job); err != nil {
		return err
	}

	if err := w.services.Exec.Run(ctx, peer.Conn, job); err != nil {
		return err
	}

	go w.Exit(nil)

	return nil
}

func (w *worker) onDisconnect(ctx context.Context, peer *salmonide.Peer) error {
	if peer.Type == salmonide.ActorTypeCoordinator {
		// TODO: If disconnected with error, return error here (need to pass error to disconnect callback).
		go w.Exit(nil)
	}
	return nil
}

func (w *worker) handleRequest(ctx context.Context, sender *salmonide.Peer, request *jsonrpc2.Request) (result interface{}, err error) {
	return nil, salmonide.NewMethodNotFoundError(request.Method)
}
