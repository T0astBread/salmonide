package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sourcegraph/jsonrpc2"
	"tbx.at/salmonide"
	"tbx.at/salmonide/service"
)

type Worker struct {
	services service.Services

	job      salmonide.Job
	jobMutex sync.RWMutex
}

func NewWorker(services service.Services, jobID salmonide.JobID) *salmonide.Actor {
	name := fmt.Sprintf("worker-%d", time.Now().Nanosecond())

	w := &Worker{
		services: services,

		job: salmonide.Job{
			ID: jobID,
		},
		jobMutex: sync.RWMutex{},
	}
	actor := salmonide.NewActor(name, salmonide.ActorTypeWorker, w.handleRequest)

	return actor
}

func (w *Worker) handleRequest(ctx context.Context, actor *salmonide.Actor, sender salmonide.PeerID, request *jsonrpc2.Request) (result interface{}, err error) {
	return nil, salmonide.NewMethodNotFoundError(request.Method)
}
