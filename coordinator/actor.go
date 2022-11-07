package coordinator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/sourcegraph/jsonrpc2"
	"tbx.at/salmonide"
)

type coordinator struct {
	handlingWorkers      map[salmonide.PeerID]*salmonide.Job
	handlingWorkersMutex sync.RWMutex
	jobs                 []*salmonide.Job
	jobsMutex            sync.RWMutex
}

func (c *coordinator) onPeerConnected(ctx context.Context, peer salmonide.Peer) error {
	if peer.Type == salmonide.ActorTypeRunner {
		c.jobsMutex.Lock()
		c.jobs = append(c.jobs, &salmonide.Job{
			ID:          salmonide.JobID(len(c.jobs)),
			SecretToken: "foo",
			ShellScript: `
	for i in $(seq 1000); do
		echo $i
		sleep .01
	done
	`,
			Image:  "nixos",
			Status: salmonide.JobStatusNotTaken,
		})
		c.jobsMutex.Unlock()

		c.jobsMutex.RLock()
		defer c.jobsMutex.RUnlock()

		for _, job := range c.jobs {
			if job.Status == salmonide.JobStatusNotTaken {
				if err := peer.Conn.Notify(ctx, salmonide.MethodRunnerJobAvailable, job, nil); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func (c *coordinator) handleRequest(ctx context.Context, actor *salmonide.Actor, sender salmonide.PeerID, request *jsonrpc2.Request) (result interface{}, err error) {
	switch request.Method {
	case salmonide.MethodCoordinatorCaptureOutput:
		return c.handleCaptureOutput(ctx, actor, sender, request)
	case salmonide.MethodCoordinatorCompleteJob:
		return c.handleCompleteJob(ctx, actor, sender, request)
	case salmonide.MethodCoordinatorStartJob:
		return c.handleStartJob(ctx, actor, sender, request)
	case salmonide.MethodCoordinatorTakeJob:
		return c.handleTakeJob(ctx, actor, sender, request)
	}
	return nil, salmonide.NewMethodNotFoundError(request.Method)
}

func (c *coordinator) handleCaptureOutput(ctx context.Context, actor *salmonide.Actor, sender salmonide.PeerID, request *jsonrpc2.Request) (result interface{}, err error) {
	if err := salmonide.MustBeNotification(request); err != nil {
		return false, err
	}

	params, err := salmonide.ParseParams[salmonide.OutputChunk](request)
	if err != nil {
		return nil, err
	}

	c.handlingWorkersMutex.Lock()
	c.jobsMutex.Lock()
	job, ok := c.handlingWorkers[sender]
	defer c.handlingWorkersMutex.Unlock()
	defer c.jobsMutex.Unlock()
	if !ok {
		return nil, errors.New("no job found that is handled by this peer")
	}

	job.Output = append(job.Output, params)

	// TODO: Remove debug output
	fmt.Fprintln(os.Stderr)
	for _, c := range job.Output {
		fmt.Fprint(os.Stderr, string(c.Content))
	}
	fmt.Fprintln(os.Stderr)
	// End of debug output

	return
}

func (c *coordinator) handleCompleteJob(ctx context.Context, actor *salmonide.Actor, sender salmonide.PeerID, request *jsonrpc2.Request) (result interface{}, err error) {
	if err := salmonide.MustBeRequest(request); err != nil {
		return false, err
	}

	params, err := salmonide.ParseParams[salmonide.CoordinatorCompleteJobParams](request)
	if err != nil {
		return nil, err
	}

	c.jobsMutex.Lock()
	defer c.jobsMutex.Unlock()
	for _, job := range c.jobs {
		if job.ID == params.JobID {
			job.Status = salmonide.JobStatusDone
			job.ExitCode = params.ExitCode
			c.handlingWorkersMutex.Lock()
			defer c.handlingWorkersMutex.Unlock()
			delete(c.handlingWorkers, sender)
			return
		}
	}

	return result, &jsonrpc2.Error{
		Code:    jsonrpc2.CodeInvalidRequest,
		Message: "job not found",
	}
}

func (c *coordinator) handleStartJob(ctx context.Context, actor *salmonide.Actor, sender salmonide.PeerID, request *jsonrpc2.Request) (result *salmonide.Job, err error) {
	if err := salmonide.MustBeRequest(request); err != nil {
		return result, err
	}

	params, err := salmonide.ParseParams[salmonide.JobID](request)
	if err != nil {
		return result, err
	}

	c.jobsMutex.Lock()
	defer c.jobsMutex.Unlock()
	for _, job := range c.jobs {
		if job.ID == params {
			if job.Status != salmonide.JobStatusTaken {
				return result, &jsonrpc2.Error{
					Code:    jsonrpc2.CodeInvalidRequest,
					Message: "bad job state",
				}
			}
			job.Status = salmonide.JobStatusRunning
			c.handlingWorkersMutex.Lock()
			defer c.handlingWorkersMutex.Unlock()
			c.handlingWorkers[sender] = job
			return job, nil
		}
	}

	return result, &jsonrpc2.Error{
		Code:    jsonrpc2.CodeInvalidRequest,
		Message: "job not found",
	}
}

func (c *coordinator) handleTakeJob(ctx context.Context, actor *salmonide.Actor, sender salmonide.PeerID, request *jsonrpc2.Request) (result bool, err error) {
	if err := salmonide.MustBeRequest(request); err != nil {
		return false, err
	}

	params, err := salmonide.ParseParams[salmonide.JobID](request)
	if err != nil {
		return false, err
	}

	c.jobsMutex.Lock()
	defer c.jobsMutex.Unlock()
	for _, job := range c.jobs {
		if job.ID == params {
			if job.Status != salmonide.JobStatusNotTaken {
				return false, nil
			}
			job.Status = salmonide.JobStatusTaken
			return true, nil
		}
	}

	return false, &jsonrpc2.Error{
		Code:    jsonrpc2.CodeInvalidRequest,
		Message: "job not found",
	}
}

func NewCoordinator() *salmonide.Actor {
	c := coordinator{
		handlingWorkers: map[salmonide.PeerID]*salmonide.Job{},
	}
	actor := salmonide.NewActor("coordinator", salmonide.ActorTypeCoordinator, c.handleRequest)
	actor.OnPeerConnected = c.onPeerConnected
	return actor
}
