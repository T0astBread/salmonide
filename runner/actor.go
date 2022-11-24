package runner

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/sourcegraph/jsonrpc2"
	"tbx.at/salmonide"
	"tbx.at/salmonide/service"
)

type msgExecutionDone struct {
	err error
}

type runner struct {
	*salmonide.Actor
	services service.Services

	diskDirectory, imageDirectory, tokenDirectory string
	runningJob                                    *salmonide.Job
}

func NewRunner(services service.Services, diskDirectory, imageDirectory, tokenDirectory string) *salmonide.Actor {
	hostname, _ := os.Hostname()
	name := fmt.Sprintf("runner-%s", hostname)

	r := &runner{
		services: services,

		diskDirectory:  diskDirectory,
		imageDirectory: imageDirectory,
		tokenDirectory: tokenDirectory,
	}

	r.Actor = salmonide.NewActor(services.LogWriter, name, salmonide.ActorTypeRunner, nil, r.handleCustomMsg, r.handleDisconnect, r.handleRequest)

	return r.Actor
}

func (r *runner) handleCustomMsg(ctx context.Context, msg interface{}) error {
	switch msg := msg.(type) {
	case msgExecutionDone:
		return r.handleMsgExecutionDone(ctx, msg)
	default:
		return r.NewErrUnknownMessage(msg)
	}
}

func (r *runner) handleMsgExecutionDone(ctx context.Context, msg msgExecutionDone) error {
	if msg.err != nil {
		r.Logger.Printf("execution of job %d finished with error: %s", r.runningJob.ID, msg.err.Error())
	}
	r.runningJob = nil
	return nil
}

func (r *runner) handleDisconnect(ctx context.Context, peer *salmonide.Peer) error {
	if peer.Type == salmonide.ActorTypeCoordinator {
		return errors.New("disconnected from coordinator")
	}

	return nil
}

func (r *runner) handleRequest(ctx context.Context, sender *salmonide.Peer, request *jsonrpc2.Request) (result interface{}, err error) {
	switch request.Method {
	case salmonide.MethodRunnerJobAvailable:
		return r.handleJobAvailable(ctx, sender, request)
	}
	return nil, salmonide.NewMethodNotFoundError(request.Method)
}

func (r *runner) handleJobAvailable(ctx context.Context, sender *salmonide.Peer, request *jsonrpc2.Request) (result interface{}, err error) {
	if err := salmonide.MustBeNotification(request); err != nil {
		return nil, err
	}

	if r.runningJob != nil {
		return
	}

	job, err := salmonide.ParseParams[salmonide.Job](request)
	if err != nil {
		return nil, err
	}

	var gotJob bool
	if err := sender.Conn.Call(ctx, salmonide.MethodCoordinatorTakeJob, job.ID, &gotJob); err != nil {
		return nil, err
	}

	if !gotJob {
		return
	}

	r.runningJob = &job

	go func() {
		var err error
		defer func() {
			r.Message(msgExecutionDone{
				err: err,
			})
		}()

		tokenFilePath, cleanTokenFile, err := r.services.TokenFile.Write(r.tokenDirectory, job)
		if cleanTokenFile != nil {
			defer cleanTokenFile()
		}
		if err != nil {
			return
		}

		diskFilePath, err := r.services.DiskFile.Create(r.imageDirectory, r.diskDirectory, job)
		if err != nil {
			return
		}

		err = r.services.Qemu.StartVM(ctx, diskFilePath, tokenFilePath)
	}()

	return nil, nil
}
