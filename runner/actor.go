package runner

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/sourcegraph/jsonrpc2"
	"tbx.at/salmonide"
	"tbx.at/salmonide/service"
)

type runner struct {
	services service.Services

	diskDirectory, imageDirectory, tokenDirectory string
	runningJob                                    *salmonide.Job
	runningJobMutex                               sync.RWMutex
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
	return salmonide.NewActor(name, salmonide.ActorTypeRunner, r.handleRequest)
}

func (r *runner) handleRequest(ctx context.Context, actor *salmonide.Actor, sender salmonide.PeerID, request *jsonrpc2.Request) (result interface{}, err error) {
	switch request.Method {
	case salmonide.MethodRunnerJobAvailable:
		return r.handleJobAvailable(ctx, actor, sender, request)
	}
	return nil, salmonide.NewMethodNotFoundError(request.Method)
}

func (r *runner) handleJobAvailable(ctx context.Context, actor *salmonide.Actor, sender salmonide.PeerID, request *jsonrpc2.Request) (result interface{}, err error) {
	if err := salmonide.MustBeNotification(request); err != nil {
		return nil, err
	}

	r.runningJobMutex.RLock()
	hasJob := r.runningJob != nil
	r.runningJobMutex.RUnlock()
	if hasJob {
		return
	}

	job, err := salmonide.ParseParams[salmonide.Job](request)
	if err != nil {
		return nil, err
	}

	senderPeer, err := actor.GetSender(sender)
	if err != nil {
		return nil, err
	}

	var gotJob bool
	if err := senderPeer.Conn.Call(ctx, salmonide.MethodCoordinatorTakeJob, job.ID, &gotJob); err != nil {
		return nil, err
	}

	if !gotJob {
		return
	}

	r.runningJobMutex.Lock()
	r.runningJob = &job
	r.runningJobMutex.Unlock()

	tokenFilePath, cleanTokenFile, err := r.services.TokenFile.Write(r.tokenDirectory, job)
	if cleanTokenFile != nil {
		defer cleanTokenFile()
	}
	if err != nil {
		return nil, err
	}

	diskFilePath, err := r.services.DiskFile.Create(r.imageDirectory, r.diskDirectory, job)
	if err != nil {
		return nil, err
	}

	return nil, r.services.Qemu.StartVM(ctx, diskFilePath, tokenFilePath)
}
