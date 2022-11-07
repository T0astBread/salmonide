package service

import (
	"context"
	"io"
	"os/exec"
	"time"

	"github.com/sourcegraph/jsonrpc2"
	"tbx.at/salmonide"
)

type ExecService interface {
	Run(ctx context.Context, coordinator *jsonrpc2.Conn, job salmonide.Job) error
}

type sendWriter struct {
	coordinator *jsonrpc2.Conn
	ctx         context.Context
	stream      salmonide.OutputStream
}

// Write implements io.Writer
func (w *sendWriter) Write(p []byte) (n int, err error) {
	chunk := salmonide.OutputChunk{
		Content:   p,
		Stream:    w.stream,
		Timestamp: time.Now(),
	}
	return len(p), w.coordinator.Notify(w.ctx, salmonide.MethodCoordinatorCaptureOutput, chunk)
}

var _ io.Writer = (*sendWriter)(nil)

type ExecServiceImpl struct{}

// Run implements ExecService
func (*ExecServiceImpl) Run(ctx context.Context, coordinator *jsonrpc2.Conn, job salmonide.Job) error {
	shell := exec.CommandContext(ctx, "bash")

	stdin, err := shell.StdinPipe()
	if err != nil {
		return err
	}
	if _, err := stdin.Write([]byte(job.ShellScript)); err != nil {
		return err
	}
	if err := stdin.Close(); err != nil {
		return err
	}

	shell.Stdout = &sendWriter{
		coordinator: coordinator,
		ctx:         ctx,
		stream:      salmonide.OutputStreamStdout,
	}
	shell.Stderr = &sendWriter{
		coordinator: coordinator,
		ctx:         ctx,
		stream:      salmonide.OutputStreamStderr,
	}

	if err := shell.Run(); err != nil {
		if _, ok := err.(*exec.ExitError); !ok {
			return err
		}
	}

	return coordinator.Call(ctx, salmonide.MethodCoordinatorCompleteJob, salmonide.CoordinatorCompleteJobParams{
		JobID:    job.ID,
		ExitCode: shell.ProcessState.ExitCode(),
	}, nil)
}

var _ ExecService = (*ExecServiceImpl)(nil)
