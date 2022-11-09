package coordinator

import (
	"context"
	"database/sql"
	"errors"
	"sync"

	"github.com/sourcegraph/jsonrpc2"
	"tbx.at/salmonide"
	"tbx.at/salmonide/service"
)

type coordinator struct {
	services service.Services

	handlingWorkers       map[salmonide.PeerID]salmonide.JobID
	handlingWorkersMutex  sync.RWMutex
	stmtInsertOutputChunk *sql.Stmt
}

func (c *coordinator) onPeerConnected(ctx context.Context, peer salmonide.Peer) error {
	if peer.Type == salmonide.ActorTypeRunner {
		tx, err := c.services.DB.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			insert into jobs (
				image,
				shell_script,
				status
			) values (?, ?, ?)
		`,
			"nixos",
			`for i in $(seq 1000); do
				echo $i
				sleep .01
			done`,
			salmonide.JobStatusNotTaken,
		); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			_ = tx.Rollback()
			return err
		}

		tx, err = c.services.DB.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, "select j.id, j.image, j.shell_script from jobs as j where j.status = ?", salmonide.JobStatusNotTaken)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		for rows.Next() {
			var id int
			var image, shellScript string
			if err := rows.Scan(&id, &image, &shellScript); err != nil {
				_ = tx.Rollback()
				return err
			}
			job := salmonide.Job{
				ID:          salmonide.JobID(id),
				Image:       image,
				ShellScript: shellScript,
			}
			if err := peer.Conn.Notify(ctx, salmonide.MethodRunnerJobAvailable, job, nil); err != nil {
				_ = tx.Rollback()
				return err
			}
		}
		return tx.Commit()
	}

	return nil
}

func (c *coordinator) handleRequest(ctx context.Context, actor *salmonide.Actor, sender salmonide.PeerID, request *jsonrpc2.Request) (result interface{}, err error) {
	switch request.Method {
	case salmonide.MethodCoordinatorCaptureOutput:
		return c.handleCaptureOutput(ctx, actor, sender, request)
	case salmonide.MethodCoordinatorCompleteJob:
		return c.handleCompleteJob(ctx, actor, sender, request)
	case salmonide.MethodCoordinatorJob:
		return c.handleJob(ctx, actor, sender, request)
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

	c.handlingWorkersMutex.RLock()
	jobID, ok := c.handlingWorkers[sender]
	defer c.handlingWorkersMutex.RUnlock()
	if !ok {
		return nil, errors.New("no job found that is handled by this peer")
	}

	if _, err := c.stmtInsertOutputChunk.ExecContext(ctx,
		params.Content,
		jobID,
		params.Stream,
		params.Timestamp,
	); err != nil {
		return nil, err
	}

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

	tx, err := c.services.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	c.handlingWorkersMutex.Lock()
	defer c.handlingWorkersMutex.Unlock()

	updateResult, err := tx.ExecContext(ctx, `
		update jobs as j
		set exit_code = ?,
			status = ?
		where j.id = ? and j.status = ?
	`,
		params.ExitCode,
		salmonide.JobStatusDone,
		params.JobID,
		salmonide.JobStatusRunning,
	)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	rowsAffected, err := updateResult.RowsAffected()
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	if rowsAffected == 0 {
		_ = tx.Rollback()
		return nil, &jsonrpc2.Error{
			Code:    jsonrpc2.CodeInvalidRequest,
			Message: "job not found or not in right state",
		}
	}

	delete(c.handlingWorkers, sender)

	return nil, tx.Commit()
}

func (c *coordinator) handleJob(ctx context.Context, actor *salmonide.Actor, sender salmonide.PeerID, request *jsonrpc2.Request) (result salmonide.Job, err error) {
	if err := salmonide.MustBeRequest(request); err != nil {
		return result, err
	}

	jobID, err := salmonide.ParseParams[salmonide.JobID](request)
	if err != nil {
		return result, err
	}

	rows, err := c.services.DB.QueryContext(ctx, `
		select
			j.exit_code,
			j.image,
			j.shell_script,
			j.status
		from jobs as j where j.id = ?
	`, jobID)
	if err != nil {
		return result, err
	}
	if !rows.Next() {
		return result, &jsonrpc2.Error{
			Code:    jsonrpc2.CodeInvalidRequest,
			Message: "job not found",
		}
	}

	var exitCode sql.NullInt32
	var image, shellScript string
	var status salmonide.JobStatus
	if err := rows.Scan(&exitCode, &image, &shellScript, &status); err != nil {
		return result, err
	}

	return salmonide.Job{
		ExitCode:    int(exitCode.Int32),
		ID:          jobID,
		Image:       image,
		ShellScript: shellScript,
		Status:      status,
	}, nil
}

func (c *coordinator) handleStartJob(ctx context.Context, actor *salmonide.Actor, sender salmonide.PeerID, request *jsonrpc2.Request) (result interface{}, err error) {
	if err := salmonide.MustBeRequest(request); err != nil {
		return nil, err
	}

	jobID, err := salmonide.ParseParams[salmonide.JobID](request)
	if err != nil {
		return nil, err
	}

	c.handlingWorkersMutex.Lock()
	defer c.handlingWorkersMutex.Unlock()

	tx, err := c.services.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	updateResult, err := tx.ExecContext(ctx, `
		update jobs as j
		set status = ?
		where j.id = ? and j.status = ?
	`,
		salmonide.JobStatusRunning,
		jobID,
		salmonide.JobStatusTaken,
	)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	rowsAffected, err := updateResult.RowsAffected()
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	if rowsAffected == 0 {
		_ = tx.Rollback()
		return nil, &jsonrpc2.Error{
			Code:    jsonrpc2.CodeInvalidRequest,
			Message: "job not found or not in right state",
		}
	}

	c.handlingWorkers[sender] = jobID

	return nil, tx.Commit()
}

func (c *coordinator) handleTakeJob(ctx context.Context, actor *salmonide.Actor, sender salmonide.PeerID, request *jsonrpc2.Request) (result bool, err error) {
	if err := salmonide.MustBeRequest(request); err != nil {
		return false, err
	}

	jobID, err := salmonide.ParseParams[salmonide.JobID](request)
	if err != nil {
		return false, err
	}

	tx, err := c.services.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}

	updateResult, err := tx.ExecContext(ctx, `
		update jobs as j
		set status = ?
		where j.id = ? and j.status = ?
	`,
		salmonide.JobStatusTaken,
		jobID,
		salmonide.JobStatusNotTaken,
	)
	if err != nil {
		_ = tx.Rollback()
		return false, err
	}
	rowsAffected, err := updateResult.RowsAffected()
	if err != nil {
		_ = tx.Rollback()
		return false, err
	}
	if rowsAffected == 0 {
		_ = tx.Rollback()
		return false, &jsonrpc2.Error{
			Code:    jsonrpc2.CodeInvalidRequest,
			Message: "job not found or not in right state",
		}
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}

	return true, nil
}

func NewCoordinator(ctx context.Context, services service.Services) (*salmonide.Actor, error) {
	stmtInsertOutputChunk, err := services.DB.PrepareContext(ctx, `
		insert into output_chunks (
			content,
			job_id,
			stream,
			timestamp
		) values (?, ?, ?, ?)
	`)
	if err != nil {
		return nil, err
	}
	go func() {
		<-ctx.Done()
		_ = stmtInsertOutputChunk.Close()
	}()

	c := coordinator{
		services: services,

		handlingWorkers:       map[salmonide.PeerID]salmonide.JobID{},
		stmtInsertOutputChunk: stmtInsertOutputChunk,
	}

	actor := salmonide.NewActor("coordinator", salmonide.ActorTypeCoordinator, c.handleRequest)
	actor.OnPeerConnected = c.onPeerConnected

	return actor, nil
}
