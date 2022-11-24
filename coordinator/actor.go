package coordinator

import (
	"context"
	"database/sql"
	"errors"

	"github.com/sourcegraph/jsonrpc2"
	"tbx.at/salmonide"
	"tbx.at/salmonide/service"
)

type coordinator struct {
	*salmonide.Actor
	services service.Services

	handlingWorkers       map[salmonide.PeerID]salmonide.JobID
	stmtInsertOutputChunk *sql.Stmt
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

	c.Actor = salmonide.NewActor(services.LogWriter, "coordinator", salmonide.ActorTypeCoordinator, c.onPeerConnected, nil, nil, c.handleRequest)

	return c.Actor, nil
}

func (c *coordinator) onPeerConnected(ctx context.Context, peer *salmonide.Peer) error {
	if peer.Type == salmonide.ActorTypeRunner {
		rows, err := c.services.DB.QueryContext(ctx, "select j.id, j.image, j.shell_script from jobs as j where j.status = ?", salmonide.JobStatusNotTaken)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id int
			var image, shellScript string
			if err := rows.Scan(&id, &image, &shellScript); err != nil {
				return err
			}
			job := salmonide.Job{
				ID:          salmonide.JobID(id),
				Image:       image,
				ShellScript: shellScript,
			}
			if err := peer.Conn.Notify(ctx, salmonide.MethodRunnerJobAvailable, job, nil); err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *coordinator) handleRequest(ctx context.Context, sender *salmonide.Peer, request *jsonrpc2.Request) (result interface{}, err error) {
	switch request.Method {
	case salmonide.MethodCoordinatorCaptureOutput:
		return c.handleCaptureOutput(ctx, sender, request)
	case salmonide.MethodCoordinatorCompleteJob:
		return c.handleCompleteJob(ctx, sender, request)
	case salmonide.MethodCoordinatorInsertJob:
		return c.handleInsertJob(ctx, sender, request)
	case salmonide.MethodCoordinatorJob:
		return c.handleJob(ctx, sender, request)
	case salmonide.MethodCoordinatorStartJob:
		return c.handleStartJob(ctx, sender, request)
	case salmonide.MethodCoordinatorTakeJob:
		return c.handleTakeJob(ctx, sender, request)
	}
	return nil, salmonide.NewMethodNotFoundError(request.Method)
}

func (c *coordinator) handleCaptureOutput(ctx context.Context, sender *salmonide.Peer, request *jsonrpc2.Request) (result interface{}, err error) {
	if err := salmonide.MustBeNotification(request); err != nil {
		return false, err
	}

	params, err := salmonide.ParseParams[salmonide.OutputChunk](request)
	if err != nil {
		return nil, err
	}

	jobID, ok := c.handlingWorkers[sender.ID]
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

func (c *coordinator) handleCompleteJob(ctx context.Context, sender *salmonide.Peer, request *jsonrpc2.Request) (result interface{}, err error) {
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

	delete(c.handlingWorkers, sender.ID)

	return nil, tx.Commit()
}

func (c *coordinator) handleInsertJob(ctx context.Context, sender *salmonide.Peer, request *jsonrpc2.Request) (result salmonide.JobID, err error) {
	if err := salmonide.MustBeRequest(request); err != nil {
		return result, err
	}

	params, err := salmonide.ParseParams[salmonide.Job](request)
	if err != nil {
		return result, err
	}

	rows, err := c.services.DB.QueryContext(ctx, `
		insert into jobs (
			image,
			shell_script,
			status
		) values (?, ?, ?)
		returning id
	`,
		params.Image,
		params.ShellScript,
		salmonide.JobStatusNotTaken,
	)
	defer rows.Close()
	if err != nil {
		return result, err
	}
	if !rows.Next() {
		return result, errors.New("no ID returned from insert")
	}
	if err := rows.Scan(&result); err != nil {
		return result, err
	}

	job := salmonide.Job{
		ID:          result,
		Image:       params.Image,
		ShellScript: params.ShellScript,
	}
	for _, runner := range c.Actor.FindPeersByType(salmonide.ActorTypeRunner) {
		if err := runner.Conn.Notify(ctx, salmonide.MethodRunnerJobAvailable, job, nil); err != nil {
			return result, err
		}
	}

	return result, nil
}

func (c *coordinator) handleJob(ctx context.Context, sender *salmonide.Peer, request *jsonrpc2.Request) (result salmonide.Job, err error) {
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

func (c *coordinator) handleStartJob(ctx context.Context, sender *salmonide.Peer, request *jsonrpc2.Request) (result interface{}, err error) {
	if err := salmonide.MustBeRequest(request); err != nil {
		return nil, err
	}

	jobID, err := salmonide.ParseParams[salmonide.JobID](request)
	if err != nil {
		return nil, err
	}

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

	c.handlingWorkers[sender.ID] = jobID

	return nil, tx.Commit()
}

func (c *coordinator) handleTakeJob(ctx context.Context, sender *salmonide.Peer, request *jsonrpc2.Request) (result bool, err error) {
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
