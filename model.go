package salmonide

import (
	"strconv"
	"time"
)

const (
	MethodCoordinatorCaptureOutput = "coordinator/capture-output"
	MethodCoordinatorCompleteJob   = "coordinator/complete-job"
	MethodCoordinatorJob           = "coordinator/job"
	MethodCoordinatorStartJob      = "coordinator/start-job"
	MethodCoordinatorTakeJob       = "coordinator/take-job"
	MethodRunnerJobAvailable       = "runner/job-available"
)

type CoordinatorCompleteJobParams struct {
	JobID    JobID
	ExitCode int
}

type OutputStream int

const (
	OutputStreamStdout OutputStream = iota
	OutputStreamStderr
)

type OutputChunk struct {
	Content   []byte
	Stream    OutputStream
	Timestamp time.Time
}

type JobStatus int

const (
	JobStatusUnknown JobStatus = iota
	JobStatusNotTaken
	JobStatusTaken
	JobStatusRunning
	JobStatusDone
	JobStatusCancelled
	JobStatusCrashed
)

type JobID uint

func (id JobID) String() string {
	return strconv.FormatUint(uint64(id), 10)
}

type Job struct {
	ExitCode    int
	ID          JobID
	Image       string
	ShellScript string
	Status      JobStatus
}
