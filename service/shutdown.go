package service

import (
	"os"
	"os/exec"
)

type ShutdownService interface {
	Shutdown(immediate bool) error
}

type ShutdownServiceImpl struct{}

// Shutdown implements ShutdownService
func (*ShutdownServiceImpl) Shutdown(immediate bool) error {
	t := "+1"
	if immediate {
		t = "now"
	}

	cmd := exec.Command("shutdown", t)
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

var _ ShutdownService = (*ShutdownServiceImpl)(nil)
