package service

import (
	"context"
	"fmt"
	"os/exec"
)

type QemuService interface {
	StartVM(ctx context.Context, diskFilePath, tokenFilePath string) error
}

type QemuServiceImpl struct{}

// StartVM implements QemuService
func (*QemuServiceImpl) StartVM(ctx context.Context, diskFilePath string, tokenFilePath string) error {
	qemu := exec.CommandContext(ctx, "qemu-system-x86_64", "-enable-kvm", "-m", "8G", "-nographic",
		"-drive", fmt.Sprintf("format=qcow2,file=%s", diskFilePath),
		"-nic", fmt.Sprintf("user,guestfwd=tcp:10.0.2.100:1234-cmd:cat %s", tokenFilePath))
	return qemu.Run()
}

var _ QemuService = (*QemuServiceImpl)(nil)
