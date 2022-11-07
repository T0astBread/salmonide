package service

import (
	"io"
	"os"
	"path/filepath"

	"tbx.at/salmonide"
)

type DiskFileService interface {
	Create(imageDirectory, diskDirectory string, job salmonide.Job) (filePath string, err error)
}

type DiskFileServiceImpl struct{}

// Create implements DiskFileService
func (*DiskFileServiceImpl) Create(imageDirectory, diskDirectory string, job salmonide.Job) (filePath string, err error) {
	imageFile, err := os.Open(filepath.Join(imageDirectory, job.Image))
	if err != nil {
		return "", err
	}
	defer imageFile.Close()
	diskFile, err := os.Create(filepath.Join(diskDirectory, job.ID.String()))
	if err != nil {
		return "", err
	}
	defer diskFile.Close()
	if err := os.Chmod(diskFile.Name(), 0600); err != nil {
		return "", err
	}
	if _, err := io.Copy(diskFile, imageFile); err != nil {
		return "", err
	}
	if err := imageFile.Close(); err != nil {
		return "", err
	}
	if err := diskFile.Close(); err != nil {
		return "", err
	}
	return diskFile.Name(), nil
}

var _ DiskFileService = (*DiskFileServiceImpl)(nil)
