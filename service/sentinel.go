package service

import (
	"errors"
	"os"
	"path/filepath"
)

type SentinelService interface {
	SentinelExists() (bool, error)
	WriteSentinel() error
}

const sentinelFilePath = "/var/lib/salmonide-worker/sentinel"

type SentinelServiceImpl struct{}

// SentinelExists implements SentinelService
func (*SentinelServiceImpl) SentinelExists() (bool, error) {
	if err := os.MkdirAll(filepath.Dir(sentinelFilePath), 0700); err != nil {
		return false, err
	}
	_, err := os.Stat(sentinelFilePath)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

// WriteSentinel implements SentinelService
func (*SentinelServiceImpl) WriteSentinel() error {
	sentinelFile, err := os.Create(sentinelFilePath)
	if err != nil {
		return err
	}
	return sentinelFile.Close()
}

var _ SentinelService = (*SentinelServiceImpl)(nil)
