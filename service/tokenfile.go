package service

import (
	"bufio"
	"errors"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"tbx.at/salmonide"
)

type TokenFileService interface {
	Read() (salmonide.JobID, error)
	Write(tokenDirectory string, job salmonide.Job) (filePath string, cleanup func() error, err error)
}

type TokenFileServiceImpl struct{}

// Read implements TokenFileService
func (*TokenFileServiceImpl) Read() (salmonide.JobID, error) {
	var conn net.Conn
	for i := 0; ; i++ {
		c, err := net.Dial("tcp4", "10.0.2.100:1234")
		if err != nil {
			log.Printf("error reading token file: %s\n", err.Error())
			if i >= 10 {
				return 0, err
			}
			log.Println("retrying token file read in 1s...")
			time.Sleep(1 * time.Second)
		}
		conn = c
		break
	}
	defer conn.Close()

	sc := bufio.NewScanner(conn)

	if !sc.Scan() {
		return 0, errors.New("no job ID to read")
	}
	jobIDStr := sc.Text()
	jobIDUint, err := strconv.ParseUint(jobIDStr, 10, 32)
	if err != nil {
		return 0, err
	}
	return salmonide.JobID(jobIDUint), nil
}

// Write implements TokenFileService
func (*TokenFileServiceImpl) Write(tokenDirectory string, job salmonide.Job) (filePath string, cleanup func() error, err error) {
	tokenFilePath := filepath.Join(tokenDirectory, job.ID.String())
	tokenFile, err := os.Create(tokenFilePath)
	if err != nil {
		return "", nil, err
	}
	defer tokenFile.Close()
	cleanup = func() error {
		return os.Remove(tokenFilePath)
	}
	if err := os.Chmod(tokenFilePath, 0600); err != nil {
		return "", cleanup, err
	}
	if _, err := tokenFile.WriteString(job.ID.String()); err != nil {
		return "", cleanup, err
	}
	if _, err := tokenFile.WriteString("\n"); err != nil {
		return "", cleanup, err
	}
	if err := tokenFile.Close(); err != nil {
		return "", cleanup, err
	}
	return tokenFilePath, cleanup, nil
}

var _ TokenFileService = (*TokenFileServiceImpl)(nil)
