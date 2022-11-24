package service

import (
	"database/sql"
	"io"
)

type Services struct {
	DB        *sql.DB
	LogWriter io.Writer

	DiskFile    DiskFileService
	Exec        ExecService
	Ping        PingService
	Qemu        QemuService
	Sentinel    SentinelService
	Shutdown    ShutdownService
	TokenFile   TokenFileService
	WorkerProxy WorkerProxyService
}
