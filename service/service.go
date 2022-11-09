package service

import "database/sql"

type Services struct {
	DB *sql.DB

	DiskFile    DiskFileService
	Exec        ExecService
	Ping        PingService
	Qemu        QemuService
	Sentinel    SentinelService
	Shutdown    ShutdownService
	TokenFile   TokenFileService
	WorkerProxy WorkerProxyService
}
