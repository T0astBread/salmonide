package service

type Services struct {
	DiskFile    DiskFileService
	Exec        ExecService
	Ping        PingService
	Qemu        QemuService
	Sentinel    SentinelService
	Shutdown    ShutdownService
	TokenFile   TokenFileService
	WorkerProxy WorkerProxyService
}
