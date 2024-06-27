package controller

type RestoreTracker interface {
	BackupTracker
}

// NewRestoreTracker returns a new RestoreTracker.
func NewRestoreTracker() RestoreTracker {
	return NewBackupTracker()
}
