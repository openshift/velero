/*
Copyright the Velero contributors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"bytes"
	"context"
	"os"
	"time"

	"github.com/vmware-tanzu/velero/pkg/datamover"

	snapshotv1api "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	snapshotterClientSet "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	snapshotv1listers "github.com/kubernetes-csi/external-snapshotter/client/v4/listers/volumesnapshot/v1"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocks "k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	kbclient "sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/vmware-tanzu/velero/pkg/features"
	"github.com/vmware-tanzu/velero/pkg/label"

	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	pkgbackup "github.com/vmware-tanzu/velero/pkg/backup"
	"github.com/vmware-tanzu/velero/pkg/metrics"
	"github.com/vmware-tanzu/velero/pkg/persistence"
	"github.com/vmware-tanzu/velero/pkg/plugin/clientmgmt"
	"github.com/vmware-tanzu/velero/pkg/plugin/framework"
	"github.com/vmware-tanzu/velero/pkg/util/encode"
)

// backupFinalizerReconciler reconciles a Backup object
type backupFinalizerReconciler struct {
	ctx                  context.Context
	client               kbclient.Client
	clock                clocks.WithTickerAndDelayedExecution
	backupper            pkgbackup.Backupper
	newPluginManager     func(logrus.FieldLogger) clientmgmt.Manager
	backupTracker        BackupTracker
	metrics              *metrics.ServerMetrics
	backupStoreGetter    persistence.ObjectBackupStoreGetter
	log                  logrus.FieldLogger
	volumeSnapshotLister snapshotv1listers.VolumeSnapshotLister
	volumeSnapshotClient snapshotterClientSet.Interface
	resourceTimeout      time.Duration
}

// NewBackupFinalizerReconciler initializes and returns backupFinalizerReconciler struct.
func NewBackupFinalizerReconciler(
	ctx context.Context,
	client kbclient.Client,
	clock clocks.WithTickerAndDelayedExecution,
	backupper pkgbackup.Backupper,
	newPluginManager func(logrus.FieldLogger) clientmgmt.Manager,
	backupTracker BackupTracker,
	backupStoreGetter persistence.ObjectBackupStoreGetter,
	log logrus.FieldLogger,
	metrics *metrics.ServerMetrics,
	volumeSnapshotLister snapshotv1listers.VolumeSnapshotLister,
	volumeSnapshotClient snapshotterClientSet.Interface,
	resourceTimeout time.Duration,
) *backupFinalizerReconciler {
	return &backupFinalizerReconciler{
		ctx:                  ctx,
		client:               client,
		clock:                clock,
		backupper:            backupper,
		newPluginManager:     newPluginManager,
		backupTracker:        backupTracker,
		backupStoreGetter:    backupStoreGetter,
		log:                  log,
		metrics:              metrics,
		volumeSnapshotLister: volumeSnapshotLister,
		volumeSnapshotClient: volumeSnapshotClient,
		resourceTimeout:      resourceTimeout,
	}
}

// +kubebuilder:rbac:groups=velero.io,resources=backups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=velero.io,resources=backups/status,verbs=get;update;patch

func (r *backupFinalizerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := r.log.WithFields(logrus.Fields{
		"controller": "backup-finalizer",
		"backup":     req.NamespacedName,
	})

	// Fetch the Backup instance.
	log.Debug("Getting Backup")
	backup := &velerov1api.Backup{}
	if err := r.client.Get(ctx, req.NamespacedName, backup); err != nil {
		if apierrors.IsNotFound(err) {
			log.Debug("Unable to find Backup")
			return ctrl.Result{}, nil
		}

		log.WithError(err).Error("Error getting Backup")
		return ctrl.Result{}, errors.WithStack(err)
	}

	switch backup.Status.Phase {
	case velerov1api.BackupPhaseFinalizing, velerov1api.BackupPhaseFinalizingPartiallyFailed:
		// only process backups finalizing after  plugin operations are complete
	default:
		log.Debug("Backup is not awaiting finalizing, skipping")
		return ctrl.Result{}, nil
	}

	original := backup.DeepCopy()
	defer func() {
		switch backup.Status.Phase {
		case velerov1api.BackupPhaseCompleted, velerov1api.BackupPhasePartiallyFailed, velerov1api.BackupPhaseFailed, velerov1api.BackupPhaseFailedValidation:
			r.backupTracker.Delete(backup.Namespace, backup.Name)
		}
		// Always attempt to Patch the backup object and status after each reconciliation.
		if err := r.client.Patch(ctx, backup, kbclient.MergeFrom(original)); err != nil {
			log.WithError(err).Error("Error updating backup")
			return
		}
	}()

	location := &velerov1api.BackupStorageLocation{}
	if err := r.client.Get(ctx, kbclient.ObjectKey{
		Namespace: backup.Namespace,
		Name:      backup.Spec.StorageLocation,
	}, location); err != nil {
		return ctrl.Result{}, errors.WithStack(err)
	}
	pluginManager := r.newPluginManager(log)
	defer pluginManager.CleanupClients()

	backupStore, err := r.backupStoreGetter.Get(location, pluginManager, log)
	if err != nil {
		log.WithError(err).Error("Error getting a backup store")
		return ctrl.Result{}, errors.WithStack(err)
	}

	// Download item operations list and backup contents
	operations, err := backupStore.GetBackupItemOperations(backup.Name)
	if err != nil {
		log.WithError(err).Error("Error getting backup item operations")
		return ctrl.Result{}, errors.WithStack(err)
	}

	backupRequest := &pkgbackup.Request{
		Backup:          backup,
		StorageLocation: location,
	}
	var outBackupFile *os.File
	if len(operations) > 0 {
		// Call itemBackupper.BackupItem for the list of items updated by async operations
		log.Info("Setting up finalized backup temp file")
		inBackupFile, err := downloadToTempFile(backup.Name, backupStore, log)
		if err != nil {
			return ctrl.Result{}, errors.Wrap(err, "error downloading backup")
		}
		defer closeAndRemoveFile(inBackupFile, log)
		outBackupFile, err = os.CreateTemp("", "")
		if err != nil {
			log.WithError(err).Error("error creating temp file for backup")
			return ctrl.Result{}, errors.WithStack(err)
		}
		defer closeAndRemoveFile(outBackupFile, log)

		log.Info("Getting backup item actions")
		actions, err := pluginManager.GetBackupItemActionsV2()
		if err != nil {
			log.WithError(err).Error("error getting Backup Item Actions")
			return ctrl.Result{}, errors.WithStack(err)
		}
		backupItemActionsResolver := framework.NewBackupItemActionResolverV2(actions)
		err = r.backupper.FinalizeBackup(log, backupRequest, inBackupFile, outBackupFile, backupItemActionsResolver, operations)
		if err != nil {
			log.WithError(err).Error("error finalizing Backup")
			return ctrl.Result{}, errors.WithStack(err)
		}
	}
	backupScheduleName := backupRequest.GetLabels()[velerov1api.ScheduleNameLabel]
	switch backup.Status.Phase {
	case velerov1api.BackupPhaseFinalizing:
		backup.Status.Phase = velerov1api.BackupPhaseCompleted
		r.metrics.RegisterBackupSuccess(backupScheduleName)
		r.metrics.RegisterBackupLastStatus(backupScheduleName, metrics.BackupLastStatusSucc)
	case velerov1api.BackupPhaseFinalizingPartiallyFailed:
		backup.Status.Phase = velerov1api.BackupPhasePartiallyFailed
		r.metrics.RegisterBackupPartialFailure(backupScheduleName)
		r.metrics.RegisterBackupLastStatus(backupScheduleName, metrics.BackupLastStatusFailure)
	}
	backup.Status.CompletionTimestamp = &metav1.Time{Time: r.clock.Now()}
	recordBackupMetrics(log, backup, outBackupFile, r.metrics, true)

	// update backup metadata in object store
	backupJSON := new(bytes.Buffer)
	if err := encode.EncodeTo(backup, "json", backupJSON); err != nil {
		return ctrl.Result{}, errors.Wrap(err, "error encoding backup json")
	}
	err = backupStore.PutBackupMetadata(backup.Name, backupJSON)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "error uploading backup json")
	}
	if len(operations) > 0 {
		err = backupStore.PutBackupContents(backup.Name, outBackupFile)
		if err != nil {
			return ctrl.Result{}, errors.Wrap(err, "error uploading backup final contents")
		}
	}

	//Defer Deleting CSI Snapshots
	// Empty slices here so that they can be passed in to the persistBackup call later, regardless of whether or not CSI's enabled.
	// This way, we only make the Lister call if the feature flag's on.
	var volumeSnapshots []snapshotv1api.VolumeSnapshot
	var volumeSnapshotContents []snapshotv1api.VolumeSnapshotContent
	if features.IsEnabled(velerov1api.CSIFeatureFlag) {
		selector := label.NewSelectorForBackup(backup.Name)
		vscList := &snapshotv1api.VolumeSnapshotContentList{}

		volumeSnapshots, err = waitVolumeSnapshotReadyToUse(context.Background(), backup.Spec.CSISnapshotTimeout.Duration, backup.Name, r.log, r.volumeSnapshotLister, r.volumeSnapshotClient)
		if err != nil {
			log.Errorf("fail to list VolumeSnapshots to be deleted: %s", err.Error())
		}

		err = r.client.List(context.Background(), vscList, &kbclient.ListOptions{LabelSelector: selector})
		if err != nil {
			log.Error(err)
		}
		if len(vscList.Items) >= 0 {
			volumeSnapshotContents = vscList.Items
		}

		// Delete the VolumeSnapshots created in the backup, when CSI feature is enabled.
		if len(volumeSnapshots) > 0 && len(volumeSnapshotContents) > 0 {
			deleteVolumeSnapshots(volumeSnapshots, volumeSnapshotContents, log, 30, r.client, r.volumeSnapshotClient, r.resourceTimeout)
		}
		// check if the VSBs associated with the backup are in completed state, if yes then clean them up
		// Also cleanup the datamover VSCs
		err = datamover.CleanupDatamoverArtifacts(backup.Name, r.log, *vscList, r.volumeSnapshotClient)
		if err != nil {
			log.Error(err)
		}
	}

	return ctrl.Result{}, nil
}

func (r *backupFinalizerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&velerov1api.Backup{}).
		Complete(r)
}
