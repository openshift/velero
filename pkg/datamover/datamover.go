package datamover

import (
	"context"
	"fmt"
	"github.com/apex/log"
	snapmoverv1alpha1 "github.com/konveyor/volume-snapshot-mover/api/v1alpha1"
	"github.com/pkg/errors"
	velerov1api "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	"golang.org/x/sync/errgroup"
	"k8s.io/apimachinery/pkg/util/wait"
	"os"
	kbclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	"strconv"
	"time"
)

const (
	// Env vars
	VolumeSnapshotMoverEnv = "VOLUME_SNAPSHOT_MOVER"
)

// We expect VolumeSnapshotMoverEnv to be set once when container is started.
// When true, we will use the csi data-mover code path.
var dataMoverCase, _ = strconv.ParseBool(os.Getenv(VolumeSnapshotMoverEnv))

// DataMoverCase use getter to avoid changing bool in other packages
func DataMoverCase() bool {
	return dataMoverCase
}

func GetVolumeSnapMoverClient() (kbclient.Client, error) {
	client, err := kbclient.New(config.GetConfigOrDie(), kbclient.Options{})
	if err != nil {
		return nil, err
	}
	err = snapmoverv1alpha1.AddToScheme(client.Scheme())
	if err != nil {
		return nil, err
	}

	return client, err
}

func CheckIfVolumeSnapshotBackupsAreComplete(ctx context.Context, volumesnapshotbackups snapmoverv1alpha1.VolumeSnapshotBackupList) error {
	eg, _ := errgroup.WithContext(ctx)
	// update this to configurable timeout
	timeout := 10 * time.Minute
	interval := 5 * time.Second

	volumeSnapMoverClient, err := GetVolumeSnapMoverClient()
	if err != nil {
		return err
	}

	for _, vsb := range volumesnapshotbackups.Items {
		volumesnapshotbackup := vsb
		eg.Go(func() error {
			err := wait.PollImmediate(interval, timeout, func() (bool, error) {
				tmpVSB := snapmoverv1alpha1.VolumeSnapshotBackup{}
				err := volumeSnapMoverClient.Get(ctx, kbclient.ObjectKey{Namespace: volumesnapshotbackup.Namespace, Name: volumesnapshotbackup.Name}, &tmpVSB)
				if err != nil {
					return false, errors.Wrapf(err, fmt.Sprintf("failed to get volumesnapshotbackup %s/%s", volumesnapshotbackup.Namespace, volumesnapshotbackup.Name))
				}
				if len(tmpVSB.Status.Phase) == 0 || tmpVSB.Status.Phase != snapmoverv1alpha1.SnapMoverBackupPhaseCompleted {
					log.Infof("Waiting for volumesnapshotbackup to complete %s/%s. Retrying in %ds", volumesnapshotbackup.Namespace, volumesnapshotbackup.Name, interval/time.Second)
					return false, nil
				}

				return true, nil
			})
			if err == wait.ErrWaitTimeout {
				log.Errorf("Timed out awaiting reconciliation of volumesnapshotbackup %s/%s", volumesnapshotbackup.Namespace, volumesnapshotbackup.Name)
			}
			return err
		})
	}
	return eg.Wait()
}

func WaitForDataMoverBackupToComplete(backupName string) error {
	//wait for all the VSBs to be complete
	volumeSnapMoverClient, err := GetVolumeSnapMoverClient()
	if err != nil {
		log.Errorf(err.Error())
		return err
	}

	VSBList := snapmoverv1alpha1.VolumeSnapshotBackupList{}
	VSBListOptions := kbclient.MatchingLabels(map[string]string{
		velerov1api.BackupNameLabel: backupName,
	})

	err = volumeSnapMoverClient.List(context.TODO(), &VSBList, VSBListOptions)
	if err != nil {
		log.Errorf(err.Error())
		return err
	}

	log.Info(fmt.Sprintf("List of all VSBs: %v", VSBList))

	//Wait for all VSBs to complete
	if len(VSBList.Items) > 0 {
		err = CheckIfVolumeSnapshotBackupsAreComplete(context.Background(), VSBList)
		if err != nil {
			log.Errorf("failed to wait for VolumeSnapshotBackups to be completed: %s", err.Error())
			return err
		}
	}
	return nil
}
