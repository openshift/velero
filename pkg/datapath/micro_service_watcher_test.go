/*
Copyright The Velero Contributors.

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

package datapath

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1api "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	kubeclientfake "k8s.io/client-go/kubernetes/fake"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/vmware-tanzu/velero/pkg/builder"
	velerotest "github.com/vmware-tanzu/velero/pkg/test"
	"github.com/vmware-tanzu/velero/pkg/uploader"
	"github.com/vmware-tanzu/velero/pkg/util/logging"
)

func TestReEnsureThisPod(t *testing.T) {
	tests := []struct {
		name          string
		namespace     string
		thisPod       string
		kubeClientObj []runtime.Object
		expectChan    bool
		expectErr     string
	}{
		{
			name:      "get pod error",
			thisPod:   "fak-pod-1",
			expectErr: "error getting this pod fak-pod-1: pods \"fak-pod-1\" not found",
		},
		{
			name:      "get pod not in terminated state",
			namespace: "velero",
			thisPod:   "fake-pod-1",
			kubeClientObj: []runtime.Object{
				builder.ForPod("velero", "fake-pod-1").Phase(corev1api.PodRunning).Result(),
			},
		},
		{
			name:      "get pod succeed state",
			namespace: "velero",
			thisPod:   "fake-pod-1",
			kubeClientObj: []runtime.Object{
				builder.ForPod("velero", "fake-pod-1").Phase(corev1api.PodSucceeded).Result(),
			},
			expectChan: true,
		},
		{
			name:      "get pod failed state",
			namespace: "velero",
			thisPod:   "fake-pod-1",
			kubeClientObj: []runtime.Object{
				builder.ForPod("velero", "fake-pod-1").Phase(corev1api.PodFailed).Result(),
			},
			expectChan: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			corev1api.AddToScheme(scheme)
			fakeClientBuilder := fake.NewClientBuilder()
			fakeClientBuilder = fakeClientBuilder.WithScheme(scheme)

			fakeClient := fakeClientBuilder.WithRuntimeObjects(test.kubeClientObj...).Build()

			ms := &microServiceBRWatcher{
				namespace: test.namespace,
				thisPod:   test.thisPod,
				client:    fakeClient,
				podCh:     make(chan *corev1api.Pod, 2),
				log:       velerotest.NewLogger(),
			}

			err := ms.reEnsureThisPod(t.Context())
			if test.expectErr != "" {
				assert.EqualError(t, err, test.expectErr)
			} else {
				if test.expectChan {
					assert.Len(t, ms.podCh, 1)
					pod := <-ms.podCh
					assert.Equal(t, pod.Name, test.thisPod)
				}
			}
		})
	}
}

type startWatchFake struct {
	terminationMessage string
	redirectErr        error
	complete           bool
	failed             bool
	canceled           bool
	progress           int
	result             Result
	failErr            error
}

func (sw *startWatchFake) getPodContainerTerminateMessage(pod *corev1api.Pod, container string) string {
	return sw.terminationMessage
}

func (sw *startWatchFake) redirectDataMoverLogs(ctx context.Context, kubeClient kubernetes.Interface, namespace string, thisPod string, thisContainer string, logger logrus.FieldLogger) error {
	return sw.redirectErr
}

func (sw *startWatchFake) getResultFromMessage(_ string, _ string, _ logrus.FieldLogger) Result {
	return Result{}
}

func (sw *startWatchFake) OnCompleted(ctx context.Context, namespace string, task string, result Result) {
	sw.complete = true
	sw.result = result
}

func (sw *startWatchFake) OnFailed(ctx context.Context, namespace string, task string, err error) {
	sw.failed = true
	sw.failErr = err
}

func (sw *startWatchFake) OnCancelled(ctx context.Context, namespace string, task string) {
	sw.canceled = true
}

func (sw *startWatchFake) OnProgress(ctx context.Context, namespace string, task string, progress *uploader.Progress) {
	sw.progress++
}

type insertEvent struct {
	event *corev1api.Event
	after time.Duration
	delay time.Duration
}

func TestStartWatch(t *testing.T) {
	tests := []struct {
		name                 string
		namespace            string
		thisPod              string
		thisContainer        string
		terminationMessage   string
		redirectLogErr       error
		insertPod            *corev1api.Pod
		insertEventsBefore   []insertEvent
		insertEventsAfter    []insertEvent
		deletePod            *corev1api.Pod
		deleteTombstone      bool
		ctxCancel            bool
		expectStartEvent     bool
		expectTerminateEvent bool
		expectComplete       bool
		expectCancel         bool
		expectFail           bool
		expectFailMsg        string
		expectResult         *Result
		expectProgress       int
		expectLog            string
	}{
		{
			name:          "exit from ctx",
			thisPod:       "fak-pod-1",
			thisContainer: "fake-container-1",
			ctxCancel:     true,
		},
		{
			name:          "completed with rantional sequence",
			thisPod:       "fak-pod-1",
			thisContainer: "fake-container-1",
			insertPod:     builder.ForPod("velero", "fake-pod-1").Phase(corev1api.PodSucceeded).Result(),
			insertEventsBefore: []insertEvent{
				{
					event: &corev1api.Event{Reason: EventReasonStarted},
				},
				{
					event: &corev1api.Event{Reason: EventReasonCompleted},
				},
				{
					event: &corev1api.Event{Reason: EventReasonStopped},
					delay: time.Second,
				},
			},
			expectStartEvent:     true,
			expectTerminateEvent: true,
			expectComplete:       true,
			expectProgress:       1,
		},
		{
			name:          "completed",
			thisPod:       "fak-pod-1",
			thisContainer: "fake-container-1",
			insertPod:     builder.ForPod("velero", "fake-pod-1").Phase(corev1api.PodSucceeded).Result(),
			insertEventsBefore: []insertEvent{
				{
					event: &corev1api.Event{Reason: EventReasonStarted},
				},
				{
					event: &corev1api.Event{Reason: EventReasonCompleted},
				},
				{
					event: &corev1api.Event{Reason: EventReasonStopped},
				},
			},
			expectStartEvent:     true,
			expectTerminateEvent: true,
			expectComplete:       true,
			expectProgress:       1,
		},
		{
			name:          "completed with redirect error",
			thisPod:       "fak-pod-1",
			thisContainer: "fake-container-1",
			insertPod:     builder.ForPod("velero", "fake-pod-1").Phase(corev1api.PodSucceeded).Result(),
			insertEventsBefore: []insertEvent{
				{
					event: &corev1api.Event{Reason: EventReasonStarted},
				},
				{
					event: &corev1api.Event{Reason: EventReasonCompleted},
				},
				{
					event: &corev1api.Event{Reason: EventReasonStopped},
				},
			},
			redirectLogErr:       errors.New("fake-error"),
			expectStartEvent:     true,
			expectTerminateEvent: true,
			expectComplete:       true,
			expectProgress:       1,
		},
		{
			name:          "complete but terminated event not received in time",
			thisPod:       "fak-pod-1",
			thisContainer: "fake-container-1",
			insertPod:     builder.ForPod("velero", "fake-pod-1").Phase(corev1api.PodSucceeded).Result(),
			insertEventsBefore: []insertEvent{
				{
					event: &corev1api.Event{Reason: EventReasonStarted},
				},
			},
			insertEventsAfter: []insertEvent{
				{
					event: &corev1api.Event{Reason: EventReasonStarted},
					after: time.Second * 6,
				},
			},
			expectStartEvent: true,
			expectComplete:   true,
			expectProgress:   1,
		},
		{
			name:          "complete but terminated event not received immediately",
			thisPod:       "fak-pod-1",
			thisContainer: "fake-container-1",
			insertPod:     builder.ForPod("velero", "fake-pod-1").Phase(corev1api.PodSucceeded).Result(),
			insertEventsBefore: []insertEvent{
				{
					event: &corev1api.Event{Reason: EventReasonStarted},
				},
			},
			insertEventsAfter: []insertEvent{
				{
					event: &corev1api.Event{Reason: EventReasonCompleted},
				},
				{
					event: &corev1api.Event{Reason: EventReasonStopped},
					delay: time.Second,
				},
			},
			expectStartEvent:     true,
			expectTerminateEvent: true,
			expectComplete:       true,
			expectProgress:       1,
		},
		{
			name:          "completed with progress",
			thisPod:       "fak-pod-1",
			thisContainer: "fake-container-1",
			insertPod:     builder.ForPod("velero", "fake-pod-1").Phase(corev1api.PodSucceeded).Result(),
			insertEventsBefore: []insertEvent{
				{
					event: &corev1api.Event{Reason: EventReasonStarted},
				},
				{
					event: &corev1api.Event{Reason: EventReasonProgress, Message: "fake-progress-1"},
				},
				{
					event: &corev1api.Event{Reason: EventReasonProgress, Message: "fake-progress-2"},
				},
				{
					event: &corev1api.Event{Reason: EventReasonCompleted},
				},
				{
					event: &corev1api.Event{Reason: EventReasonStopped},
					delay: time.Second,
				},
			},
			expectStartEvent:     true,
			expectTerminateEvent: true,
			expectComplete:       true,
			expectProgress:       3,
		},
		{
			name:          "failed",
			thisPod:       "fak-pod-1",
			thisContainer: "fake-container-1",
			insertPod:     builder.ForPod("velero", "fake-pod-1").Phase(corev1api.PodFailed).Result(),
			insertEventsBefore: []insertEvent{
				{
					event: &corev1api.Event{Reason: EventReasonStarted},
				},
				{
					event: &corev1api.Event{Reason: EventReasonCancelled},
				},
				{
					event: &corev1api.Event{Reason: EventReasonStopped},
				},
			},
			terminationMessage:   "fake-termination-message-1",
			expectStartEvent:     true,
			expectTerminateEvent: true,
			expectFail:           true,
		},
		{
			name:               "pod crash",
			thisPod:            "fak-pod-1",
			thisContainer:      "fake-container-1",
			insertPod:          builder.ForPod("velero", "fake-pod-1").Phase(corev1api.PodFailed).Result(),
			terminationMessage: "fake-termination-message-2",
			expectFail:         true,
		},
		{
			name:          "canceled",
			thisPod:       "fak-pod-1",
			thisContainer: "fake-container-1",
			insertPod:     builder.ForPod("velero", "fake-pod-1").Phase(corev1api.PodFailed).Result(),
			insertEventsBefore: []insertEvent{
				{
					event: &corev1api.Event{Reason: EventReasonStarted},
				},
				{
					event: &corev1api.Event{Reason: EventReasonCancelled},
				},
				{
					event: &corev1api.Event{Reason: EventReasonStopped},
				},
			},
			terminationMessage:   fmt.Sprintf("Failed to init data path service for DataUpload %s: %v", "fake-du-name", errors.New(ErrCancelled)),
			expectStartEvent:     true,
			expectTerminateEvent: true,
			expectCancel:         true,
		},
		{
			name:          "deleted while running",
			namespace:     "velero",
			thisPod:       "fake-pod-1",
			thisContainer: "fake-container-1",
			deletePod:     builder.ForPod("velero", "fake-pod-1").Phase(corev1api.PodRunning).Result(),
			insertEventsBefore: []insertEvent{
				{
					event: &corev1api.Event{Reason: EventReasonStarted},
				},
			},
			expectStartEvent: true,
			expectFail:       true,
			expectFailMsg:    "data path pod fake-pod-1 was deleted in phase Running before the data path completed",
			expectLog:        "Data path pod fake-pod-1 was deleted in phase Running, failing backup task fake-du-name through the pod deletion path",
		},
		{
			name:            "deleted while running, delete recovered from tombstone",
			namespace:       "velero",
			thisPod:         "fake-pod-1",
			thisContainer:   "fake-container-1",
			deletePod:       builder.ForPod("velero", "fake-pod-1").Phase(corev1api.PodRunning).Result(),
			deleteTombstone: true,
			insertEventsBefore: []insertEvent{
				{
					event: &corev1api.Event{Reason: EventReasonStarted},
				},
				{
					event: &corev1api.Event{Reason: EventReasonStopped},
				},
			},
			expectStartEvent:     true,
			expectTerminateEvent: true,
			expectFail:           true,
			expectFailMsg:        "data path pod fake-pod-1 was deleted in phase Running before the data path completed",
			expectLog:            "Data path pod fake-pod-1 was deleted in phase Running, failing backup task fake-du-name through the pod deletion path",
		},
		{
			name:          "deleted while running after failed event",
			namespace:     "velero",
			thisPod:       "fake-pod-1",
			thisContainer: "fake-container-1",
			deletePod:     builder.ForPod("velero", "fake-pod-1").Phase(corev1api.PodRunning).Result(),
			insertEventsBefore: []insertEvent{
				{
					event: &corev1api.Event{Reason: EventReasonStarted},
				},
				{
					event: &corev1api.Event{Reason: EventReasonFailed, Message: "fake-failed-message"},
				},
				{
					event: &corev1api.Event{Reason: EventReasonStopped},
				},
			},
			expectStartEvent:     true,
			expectTerminateEvent: true,
			expectFail:           true,
			expectFailMsg:        "data path pod fake-pod-1 was deleted in phase Running, data path failed: fake-failed-message",
			expectLog:            "Data path pod fake-pod-1 was deleted in phase Running, failing backup task fake-du-name through the pod deletion path",
		},
		{
			name:          "deleted after completed event",
			namespace:     "velero",
			thisPod:       "fake-pod-1",
			thisContainer: "fake-container-1",
			deletePod:     builder.ForPod("velero", "fake-pod-1").Phase(corev1api.PodRunning).Result(),
			insertEventsBefore: []insertEvent{
				{
					event: &corev1api.Event{Reason: EventReasonStarted},
				},
				{
					event: &corev1api.Event{Reason: EventReasonCompleted, Message: `{"snapshotID":"fake-snapshot-id","emptySnapshot":false,"source":{"byPath":"/fake-path","volumeMode":"Block"},"totalBytes":1073741824,"incrementalBytes":1024}`},
				},
				{
					event: &corev1api.Event{Reason: EventReasonStopped},
				},
			},
			expectStartEvent:     true,
			expectTerminateEvent: true,
			expectComplete:       true,
			expectResult: &Result{Backup: BackupResult{
				SnapshotID:       "fake-snapshot-id",
				Source:           AccessPoint{ByPath: "/fake-path", VolMode: uploader.PersistentVolumeBlock},
				TotalBytes:       1073741824,
				IncrementalBytes: 1024,
			}},
			expectProgress: 1,
			expectLog:      "Data path pod fake-pod-1 was deleted in phase Running after the data path completed, completing backup task fake-du-name through the pod deletion path",
		},
		{
			name:          "deleted after completed event, stop event not received",
			namespace:     "velero",
			thisPod:       "fake-pod-1",
			thisContainer: "fake-container-1",
			deletePod:     builder.ForPod("velero", "fake-pod-1").Phase(corev1api.PodRunning).Result(),
			insertEventsBefore: []insertEvent{
				{
					event: &corev1api.Event{Reason: EventReasonStarted},
				},
				{
					event: &corev1api.Event{Reason: EventReasonCompleted, Message: `{"snapshotID":"fake-snapshot-id","emptySnapshot":false,"source":{"byPath":"/fake-path","volumeMode":"Block"},"totalBytes":1073741824,"incrementalBytes":1024}`},
				},
			},
			expectStartEvent: true,
			expectComplete:   true,
			expectResult: &Result{Backup: BackupResult{
				SnapshotID:       "fake-snapshot-id",
				Source:           AccessPoint{ByPath: "/fake-path", VolMode: uploader.PersistentVolumeBlock},
				TotalBytes:       1073741824,
				IncrementalBytes: 1024,
			}},
			expectProgress: 1,
			expectLog:      "Data path pod fake-pod-1 was deleted in phase Running after the data path completed, completing backup task fake-du-name through the pod deletion path",
		},
		{
			name:          "deleted after completed event with invalid message",
			namespace:     "velero",
			thisPod:       "fake-pod-1",
			thisContainer: "fake-container-1",
			deletePod:     builder.ForPod("velero", "fake-pod-1").Phase(corev1api.PodRunning).Result(),
			insertEventsBefore: []insertEvent{
				{
					event: &corev1api.Event{Reason: EventReasonStarted},
				},
				{
					event: &corev1api.Event{Reason: EventReasonCompleted, Message: "fake-invalid-message"},
				},
				{
					event: &corev1api.Event{Reason: EventReasonStopped},
				},
			},
			expectStartEvent:     true,
			expectTerminateEvent: true,
			expectFail:           true,
			expectFailMsg:        "data path pod fake-pod-1 was deleted in phase Running before the data path completed",
			expectLog:            "Data path pod fake-pod-1 was deleted in phase Running, failing backup task fake-du-name through the pod deletion path",
		},
		{
			name:          "failed phase after completed event",
			namespace:     "velero",
			thisPod:       "fake-pod-1",
			thisContainer: "fake-container-1",
			insertPod:     builder.ForPod("velero", "fake-pod-1").Phase(corev1api.PodFailed).Result(),
			insertEventsBefore: []insertEvent{
				{
					event: &corev1api.Event{Reason: EventReasonStarted},
				},
				{
					event: &corev1api.Event{Reason: EventReasonCompleted, Message: `{"snapshotID":"fake-snapshot-id","emptySnapshot":false,"source":{"byPath":"/fake-path","volumeMode":"Block"},"totalBytes":1073741824,"incrementalBytes":1024}`},
				},
				{
					event: &corev1api.Event{Reason: EventReasonStopped},
				},
			},
			expectStartEvent:     true,
			expectTerminateEvent: true,
			expectComplete:       true,
			expectResult: &Result{Backup: BackupResult{
				SnapshotID:       "fake-snapshot-id",
				Source:           AccessPoint{ByPath: "/fake-path", VolMode: uploader.PersistentVolumeBlock},
				TotalBytes:       1073741824,
				IncrementalBytes: 1024,
			}},
			expectProgress: 1,
			expectLog:      "Data path pod fake-pod-1 is in phase Failed after the data path completed, completing backup task fake-du-name with the result from the completed event",
		},
		{
			name:          "deleted after canceled event",
			namespace:     "velero",
			thisPod:       "fake-pod-1",
			thisContainer: "fake-container-1",
			deletePod:     builder.ForPod("velero", "fake-pod-1").Phase(corev1api.PodRunning).Result(),
			insertEventsBefore: []insertEvent{
				{
					event: &corev1api.Event{Reason: EventReasonStarted},
				},
				{
					event: &corev1api.Event{Reason: EventReasonCancelled},
				},
				{
					event: &corev1api.Event{Reason: EventReasonStopped},
				},
			},
			expectStartEvent:     true,
			expectTerminateEvent: true,
			expectCancel:         true,
			expectLog:            "Data path pod fake-pod-1 was deleted in phase Running, canceling backup task fake-du-name through the pod deletion path",
		},
		{
			name:          "deleted with canceled termination message",
			namespace:     "velero",
			thisPod:       "fake-pod-1",
			thisContainer: "fake-container-1",
			deletePod:     builder.ForPod("velero", "fake-pod-1").Phase(corev1api.PodRunning).Result(),
			insertEventsBefore: []insertEvent{
				{
					event: &corev1api.Event{Reason: EventReasonStarted},
				},
				{
					event: &corev1api.Event{Reason: EventReasonStopped},
				},
			},
			terminationMessage:   fmt.Sprintf("Failed to run data path service for DataUpload %s: %v", "fake-du-name", errors.New(ErrCancelled)),
			expectStartEvent:     true,
			expectTerminateEvent: true,
			expectCancel:         true,
			expectLog:            "Data path pod fake-pod-1 was deleted in phase Running, canceling backup task fake-du-name through the pod deletion path",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			eventWaitTimeout = time.Second * 5

			sw := startWatchFake{
				terminationMessage: test.terminationMessage,
				redirectErr:        test.redirectLogErr,
			}
			funcGetPodTerminationMessage = sw.getPodContainerTerminateMessage
			funcRedirectLog = sw.redirectDataMoverLogs
			funcGetResultFromMessage = sw.getResultFromMessage

			logger, logHook := logrustest.NewNullLogger()

			ms := &microServiceBRWatcher{
				ctx:           ctx,
				namespace:     test.namespace,
				taskType:      TaskTypeBackup,
				taskName:      "fake-du-name",
				thisPod:       test.thisPod,
				thisContainer: test.thisContainer,
				podCh:         make(chan *corev1api.Pod, 2),
				eventCh:       make(chan *corev1api.Event, 10),
				log:           logger,
				callbacks: Callbacks{
					OnCompleted: sw.OnCompleted,
					OnFailed:    sw.OnFailed,
					OnCancelled: sw.OnCancelled,
					OnProgress:  sw.OnProgress,
				},
			}

			ms.startWatch()

			if test.ctxCancel {
				cancel()
			}

			for _, ev := range test.insertEventsBefore {
				if ev.after != 0 {
					time.Sleep(ev.after)
				}

				ms.eventCh <- ev.event

				if ev.delay != 0 {
					time.Sleep(ev.delay)
				}
			}

			if test.insertPod != nil {
				ms.podCh <- test.insertPod
			}

			if test.deletePod != nil {
				if test.deleteTombstone {
					ms.onPodDeleted(cache.DeletedFinalStateUnknown{Key: "velero/" + test.deletePod.Name, Obj: test.deletePod})
				} else {
					ms.onPodDeleted(test.deletePod)
				}
			}

			for _, ev := range test.insertEventsAfter {
				if ev.after != 0 {
					time.Sleep(ev.after)
				}

				ms.eventCh <- ev.event

				if ev.delay != 0 {
					time.Sleep(ev.delay)
				}
			}

			ms.wgWatcher.Wait()

			assert.Equal(t, test.expectStartEvent, ms.startedFromEvent)
			assert.Equal(t, test.expectTerminateEvent, ms.terminatedFromEvent)
			assert.Equal(t, test.expectComplete, sw.complete)
			assert.Equal(t, test.expectCancel, sw.canceled)
			assert.Equal(t, test.expectFail, sw.failed)
			assert.Equal(t, test.expectProgress, sw.progress)

			if test.expectFailMsg != "" {
				require.Error(t, sw.failErr)
				assert.Equal(t, test.expectFailMsg, sw.failErr.Error())
			}

			if test.expectResult != nil {
				assert.Equal(t, *test.expectResult, sw.result)
			}

			if test.expectLog != "" {
				found := false
				for _, entry := range logHook.AllEntries() {
					if entry.Message == test.expectLog {
						found = true
						break
					}
				}
				assert.True(t, found, "expected log %q not found", test.expectLog)
			}

			cancel()
		})
	}
}

func TestOnPodDeleted(t *testing.T) {
	thisPod := builder.ForPod("velero", "fake-pod-1").Phase(corev1api.PodRunning).Result()

	tests := []struct {
		name      string
		obj       any
		podChLen  int
		expectPod bool
	}{
		{
			name:      "this pod is deleted",
			obj:       thisPod,
			expectPod: true,
		},
		{
			name:      "this pod is deleted with final state unknown",
			obj:       cache.DeletedFinalStateUnknown{Key: "velero/fake-pod-1", Obj: thisPod},
			expectPod: true,
		},
		{
			name: "other pod is deleted",
			obj:  builder.ForPod("velero", "fake-pod-2").Phase(corev1api.PodRunning).Result(),
		},
		{
			name: "pod in other namespace is deleted",
			obj:  builder.ForPod("fake-ns", "fake-pod-1").Phase(corev1api.PodRunning).Result(),
		},
		{
			name: "other pod is deleted with final state unknown",
			obj:  cache.DeletedFinalStateUnknown{Key: "velero/fake-pod-2", Obj: builder.ForPod("velero", "fake-pod-2").Phase(corev1api.PodRunning).Result()},
		},
		{
			name: "final state unknown with unexpected object",
			obj:  cache.DeletedFinalStateUnknown{Key: "velero/fake-pod-1", Obj: &corev1api.Event{}},
		},
		{
			name: "unexpected object",
			obj:  &corev1api.Event{},
		},
		{
			name:      "pod channel is full",
			obj:       thisPod,
			podChLen:  2,
			expectPod: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logger, logHook := logrustest.NewNullLogger()

			ms := &microServiceBRWatcher{
				namespace: "velero",
				taskType:  TaskTypeBackup,
				taskName:  "fake-du-name",
				thisPod:   "fake-pod-1",
				podCh:     make(chan *corev1api.Pod, 2),
				log:       logger,
			}

			for range test.podChLen {
				ms.podCh <- builder.ForPod("velero", "fake-pod-1").Phase(corev1api.PodFailed).Result()
			}

			// the hand-off must never block the informer handler, even when the channel is full
			ms.onPodDeleted(test.obj)

			if !test.expectPod {
				assert.Empty(t, ms.podCh)
				assert.Empty(t, logHook.AllEntries())
				return
			}

			require.NotNil(t, logHook.LastEntry())
			assert.Equal(t, "Data path pod fake-pod-1 of backup task fake-du-name is deleted in phase Running", logHook.LastEntry().Message)

			if test.podChLen > 0 {
				assert.Len(t, ms.podCh, test.podChLen)
				return
			}

			require.Len(t, ms.podCh, 1)
			pod := <-ms.podCh
			assert.Equal(t, corev1api.PodRunning, pod.Status.Phase)
		})
	}
}

func TestGetResultFromMessage(t *testing.T) {
	tests := []struct {
		name         string
		taskType     string
		message      string
		expectResult Result
	}{
		{
			name:         "error to unmarshall backup result",
			taskType:     TaskTypeBackup,
			message:      "fake-message",
			expectResult: Result{},
		},
		{
			name:         "error to unmarshall restore result",
			taskType:     TaskTypeRestore,
			message:      "fake-message",
			expectResult: Result{},
		},
		{
			name:     "succeed to unmarshall backup result",
			taskType: TaskTypeBackup,
			message:  "{\"snapshotID\":\"fake-snapshot-id\",\"emptySnapshot\":true,\"source\":{\"byPath\":\"fake-path-1\",\"volumeMode\":\"Block\"}}",
			expectResult: Result{
				Backup: BackupResult{
					SnapshotID:    "fake-snapshot-id",
					EmptySnapshot: true,
					Source: AccessPoint{
						ByPath:  "fake-path-1",
						VolMode: uploader.PersistentVolumeBlock,
					},
				},
			},
		},
		{
			name:     "succeed to unmarshall restore result",
			taskType: TaskTypeRestore,
			message:  "{\"target\":{\"byPath\":\"fake-path-2\",\"volumeMode\":\"Filesystem\"}}",
			expectResult: Result{
				Restore: RestoreResult{
					Target: AccessPoint{
						ByPath:  "fake-path-2",
						VolMode: uploader.PersistentVolumeFilesystem,
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := getResultFromMessage(test.taskType, test.message, velerotest.NewLogger())
			assert.Equal(t, test.expectResult, result)
		})
	}
}

func TestGetProgressFromMessage(t *testing.T) {
	tests := []struct {
		name           string
		message        string
		expectProgress uploader.Progress
	}{
		{
			name:           "error to unmarshall progress",
			message:        "fake-message",
			expectProgress: uploader.Progress{},
		},
		{
			name:    "succeed to unmarshall progress",
			message: "{\"totalBytes\":1000,\"doneBytes\":200}",
			expectProgress: uploader.Progress{
				TotalBytes: 1000,
				BytesDone:  200,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			progress := getProgressFromMessage(test.message, velerotest.NewLogger())
			assert.Equal(t, test.expectProgress, *progress)
		})
	}
}

type redirectFake struct {
	logFile       *os.File
	createTempErr error
	getPodLogErr  error
	logMessage    string
}

func (rf *redirectFake) fakeCreateTempFile(_ string, _ string) (*os.File, error) {
	if rf.createTempErr != nil {
		return nil, rf.createTempErr
	}

	return rf.logFile, nil
}

func (rf *redirectFake) fakeCollectPodLogs(_ context.Context, _ corev1client.CoreV1Interface, _ string, _ string, _ string, output io.Writer) error {
	if rf.getPodLogErr != nil {
		return rf.getPodLogErr
	}

	_, err := output.Write([]byte(rf.logMessage))

	return err
}

func TestRedirectDataMoverLogs(t *testing.T) {
	logFileName := path.Join(os.TempDir(), "test-logger-file.log")

	var buffer string

	tests := []struct {
		name          string
		thisPod       string
		logMessage    string
		logger        logrus.FieldLogger
		createTempErr error
		collectLogErr error
		expectErr     string
	}{
		{
			name:          "error to create temp file",
			thisPod:       "fake-pod",
			createTempErr: errors.New("fake-create-temp-error"),
			logger:        velerotest.NewLogger(),
			expectErr:     "error to create temp file for data mover pod log: fake-create-temp-error",
		},
		{
			name:          "error to collect pod log",
			thisPod:       "fake-pod",
			collectLogErr: errors.New("fake-collect-log-error"),
			logger:        velerotest.NewLogger(),
			expectErr:     fmt.Sprintf("error to collect logs to %s for data mover pod fake-pod: fake-collect-log-error", logFileName),
		},
		{
			name:       "succeed",
			thisPod:    "fake-pod",
			logMessage: "fake-log-message-01\nfake-log-message-02\nfake-log-message-03\n",
			logger:     velerotest.NewSingleLoggerWithHooks(&buffer, logging.DefaultHooks(true)),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			buffer = ""

			logFile, err := os.Create(logFileName)
			require.NoError(t, err)

			rf := redirectFake{
				logFile:       logFile,
				createTempErr: test.createTempErr,
				getPodLogErr:  test.collectLogErr,
				logMessage:    test.logMessage,
			}

			funcCreateTemp = rf.fakeCreateTempFile
			funcCollectPodLogs = rf.fakeCollectPodLogs

			fakeKubeClient := kubeclientfake.NewSimpleClientset()

			err = redirectDataMoverLogs(t.Context(), fakeKubeClient, "", test.thisPod, "", test.logger)
			if test.expectErr != "" {
				assert.EqualError(t, err, test.expectErr)
			} else {
				require.NoError(t, err)

				assert.Contains(t, buffer, test.logMessage)
			}
		})
	}
}
