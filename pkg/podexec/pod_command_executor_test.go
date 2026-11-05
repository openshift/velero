/*
Copyright 2017, 2020 the Velero contributors.

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

package podexec

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	corev1api "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	restfake "k8s.io/client-go/rest/fake"
	"k8s.io/client-go/tools/remotecommand"

	v1 "github.com/vmware-tanzu/velero/pkg/apis/velero/v1"
	velerotest "github.com/vmware-tanzu/velero/pkg/test"
)

func TestNewPodCommandExecutor(t *testing.T) {
	restClientConfig := &rest.Config{Host: "foo"}
	poster := &mockPoster{}
	pce := NewPodCommandExecutor(restClientConfig, poster).(*defaultPodCommandExecutor)
	assert.Equal(t, restClientConfig, pce.restClientConfig)
	assert.Equal(t, poster, pce.restClient)
	assert.Equal(t, &defaultStreamExecutorFactory{}, pce.streamExecutorFactory)
}

func TestExecutePodCommandMissingInputs(t *testing.T) {
	tests := []struct {
		name         string
		item         map[string]any
		podNamespace string
		podName      string
		hookName     string
		hook         *v1.ExecHook
	}{
		{
			name: "missing item",
		},
		{
			name: "missing pod namespace",
			item: map[string]any{},
		},
		{
			name:         "missing pod name",
			item:         map[string]any{},
			podNamespace: "ns",
		},
		{
			name:         "missing hookName",
			item:         map[string]any{},
			podNamespace: "ns",
			podName:      "pod",
		},
		{
			name:         "missing hook",
			item:         map[string]any{},
			podNamespace: "ns",
			podName:      "pod",
			hookName:     "hook",
		},
		{
			name:         "container not found",
			item:         velerotest.UnstructuredOrDie(`{"kind":"Pod","spec":{"containers":[{"name":"foo"}]}}`).Object,
			podNamespace: "ns",
			podName:      "pod",
			hookName:     "hook",
			hook: &v1.ExecHook{
				Container: "missing",
			},
		},
		{
			name:         "command missing",
			item:         velerotest.UnstructuredOrDie(`{"kind":"Pod","spec":{"containers":[{"name":"foo"}]}}`).Object,
			podNamespace: "ns",
			podName:      "pod",
			hookName:     "hook",
			hook: &v1.ExecHook{
				Container: "foo",
			},
		},
		{
			name:         "hook's container is not overwritten by pod",
			item:         velerotest.UnstructuredOrDie(`{"kind":"Pod","spec":{"containers":[{"name":"foo"}]}}`).Object,
			podNamespace: "ns",
			podName:      "pod",
			hookName:     "hook",
			hook: &v1.ExecHook{
				Container: "",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pod := new(corev1api.Pod)
			hookPodContainerNotSame := false
			if err := runtime.DefaultUnstructuredConverter.FromUnstructured(test.item, pod); err != nil {
				assert.Error(t, err)
			}
			if (len(pod.Spec.Containers) > 0) && (pod.Spec.Containers[0].Name != test.hook.Container) {
				hookPodContainerNotSame = true
			}

			e := &defaultPodCommandExecutor{}
			err := e.ExecutePodCommand(velerotest.NewLogger(), test.item, test.podNamespace, test.podName, test.hookName, test.hook)

			if hookPodContainerNotSame && test.hook.Container == pod.Spec.Containers[0].Name {
				require.Error(t, fmt.Errorf("hook exec container is overwritten"))
			}
			assert.Error(t, err)
		})
	}
}

func TestExecutePodCommand(t *testing.T) {
	tests := []struct {
		name                  string
		containerName         string
		expectedContainerName string
		command               []string
		errorMode             v1.HookErrorMode
		expectedErrorMode     v1.HookErrorMode
		timeout               time.Duration
		expectedTimeout       time.Duration
		hookError             error
		expectedError         string
	}{
		{
			name:                  "validate defaults",
			command:               []string{"some", "command"},
			expectedContainerName: "foo",
			expectedErrorMode:     v1.HookErrorModeFail,
			expectedTimeout:       30 * time.Second,
		},
		{
			name:                  "use specified values",
			command:               []string{"some", "command"},
			containerName:         "bar",
			expectedContainerName: "bar",
			errorMode:             v1.HookErrorModeContinue,
			expectedErrorMode:     v1.HookErrorModeContinue,
			timeout:               10 * time.Second,
			expectedTimeout:       10 * time.Second,
		},
		{
			name:                  "hook error",
			command:               []string{"some", "command"},
			expectedContainerName: "foo",
			expectedErrorMode:     v1.HookErrorModeFail,
			expectedTimeout:       30 * time.Second,
			hookError:             errors.New("hook error"),
			expectedError:         "hook error",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			hook := v1.ExecHook{
				Container: test.containerName,
				Command:   test.command,
				OnError:   test.errorMode,
				Timeout:   metav1.Duration{Duration: test.timeout},
			}

			pod, err := velerotest.GetAsMap(`
{
	"metadata": {
		"namespace": "namespace",
		"name": "name"
	},
	"spec": {
		"containers": [
			{"name": "foo"},
			{"name": "bar"}
		]
	}
}`)

			require.NoError(t, err)

			clientConfig := &rest.Config{}
			poster := &mockPoster{}
			defer poster.AssertExpectations(t)
			podCommandExecutor := NewPodCommandExecutor(clientConfig, poster).(*defaultPodCommandExecutor)

			streamExecutorFactory := &mockStreamExecutorFactory{}
			defer streamExecutorFactory.AssertExpectations(t)
			podCommandExecutor.streamExecutorFactory = streamExecutorFactory

			baseURL, _ := url.Parse("https://some.server")
			contentConfig := rest.ClientContentConfig{
				GroupVersion: schema.GroupVersion{Group: "", Version: "v1"},
			}
			poster.On("Post").Return(rest.NewRequestWithClient(baseURL, "/api/v1", contentConfig, nil))

			streamExecutor := &mockStreamExecutor{}
			defer streamExecutor.AssertExpectations(t)

			expectedCommand := strings.Join(test.command, "&command=")
			expectedURL, _ := url.Parse(
				fmt.Sprintf("https://some.server/api/v1/namespaces/namespace/pods/name/exec?command=%s&container=%s&stderr=true&stdout=true", expectedCommand, test.expectedContainerName),
			)
			streamExecutorFactory.On("NewSPDYExecutor", clientConfig, "POST", expectedURL).Return(streamExecutor, nil)

			var stdout, stderr bytes.Buffer
			expectedStreamOptions := remotecommand.StreamOptions{
				Stdout: &stdout,
				Stderr: &stderr,
			}
			streamExecutor.On("StreamWithContext", mock.Anything, expectedStreamOptions).Return(test.hookError)

			err = podCommandExecutor.ExecutePodCommand(velerotest.NewLogger(), pod, "namespace", "name", "hookName", &hook)
			if test.expectedError != "" {
				assert.EqualError(t, err, test.expectedError)
				return
			}

			require.NoError(t, err)
		})
	}
}

func TestEnsureContainerExists(t *testing.T) {
	pod := &corev1api.Pod{
		Spec: corev1api.PodSpec{
			Containers: []corev1api.Container{
				{
					Name: "foo",
				},
			},
		},
	}

	err := ensureContainerExists(pod, "bar")
	require.EqualError(t, err, `no such container: "bar"`)

	err = ensureContainerExists(pod, "foo")
	assert.NoError(t, err)
}

func TestPodCompeted(t *testing.T) {
	pod := &corev1api.Pod{
		Spec: corev1api.PodSpec{
			Containers: []corev1api.Container{
				{
					Name: "foo",
				},
			},
		},
		Status: corev1api.PodStatus{
			Phase: corev1api.PodSucceeded,
		},
	}

	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(pod)
	require.NoError(t, err)

	clientConfig := &rest.Config{}
	poster := &mockPoster{}
	defer poster.AssertExpectations(t)
	podCommandExecutor := NewPodCommandExecutor(clientConfig, poster).(*defaultPodCommandExecutor)

	hook := v1.ExecHook{
		Container: "foo",
		Command:   []string{"some", "command"},
	}

	err = podCommandExecutor.ExecutePodCommand(velerotest.NewLogger(), obj, "namespace", "name", "hookName", &hook)
	require.NoError(t, err)
}

func TestExecutePodCommandRecheckPod(t *testing.T) {
	now := metav1.Now()

	tests := []struct {
		name       string
		currentPod *corev1api.Pod
		getErr     error
		expectExec bool
		expectLog  string
	}{
		{
			name:       "pod is still running",
			currentPod: &corev1api.Pod{Status: corev1api.PodStatus{Phase: corev1api.PodRunning}},
			expectExec: true,
		},
		{
			name:      "pod no longer exists",
			getErr:    apierrors.NewNotFound(corev1api.Resource("pods"), "name"),
			expectLog: "Skipping exec hook, pod no longer exists since it was collected for backup",
		},
		{
			name:       "pod is being deleted",
			currentPod: &corev1api.Pod{ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &now}, Status: corev1api.PodStatus{Phase: corev1api.PodRunning}},
			expectLog:  "Skipping exec hook, pod is being deleted since it was collected for backup",
		},
		{
			name:       "pod succeeded after migration",
			currentPod: &corev1api.Pod{Status: corev1api.PodStatus{Phase: corev1api.PodSucceeded}},
			expectLog:  "Skipping exec hook, pod entered phase Succeeded since it was collected for backup",
		},
		{
			name:       "pod failed",
			currentPod: &corev1api.Pod{Status: corev1api.PodStatus{Phase: corev1api.PodFailed}},
			expectLog:  "Skipping exec hook, pod entered phase Failed since it was collected for backup",
		},
		{
			name:       "error getting pod runs the hook anyway",
			getErr:     errors.New("fake-get-error"),
			expectExec: true,
			expectLog:  "Failed to get the current pod before running exec hook, running the hook anyway",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// the collected copy of the pod is still running
			pod, err := velerotest.GetAsMap(`
{
	"metadata": {
		"namespace": "namespace",
		"name": "name"
	},
	"spec": {
		"containers": [
			{"name": "compute"}
		]
	},
	"status": {
		"phase": "Running"
	}
}`)
			require.NoError(t, err)

			clientConfig := &rest.Config{}
			poster := &mockPoster{}
			defer poster.AssertExpectations(t)
			podCommandExecutor := NewPodCommandExecutor(clientConfig, poster).(*defaultPodCommandExecutor)
			podCommandExecutor.getPod = func(_ context.Context, namespace, name string) (*corev1api.Pod, error) {
				assert.Equal(t, "namespace", namespace)
				assert.Equal(t, "name", name)
				return test.currentPod, test.getErr
			}

			streamExecutorFactory := &mockStreamExecutorFactory{}
			defer streamExecutorFactory.AssertExpectations(t)
			podCommandExecutor.streamExecutorFactory = streamExecutorFactory

			if test.expectExec {
				baseURL, _ := url.Parse("https://some.server")
				contentConfig := rest.ClientContentConfig{
					GroupVersion: schema.GroupVersion{Group: "", Version: "v1"},
				}
				poster.On("Post").Return(rest.NewRequestWithClient(baseURL, "/api/v1", contentConfig, nil))

				streamExecutor := &mockStreamExecutor{}
				defer streamExecutor.AssertExpectations(t)
				streamExecutorFactory.On("NewSPDYExecutor", clientConfig, "POST", mock.Anything).Return(streamExecutor, nil)
				streamExecutor.On("StreamWithContext", mock.Anything, mock.Anything).Return(nil)
			}

			logger, logHook := logrustest.NewNullLogger()

			hook := v1.ExecHook{
				Container: "compute",
				Command:   []string{"/usr/bin/virt-freezer", "--freeze"},
			}

			err = podCommandExecutor.ExecutePodCommand(logger, pod, "namespace", "name", "hookName", &hook)
			require.NoError(t, err)

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
		})
	}
}

func TestNewPodCommandExecutorGetPod(t *testing.T) {
	tests := []struct {
		name           string
		status         int
		body           runtime.Object
		expectPhase    corev1api.PodPhase
		expectNotFound bool
	}{
		{
			name:        "pod is returned",
			status:      http.StatusOK,
			body:        &corev1api.Pod{TypeMeta: metav1.TypeMeta{Kind: "Pod", APIVersion: "v1"}, ObjectMeta: metav1.ObjectMeta{Namespace: "namespace", Name: "name"}, Status: corev1api.PodStatus{Phase: corev1api.PodSucceeded}},
			expectPhase: corev1api.PodSucceeded,
		},
		{
			name:           "pod is not found",
			status:         http.StatusNotFound,
			body:           &apierrors.NewNotFound(corev1api.Resource("pods"), "name").ErrStatus,
			expectNotFound: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			codec := kscheme.Codecs.LegacyCodec(corev1api.SchemeGroupVersion)
			client := &restfake.RESTClient{
				NegotiatedSerializer: kscheme.Codecs.WithoutConversion(),
				GroupVersion:         corev1api.SchemeGroupVersion,
				Client: restfake.CreateHTTPClient(func(req *http.Request) (*http.Response, error) {
					assert.Equal(t, http.MethodGet, req.Method)
					assert.Equal(t, "/namespaces/namespace/pods/name", req.URL.Path)

					body, err := runtime.Encode(codec, test.body)
					require.NoError(t, err)

					return &http.Response{
						StatusCode: test.status,
						Header:     http.Header{"Content-Type": []string{runtime.ContentTypeJSON}},
						Body:       io.NopCloser(bytes.NewReader(body)),
					}, nil
				}),
			}

			pce := NewPodCommandExecutor(&rest.Config{}, client).(*defaultPodCommandExecutor)
			require.NotNil(t, pce.getPod)

			pod, err := pce.getPod(t.Context(), "namespace", "name")
			if test.expectNotFound {
				assert.True(t, apierrors.IsNotFound(err))
				return
			}

			require.NoError(t, err)
			assert.Equal(t, test.expectPhase, pod.Status.Phase)
		})
	}
}

type mockStreamExecutorFactory struct {
	mock.Mock
}

func (f *mockStreamExecutorFactory) NewSPDYExecutor(config *rest.Config, method string, url *url.URL) (remotecommand.Executor, error) {
	args := f.Called(config, method, url)
	return args.Get(0).(remotecommand.Executor), args.Error(1)
}

type mockStreamExecutor struct {
	mock.Mock
	remotecommand.Executor
}

func (e *mockStreamExecutor) StreamWithContext(ctx context.Context, options remotecommand.StreamOptions) error {
	args := e.Called(ctx, options)
	return args.Error(0)
}

type mockPoster struct {
	mock.Mock
}

func (p *mockPoster) Post() *rest.Request {
	args := p.Called()
	return args.Get(0).(*rest.Request)
}
