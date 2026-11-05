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

package exposer

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestIgnoreHostingPodGoneInGracePeriod(t *testing.T) {
	podGoneErr := fmt.Errorf("%w: fake-detail", ErrHostingPodGone)
	otherErr := errors.New("fake-error")
	recent := &metav1.Time{Time: time.Now().Add(-time.Second)}
	old := &metav1.Time{Time: time.Now().Add(-HostingPodGoneGracePeriod - time.Minute)}

	tests := []struct {
		name     string
		err      error
		accepted *metav1.Time
		expected error
	}{
		{
			name: "no error",
		},
		{
			name:     "other error is kept in grace period",
			err:      otherErr,
			accepted: recent,
			expected: otherErr,
		},
		{
			name:     "pod gone in grace period is ignored",
			err:      podGoneErr,
			accepted: recent,
		},
		{
			name: "pod gone with unknown accepted time is ignored",
			err:  podGoneErr,
		},
		{
			name:     "pod gone after grace period is kept",
			err:      podGoneErr,
			accepted: old,
			expected: podGoneErr,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, IgnoreHostingPodGoneInGracePeriod(test.err, test.accepted))
		})
	}
}
