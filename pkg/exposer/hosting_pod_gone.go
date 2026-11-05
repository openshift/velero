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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ErrHostingPodGone is returned (wrapped) by PeekExposed when the data mover pod
// created by Expose was deleted or evicted (or is being deleted) before the data
// path started. Such a CR can never reach Prepared, so it should not wait for the
// prepare timeout.
var ErrHostingPodGone = errors.New("data mover pod was deleted or evicted while preparing")

// HostingPodGoneGracePeriod is how long a missing data mover pod is tolerated
// after the CR was accepted, or after the expose object that is created right
// before the pod. It covers the window between the phase moving to Accepted and
// Expose creating the pod, so a CR whose pod is simply not created yet is never
// canceled.
const HostingPodGoneGracePeriod = 2 * time.Minute

// IgnoreHostingPodGoneInGracePeriod returns nil if err is ErrHostingPodGone and
// the CR was accepted less than HostingPodGoneGracePeriod ago (or the accepted
// time is unknown). Otherwise it returns err unchanged.
func IgnoreHostingPodGoneInGracePeriod(err error, acceptedTimestamp *metav1.Time) error {
	if err == nil || !errors.Is(err, ErrHostingPodGone) {
		return err
	}

	if acceptedTimestamp == nil || time.Since(acceptedTimestamp.Time) < HostingPodGoneGracePeriod {
		return nil
	}

	return err
}
