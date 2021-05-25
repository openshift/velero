/*
Copyright 2017, 2019 the Velero contributors.

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

package discovery

import (
	"sort"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	kcmdutil "github.com/vmware-tanzu/velero/third_party/kubernetes/pkg/kubectl/cmd/util"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/restmapper"
)

// Helper exposes functions for interacting with the Kubernetes discovery
// API.
type Helper interface {
	// Resources gets the current set of resources retrieved from discovery
	// that are backuppable by Velero.
	Resources() []*metav1.APIResourceList

	// ResourceFor gets a fully-resolved GroupVersionResource and an
	// APIResource for the provided partially-specified GroupVersionResource.
	ResourceFor(input schema.GroupVersionResource) (schema.GroupVersionResource, metav1.APIResource, error)

	// KindFor gets a fully-resolved GroupVersionResource and an
	// APIResource for the provided partially-specified GroupVersionKind.
	KindFor(input schema.GroupVersionKind) (schema.GroupVersionResource, metav1.APIResource, error)

	// Refresh pulls an updated set of Velero-backuppable resources from the
	// discovery API.
	Refresh() error

	// APIGroups gets the current set of supported APIGroups
	// in the cluster.
	APIGroups() []metav1.APIGroup

	// ServerVersion retrieves and parses the server's k8s version (git version)
	// in the cluster.
	ServerVersion() *version.Info

	// SetShortcutExpanderFunc is used to set a customer shortcut expander. Once set
	// the helper implementation is supposed to store this and invoke during Refresh as needed
	SetShortcutExpanderFunc() error

	// This function is not thread-safe, only meant for internal consumption for refresh function
	// which is why it is unexported. It can be configured by SetShortcutExpanderFunc which is thread-safe.
	shortcutExpander() (meta.RESTMapper, error)
}

type serverResourcesInterface interface {
	// ServerPreferredResources() is used to populate Resources() with only Preferred Versions - this is the default
	ServerPreferredResources() ([]*metav1.APIResourceList, error)
	// ServerGroupsAndResources returns supported groups and resources for *all* groups and versions
	// Used to populate Resources() if feature flag is passed
	ServerGroupsAndResources() ([]*metav1.APIGroup, []*metav1.APIResourceList, error)
}

type helper struct {
	discoveryClient        discovery.DiscoveryInterface
	logger                 logrus.FieldLogger
	apiVersionsFeatureFlag bool

	// lock guards mapper, resources and resourcesMap
	lock          sync.RWMutex
	mapper        meta.RESTMapper
	resources     []*metav1.APIResourceList
	resourcesMap  map[schema.GroupVersionResource]metav1.APIResource
	kindMap       map[schema.GroupVersionKind]metav1.APIResource
	apiGroups     []metav1.APIGroup
	serverVersion *version.Info

	// this is needed so we can override the shortcut expander at
	// compile time.
	shortcutExpanderFunc func() (meta.RESTMapper, error)
}

var _ Helper = &helper{}

func NewHelper(discoveryClient discovery.DiscoveryInterface, logger logrus.FieldLogger, apiVersionsFeatureFlag bool) (Helper, error) {
	h := &helper{
		discoveryClient:        discoveryClient,
		logger:                 logger,
		apiVersionsFeatureFlag: apiVersionsFeatureFlag,
	}
	if err := h.Refresh(); err != nil {
		return nil, err
	}
	return h, nil
}

func (h *helper) ResourceFor(input schema.GroupVersionResource) (schema.GroupVersionResource, metav1.APIResource, error) {
	var err error
	err = h.SetShortcutExpanderFunc()
	if err != nil {
		return schema.GroupVersionResource{}, metav1.APIResource{}, err
	}

	h.lock.RLock()
	defer h.lock.RUnlock()

	h.mapper, err = h.shortcutExpander()
	if err != nil {
		return schema.GroupVersionResource{}, metav1.APIResource{}, err
	}
	gvr, err := h.mapper.ResourceFor(input)
	if err != nil {
		return schema.GroupVersionResource{}, metav1.APIResource{}, err
	}

	apiResource, found := h.resourcesMap[gvr]
	if !found {
		return schema.GroupVersionResource{}, metav1.APIResource{}, errors.Errorf("APIResource not found for GroupVersionResource %s", gvr)
	}

	return gvr, apiResource, nil
}

func (h *helper) KindFor(input schema.GroupVersionKind) (schema.GroupVersionResource, metav1.APIResource, error) {
	var err error
	h.SetShortcutExpanderFunc()
	err = h.SetShortcutExpanderFunc()
	if err != nil {
		return schema.GroupVersionResource{}, metav1.APIResource{}, err
	}

	h.lock.RLock()
	defer h.lock.RUnlock()

	h.mapper, err = h.shortcutExpander()
	if err != nil {
		return schema.GroupVersionResource{}, metav1.APIResource{}, err
	}
	if resource, ok := h.kindMap[input]; ok {
		return schema.GroupVersionResource{
			Group:    resource.Group,
			Version:  resource.Version,
			Resource: resource.Name,
		}, resource, nil
	}
	m, err := h.mapper.RESTMapping(schema.GroupKind{Group: input.Group, Kind: input.Kind}, input.Version)
	if err != nil {
		return schema.GroupVersionResource{}, metav1.APIResource{}, err
	}
	if resource, ok := h.kindMap[m.GroupVersionKind]; ok {
		return schema.GroupVersionResource{
			Group:    resource.Group,
			Version:  resource.Version,
			Resource: resource.Name,
		}, resource, nil
	}
	return schema.GroupVersionResource{}, metav1.APIResource{}, errors.Errorf("APIResource not found for GroupVersionKind %v ", input)
}

func (h *helper) Refresh() error {
	h.lock.Lock()
	defer h.lock.Unlock()

	var serverResources []*metav1.APIResourceList

	if h.apiVersionsFeatureFlag {
		// ServerGroupsAndResources returns all APIGroup and APIResouceList - not only preferred versions
		_, serverAllResources, err := refreshServerGroupsAndResources(h.discoveryClient, h.logger)
		if err != nil {
			return errors.WithStack(err)
		}
		h.logger.Infof("The '%s' feature flag was specified, using all API group versions.", h.apiVersionsFeatureFlag)
		serverResources = serverAllResources
	} else {
		// ServerPreferredResources() returns only preferred APIGroup - this is the default since no feature flag has been passed
		serverPreferredResources, err := refreshServerPreferredResources(h.discoveryClient, h.logger)
		if err != nil {
			return errors.WithStack(err)
		}
		serverResources = serverPreferredResources
	}

	// TODO: parameterize this but not needed immediately
	h.resources = discovery.FilteredBy(
		discovery.ResourcePredicateFunc(filterByVerbs),
		serverResources,
	)

	// this mainly handles extensions/apps priority for deployments and like resources
	sortResources(h.resources)

	h.resourcesMap = make(map[schema.GroupVersionResource]metav1.APIResource)
	h.kindMap = make(map[schema.GroupVersionKind]metav1.APIResource)
	for _, resourceGroup := range h.resources {
		gv, err := schema.ParseGroupVersion(resourceGroup.GroupVersion)
		if err != nil {
			return errors.Wrapf(err, "unable to parse GroupVersion %s", resourceGroup.GroupVersion)
		}

		for _, resource := range resourceGroup.APIResources {
			gvr := gv.WithResource(resource.Name)
			gvk := gv.WithKind(resource.Kind)
			h.resourcesMap[gvr] = resource
			h.kindMap[gvk] = resource
		}
	}

	apiGroupList, err := h.discoveryClient.ServerGroups()
	if err != nil {
		return errors.WithStack(err)
	}
	h.apiGroups = apiGroupList.Groups

	serverVersion, err := h.discoveryClient.ServerVersion()
	if err != nil {
		return errors.WithStack(err)
	}

	h.serverVersion = serverVersion

	return nil
}

func refreshServerPreferredResources(discoveryClient serverResourcesInterface, logger logrus.FieldLogger) ([]*metav1.APIResourceList, error) {
	preferredResources, err := discoveryClient.ServerPreferredResources()
	if err != nil {
		if discoveryErr, ok := err.(*discovery.ErrGroupDiscoveryFailed); ok {
			for groupVersion, err := range discoveryErr.Groups {
				logger.WithError(err).Warnf("Failed to discover group: %v", groupVersion)
			}
			return preferredResources, nil
		}
	}
	return preferredResources, err
}

func refreshServerGroupsAndResources(discoveryClient serverResourcesInterface, logger logrus.FieldLogger) ([]*metav1.APIGroup, []*metav1.APIResourceList, error) {
	serverGroups, serverResources, err := discoveryClient.ServerGroupsAndResources()
	if err != nil {
		if discoveryErr, ok := err.(*discovery.ErrGroupDiscoveryFailed); ok {
			for groupVersion, err := range discoveryErr.Groups {
				logger.WithError(err).Warnf("Failed to discover group: %v", groupVersion)
			}
			return serverGroups, serverResources, nil
		}
	}
	return serverGroups, serverResources, err
}

func filterByVerbs(groupVersion string, r *metav1.APIResource) bool {
	return discovery.SupportsAllVerbs{Verbs: []string{"list", "create", "get", "delete"}}.Match(groupVersion, r)
}

// sortResources sources resources by moving extensions to the end of the slice. The order of all
// the other resources is preserved.
func sortResources(resources []*metav1.APIResourceList) {
	sort.SliceStable(resources, func(i, j int) bool {
		left := resources[i]
		leftGV, _ := schema.ParseGroupVersion(left.GroupVersion)
		// not checking error because it should be impossible to fail to parse data coming from the
		// apiserver
		if leftGV.Group == "extensions" {
			// always sort extensions at the bottom by saying left is "greater"
			return false
		}

		right := resources[j]
		rightGV, _ := schema.ParseGroupVersion(right.GroupVersion)
		// not checking error because it should be impossible to fail to parse data coming from the
		// apiserver
		if rightGV.Group == "extensions" {
			// always sort extensions at the bottom by saying left is "less"
			return true
		}

		return i < j
	})
}

func (h *helper) Resources() []*metav1.APIResourceList {
	h.lock.RLock()
	defer h.lock.RUnlock()
	return h.resources
}

func (h *helper) APIGroups() []metav1.APIGroup {
	h.lock.RLock()
	defer h.lock.RUnlock()
	return h.apiGroups
}

func (h *helper) ServerVersion() *version.Info {
	h.lock.RLock()
	defer h.lock.RUnlock()
	return h.serverVersion
}

func (h *helper) Logger() logrus.FieldLogger {
	h.lock.RLock()
	defer h.lock.RUnlock()
	return h.logger
}

func (h *helper) SetShortcutExpanderFunc() error {
	h.lock.RLock()
	defer h.lock.RUnlock()
	groupResources, err := restmapper.GetAPIGroupResources(h.discoveryClient)
	if err != nil {
		return err
	}

	h.shortcutExpanderFunc = func() (meta.RESTMapper, error) {
		return kcmdutil.NewShortcutExpander(restmapper.NewDiscoveryRESTMapper(groupResources), h.Resources(), h.logger)
	}
	return nil
}

func (h *helper) shortcutExpander() (meta.RESTMapper, error) {
	if h.shortcutExpanderFunc == nil {
		return nil, errors.Errorf("shortcutExpanderFunc is nil, use the SetShortcutExpanderFunc to use this method\n")
	}
	return h.shortcutExpanderFunc()
}
