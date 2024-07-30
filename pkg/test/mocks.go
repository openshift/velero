package test

import (
	snapshotv1 "github.com/kubernetes-csi/external-snapshotter/client/v7/apis/volumesnapshot/v1"
	snapshotv1listers "github.com/kubernetes-csi/external-snapshotter/client/v7/listers/volumesnapshot/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// VolumeSnapshotLister helps list VolumeSnapshots.
// All objects returned here must be treated as read-only.
//
//go:generate mockery --name VolumeSnapshotLister
type VolumeSnapshotLister interface {
	// List lists all VolumeSnapshots in the indexer.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*snapshotv1.VolumeSnapshot, err error)
	// VolumeSnapshots returns an object that can list and get VolumeSnapshots.
	VolumeSnapshots(namespace string) snapshotv1listers.VolumeSnapshotNamespaceLister
	snapshotv1listers.VolumeSnapshotListerExpansion
}

// Client knows how to perform CRUD operations on Kubernetes objects.

//go:generate mockery --name Client
type Client interface {
	client.Reader
	client.Writer
	client.StatusClient
	client.SubResourceClientConstructor

	// Scheme returns the scheme this client is using.
	Scheme() *runtime.Scheme
	// RESTMapper returns the rest this client is using.
	RESTMapper() meta.RESTMapper
	// GroupVersionKindFor returns the GroupVersionKind for the given object.
	GroupVersionKindFor(obj runtime.Object) (schema.GroupVersionKind, error)
	// IsObjectNamespaced returns true if the GroupVersionKind of the object is namespaced.
	IsObjectNamespaced(obj runtime.Object) (bool, error)
}
