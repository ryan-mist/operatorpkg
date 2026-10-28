/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

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

package status_test

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/awslabs/operatorpkg/object"
	"github.com/awslabs/operatorpkg/status"
	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// mapClient is a minimal stand-in for controller-runtime's cache reader: Get deep-copies the stored object into the
// caller's object and sets its GVK. Only Get is implemented. It avoids the fake client's serialization round trip so
// that benchmarks measure the controller rather than the client.
type mapClient struct {
	client.Client
	mu      sync.RWMutex
	objects map[types.NamespacedName]client.Object
	rv      int
}

func newMapClient() *mapClient {
	return &mapClient{objects: map[types.NamespacedName]client.Object{}}
}

func (m *mapClient) Get(_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	stored, ok := m.objects[key]
	if !ok {
		return errors.NewNotFound(object.GVK(obj).GroupVersion().WithResource(strings.ToLower(object.GVK(obj).Kind)).GroupResource(), key.Name)
	}
	reflect.ValueOf(obj).Elem().Set(reflect.ValueOf(stored.DeepCopyObject()).Elem())
	obj.GetObjectKind().SetGroupVersionKind(object.GVK(stored))
	return nil
}

// apply stores a copy of obj with a new resourceVersion, like an API server write
func (m *mapClient) apply(obj client.Object) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rv++
	obj = obj.DeepCopyObject().(client.Object)
	obj.SetResourceVersion(strconv.Itoa(m.rv))
	m.objects[client.ObjectKeyFromObject(obj)] = obj
}

// touch gives the stored object a new resourceVersion without changing anything else, like a no-op update
func (m *mapClient) touch(key types.NamespacedName) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rv++
	m.objects[key].SetResourceVersion(strconv.Itoa(m.rv))
}

func (m *mapClient) delete(key types.NamespacedName) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)
}

// benchmarkNode returns a Node shaped like a real cluster node: ~60 labels, 10 conditions (5 kubelet + 5
// node-problem-detector), images, addresses, resources and managedFields.
func benchmarkNode(name string) *corev1.Node {
	labels := map[string]string{
		corev1.LabelHostname:             name,
		corev1.LabelInstanceTypeStable:   "m5.xlarge",
		corev1.LabelTopologyZone:         "us-west-2a",
		corev1.LabelTopologyRegion:       "us-west-2",
		corev1.LabelOSStable:             "linux",
		corev1.LabelArchStable:           "amd64",
		"karpenter.sh/nodepool":          "default",
		"karpenter.sh/initialized":       "true",
		"karpenter.sh/registered":        "true",
		"karpenter.sh/capacity-type":     "on-demand",
		"karpenter.k8s.aws/ec2nodeclass": "default",
	}
	for i := 0; len(labels) < 60; i++ {
		labels[fmt.Sprintf("karpenter.k8s.aws/instance-attribute-%d", i)] = fmt.Sprintf("value-%d", i)
	}
	ltt := metav1.NewTime(time.Now().Add(-time.Hour).Truncate(time.Second))
	heartbeat := metav1.NewTime(time.Now().Truncate(time.Second))
	var conditions []corev1.NodeCondition
	for _, c := range []struct {
		t      corev1.NodeConditionType
		status corev1.ConditionStatus
		reason string
	}{
		{corev1.NodeReady, corev1.ConditionTrue, "KubeletReady"},
		{corev1.NodeMemoryPressure, corev1.ConditionFalse, "KubeletHasSufficientMemory"},
		{corev1.NodeDiskPressure, corev1.ConditionFalse, "KubeletHasNoDiskPressure"},
		{corev1.NodePIDPressure, corev1.ConditionFalse, "KubeletHasSufficientPID"},
		{corev1.NodeNetworkUnavailable, corev1.ConditionFalse, "RouteCreated"},
		{"KernelDeadlock", corev1.ConditionFalse, "KernelHasNoDeadlock"},
		{"ReadonlyFilesystem", corev1.ConditionFalse, "FilesystemIsNotReadOnly"},
		{"FrequentKubeletRestart", corev1.ConditionFalse, "NoFrequentKubeletRestart"},
		{"FrequentContainerdRestart", corev1.ConditionFalse, "NoFrequentContainerdRestart"},
		{"CorruptDockerOverlay2", corev1.ConditionFalse, "NoCorruptDockerOverlay2"},
	} {
		conditions = append(conditions, corev1.NodeCondition{Type: c.t, Status: c.status, Reason: c.reason, Message: "condition " + string(c.t), LastHeartbeatTime: heartbeat, LastTransitionTime: ltt})
	}
	var images []corev1.ContainerImage
	for i := range 20 {
		images = append(images, corev1.ContainerImage{
			Names:     []string{fmt.Sprintf("registry.example.com/team/image-%d@sha256:%064d", i, i), fmt.Sprintf("registry.example.com/team/image-%d:v1.2.%d", i, i)},
			SizeBytes: int64(1e8 + i),
		})
	}
	resources := corev1.ResourceList{
		corev1.ResourceCPU:              resource.MustParse("3920m"),
		corev1.ResourceMemory:           resource.MustParse("14848Mi"),
		corev1.ResourcePods:             resource.MustParse("58"),
		corev1.ResourceEphemeralStorage: resource.MustParse("95551679124"),
		"hugepages-1Gi":                 resource.MustParse("0"),
		"hugepages-2Mi":                 resource.MustParse("0"),
	}
	var managedFields []metav1.ManagedFieldsEntry
	for _, manager := range []string{"kubelet", "karpenter", "kube-controller-manager", "aws-node"} {
		managedFields = append(managedFields, metav1.ManagedFieldsEntry{
			Manager:    manager,
			Operation:  metav1.ManagedFieldsOperationUpdate,
			APIVersion: "v1",
			Time:       &heartbeat,
			FieldsType: "FieldsV1",
			FieldsV1:   &metav1.FieldsV1{Raw: []byte(`{"f:metadata":{"f:labels":{` + strings.Repeat(`"f:example.com/some-label-key":{},`, 30) + `"f:last":{}}},"f:status":{"f:conditions":{"k:{\"type\":\"Ready\"}":{"f:lastHeartbeatTime":{},"f:message":{},"f:reason":{},"f:status":{}}}}}`)},
		})
	}
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			UID:               types.UID("uid-" + name),
			Generation:        1,
			CreationTimestamp: ltt,
			Labels:            labels,
			Annotations: map[string]string{
				"alpha.kubernetes.io/provided-node-ip":                   "10.0.0.1",
				"node.alpha.kubernetes.io/ttl":                           "0",
				"volumes.kubernetes.io/controller-managed-attach-detach": "true",
				"karpenter.k8s.aws/ec2nodeclass-hash":                    "1234567890",
				"karpenter.sh/nodepool-hash":                             "1234567890",
			},
			Finalizers:    []string{"karpenter.sh/termination"},
			ManagedFields: managedFields,
		},
		Spec: corev1.NodeSpec{
			ProviderID: "aws:///us-west-2a/i-0123456789abcdef0",
			Taints:     []corev1.Taint{{Key: "example.com/taint", Value: "true", Effect: corev1.TaintEffectNoSchedule}},
		},
		Status: corev1.NodeStatus{
			Capacity:    resources,
			Allocatable: resources,
			Conditions:  conditions,
			Addresses: []corev1.NodeAddress{
				{Type: corev1.NodeInternalIP, Address: "10.0.0.1"},
				{Type: corev1.NodeHostName, Address: name},
				{Type: corev1.NodeInternalDNS, Address: name + ".us-west-2.compute.internal"},
			},
			DaemonEndpoints: corev1.NodeDaemonEndpoints{KubeletEndpoint: corev1.DaemonEndpoint{Port: 10250}},
			NodeInfo: corev1.NodeSystemInfo{
				MachineID: "ec2" + name, SystemUUID: "ec2" + name, BootID: "boot-" + name,
				KernelVersion: "6.1.0", OSImage: "Amazon Linux 2023", ContainerRuntimeVersion: "containerd://1.7.0",
				KubeletVersion: "v1.34.0", OperatingSystem: "linux", Architecture: "amd64",
			},
			Images: images,
		},
	}
}

// BenchmarkGenericNodeReconcile measures a Reconcile of a Node by the generic status controller, configured the way
// karpenter configures it, with 1 and 5000 Nodes tracked: steady state (the 10s requeue, nothing changed) and after an
// update that changed nothing the controller reads (a new resourceVersion only).
func BenchmarkGenericNodeReconcile(b *testing.B) {
	for _, bc := range []struct {
		n       int
		updated bool
	}{{1, false}, {5000, false}, {1, true}, {5000, true}} {
		n := bc.n
		b.Run(fmt.Sprintf("nodes=%d/updated=%t", n, bc.updated), func(b *testing.B) {
			original := metrics.Registry
			metrics.Registry = prometheus.NewRegistry()
			defer func() { metrics.Registry = original }()

			kubeClient := newMapClient()
			controller := status.NewGenericObjectController[*corev1.Node](kubeClient, record.NewFakeRecorder(100),
				status.WithLabels("karpenter.k8s.aws/ec2nodeclass", "karpenter.sh/nodepool", "karpenter.sh/initialized"))
			ctx := context.Background()
			reqs := make([]reconcile.Request, n)
			for i := range reqs {
				node := benchmarkNode(fmt.Sprintf("node-%d", i))
				kubeClient.apply(node)
				reqs[i] = reconcile.Request{NamespacedName: client.ObjectKeyFromObject(node)}
				if _, err := controller.Reconcile(ctx, reqs[i]); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				if bc.updated {
					kubeClient.touch(reqs[i%n].NamespacedName)
				}
				if _, err := controller.Reconcile(ctx, reqs[i%n]); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
