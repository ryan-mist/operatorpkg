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

package status

import (
	"reflect"
	"testing"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// capacityBuffer has the shape of karpenter's autoscaling.x-k8s.io/v1beta1 CapacityBuffer
type capacityBuffer struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              capacityBufferSpec   `json:"spec"`
	Status            capacityBufferStatus `json:"status,omitempty"`
}

type capacityBufferSpec struct {
	ProvisioningStrategy *string                `json:"provisioningStrategy,omitempty"`
	PodTemplateRef       *struct{ Name string } `json:"podTemplateRef,omitempty"`
	Replicas             *int32                 `json:"replicas,omitempty"`
	Limits               corev1.ResourceList    `json:"limits,omitempty"`
}

type capacityBufferStatus struct {
	PodTemplateRef        *struct{ Name string } `json:"podTemplateRef,omitempty"`
	Replicas              *int32                 `json:"replicas,omitempty"`
	PodTemplateGeneration *int64                 `json:"podTemplateGeneration,omitempty"`
	Conditions            []metav1.Condition     `json:"conditions,omitempty"`
	ProvisioningStrategy  *string                `json:"provisioningStrategy,omitempty"`
}

func (in *capacityBuffer) DeepCopyObject() runtime.Object {
	out := *in
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Status.Conditions = append([]metav1.Condition(nil), in.Status.Conditions...)
	return &out
}

type pointerStatusObject struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Status            *struct {
		Conditions []metav1.Condition `json:"conditions"`
	} `json:"status,omitempty"`
}

func (in *pointerStatusObject) DeepCopyObject() runtime.Object { panic("unused") }

type marshalingStatus struct {
	Conditions []metav1.Condition `json:"conditions"`
}

func (marshalingStatus) MarshalJSON() ([]byte, error) { return []byte(`{}`), nil }

type inlineStatus struct {
	Status struct {
		Conditions []metav1.Condition `json:"conditions"`
	} `json:"status"`
}

func TestFindConditionsField(t *testing.T) {
	for _, tc := range []struct {
		name    string
		obj     any
		partial bool
	}{
		{"Node", &corev1.Node{}, true},
		{"CapacityBuffer", &capacityBuffer{}, true},
		{"pointer status", &pointerStatusObject{}, true},
		{"no status", &corev1.ConfigMap{}, false},
		{"unstructured", &unstructured.Unstructured{}, false},
		{"status with custom JSON", &struct {
			metav1.TypeMeta   `json:",inline"`
			metav1.ObjectMeta `json:"metadata,omitempty"`
			Status            marshalingStatus `json:"status"`
		}{}, false},
		{"inlined status", &struct {
			metav1.TypeMeta   `json:",inline"`
			metav1.ObjectMeta `json:"metadata,omitempty"`
			inlineStatus      `json:",inline"`
		}{}, false},
		{"conditions not a slice", &struct {
			metav1.TypeMeta   `json:",inline"`
			metav1.ObjectMeta `json:"metadata,omitempty"`
			Status            struct {
				Conditions map[string]string `json:"conditions"`
			} `json:"status"`
		}{}, false},
	} {
		if got := findConditionsField(reflect.TypeOf(tc.obj)) != nil; got != tc.partial {
			t.Errorf("%s: partial conversion = %v, want %v", tc.name, got, tc.partial)
		}
	}
}

// TestPartialConversion asserts that every value the controller reads from the partially converted object is the
// same as from the fully converted one
func TestPartialConversion(t *testing.T) {
	ltt := metav1.NewTime(time.Now().Add(-time.Hour).Add(123456789 * time.Nanosecond))
	deletion := metav1.NewTime(time.Now().Add(-time.Minute).Add(987654321 * time.Nanosecond))
	metas := []metav1.ObjectMeta{
		{Name: "empty"},
		{Name: "full", Namespace: "ns", UID: "uid", ResourceVersion: "42", Generation: 7, DeletionTimestamp: &deletion,
			Finalizers: []string{"a", "b"}, Labels: map[string]string{"karpenter.sh/nodepool": "np", "other": "x", "karpenter.sh/initialized": ""},
			Annotations: map[string]string{"a": "b"}, CreationTimestamp: ltt},
		{Name: "empty collections", Finalizers: []string{}, Labels: map[string]string{}},
		{Name: "unrelated labels", Labels: map[string]string{"other": "x"}},
	}
	nodeConditions := [][]corev1.NodeCondition{
		nil,
		{},
		{{Type: corev1.NodeReady, Status: corev1.ConditionTrue, Reason: "KubeletReady", Message: "ok", LastTransitionTime: ltt, LastHeartbeatTime: ltt}, {Type: "Custom", Status: corev1.ConditionUnknown}},
	}
	bufferConditions := [][]metav1.Condition{
		nil,
		{},
		{{Type: "ReadyForProvisioning", Status: metav1.ConditionTrue, Reason: "Ready", Message: "ok", LastTransitionTime: ltt, ObservedGeneration: 3}, {Type: "Provisioning", Status: metav1.ConditionFalse}},
	}
	labels := WithLabels("karpenter.sh/nodepool", "karpenter.k8s.aws/ec2nodeclass")
	gaugeLabels := WithGaugeLabels("karpenter.sh/initialized")
	for _, meta := range metas {
		for _, conditions := range nodeConditions {
			expectPartialConversion(t, &corev1.Node{ObjectMeta: meta, Status: corev1.NodeStatus{Conditions: conditions, NodeInfo: corev1.NodeSystemInfo{KubeletVersion: "v1"}}}, labels, gaugeLabels)
		}
		for _, conditions := range bufferConditions {
			expectPartialConversion(t, &capacityBuffer{ObjectMeta: meta, Status: capacityBufferStatus{Conditions: conditions, Replicas: lo.ToPtr[int32](3)}})
			expectPartialConversion(t, &capacityBuffer{ObjectMeta: meta, Status: capacityBufferStatus{Conditions: conditions}}, labels)
			expectPartialConversion(t, &pointerStatusObject{ObjectMeta: meta, Status: &struct {
				Conditions []metav1.Condition `json:"conditions"`
			}{Conditions: conditions}}, gaugeLabels)
		}
		expectPartialConversion(t, &pointerStatusObject{ObjectMeta: meta}, labels)
	}
}

func expectPartialConversion[T client.Object](t *testing.T, obj T, opts ...func(*Option)) {
	t.Helper()
	options := Option{}
	for _, opt := range opts {
		opt(&options)
	}
	c := &GenericObjectController[T]{
		Controller:      &Controller[*UnstructuredAdapter[T]]{additionalMetricLabels: options.MetricLabels, additionalGaugeMetricLabels: options.GaugeMetricLabels},
		conditionsField: findConditionsField(reflect.TypeOf(obj)),
		labelKeys:       append(options.MetricLabels, options.GaugeMetricLabels...),
	}
	if c.conditionsField == nil {
		t.Fatalf("%T: expected a partial conversion", obj)
	}
	content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		t.Fatal(err)
	}
	full := &UnstructuredAdapter[T]{Unstructured: unstructured.Unstructured{Object: content}}
	u, err := c.toUnstructured(obj)
	if err != nil {
		t.Fatal(err)
	}
	partial := &UnstructuredAdapter[T]{Unstructured: *u}

	for name, get := range map[string]func(*UnstructuredAdapter[T]) any{
		"namespace":         func(u *UnstructuredAdapter[T]) any { return u.GetNamespace() },
		"name":              func(u *UnstructuredAdapter[T]) any { return u.GetName() },
		"uid":               func(u *UnstructuredAdapter[T]) any { return u.GetUID() },
		"resourceVersion":   func(u *UnstructuredAdapter[T]) any { return u.GetResourceVersion() },
		"generation":        func(u *UnstructuredAdapter[T]) any { return u.GetGeneration() },
		"deletionTimestamp": func(u *UnstructuredAdapter[T]) any { return u.GetDeletionTimestamp() },
		"conditions":        func(u *UnstructuredAdapter[T]) any { return u.GetConditions() },
		"statusConditions":  func(u *UnstructuredAdapter[T]) any { return u.StatusConditions().List() },
		"metricLabels":      func(u *UnstructuredAdapter[T]) any { return c.toAdditionalMetricLabels(u) },
		"gaugeMetricLabels": func(u *UnstructuredAdapter[T]) any {
			return c.toAdditionalGaugeMetricLabels(u, c.toAdditionalMetricLabels(u))
		},
	} {
		if want, got := get(full), get(partial); !reflect.DeepEqual(want, got) {
			t.Errorf("%T %s: %s = %#v, want %#v", obj, obj.GetName(), name, got, want)
		}
	}
	if want, got := full.GetFinalizers(), partial.GetFinalizers(); len(want) != len(got) || (len(want) > 0 && !reflect.DeepEqual(want, got)) {
		t.Errorf("%T %s: finalizers = %#v, want %#v", obj, obj.GetName(), got, want)
	}
}
