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
	"maps"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	pmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/awslabs/operatorpkg/object"
	"github.com/awslabs/operatorpkg/option"
	"github.com/awslabs/operatorpkg/status"
	"github.com/awslabs/operatorpkg/test"
	opunstructured "github.com/awslabs/operatorpkg/unstructured"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/tools/reference"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// The status controller is optimized without changing what it emits. These tests drive the controller and a frozen
// copy of the original implementation (refController, from awslabs/operatorpkg#225 e9406a4) through the same sequence
// of object updates and assert that, after every reconcile, both produced the same gathered metrics (names, labels and
// values; time-based values within a small tolerance) and the same events.

// refController is a copy of status.Controller's reconcile logic as of e9406a4. It reuses the real controller's
// metric definitions and the UnstructuredAdapter, which these optimizations don't change. Do not "fix" it.
type refController[T status.Object] struct {
	*status.Controller[T] // metrics only
	options               status.Option
	kubeClient            client.Client
	eventRecorder         record.EventRecorder
	observedConditions    sync.Map // map[reconcile.Request]ConditionSet
	observedGaugeLabels   sync.Map // map[reconcile.Request]map[string]string
	observedFinalizers    sync.Map // map[reconcile.Request]Finalizer
	terminatingObjects    sync.Map // map[reconcile.Request]Object
}

func newRefController[T status.Object](kubeClient client.Client, eventRecorder record.EventRecorder, registry *prometheus.Registry, opts ...option.Function[status.Option]) *refController[T] {
	return &refController[T]{
		Controller:    withRegistry(registry, func() *status.Controller[T] { return status.NewController[T](kubeClient, eventRecorder, opts...) }),
		options:       *option.Resolve(opts...),
		kubeClient:    kubeClient,
		eventRecorder: eventRecorder,
	}
}

func (c *refController[T]) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	o := object.New[T]()
	if err := c.kubeClient.Get(ctx, req.NamespacedName, o); err != nil {
		if errors.IsNotFound(err) {
			return reconcile.Result{}, c.cleanupNotFound(req, o)
		}
		return reconcile.Result{}, fmt.Errorf("getting object, %w", err)
	}
	return c.reconcileFound(req, o)
}

type refGenericObjectController[T client.Object] struct {
	*refController[*status.UnstructuredAdapter[T]]
}

func (c *refGenericObjectController[T]) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	typedObj := object.New[T]()
	if err := c.kubeClient.Get(ctx, req.NamespacedName, typedObj); err != nil {
		if errors.IsNotFound(err) {
			return reconcile.Result{}, c.cleanupNotFound(req, status.NewUnstructuredAdapter[T](typedObj))
		}
		return reconcile.Result{}, fmt.Errorf("getting object, %w", err)
	}
	unstructuredContent, err := runtime.DefaultUnstructuredConverter.ToUnstructured(typedObj)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("converting object to unstructured, %w", err)
	}
	unstructuredObj := &unstructured.Unstructured{Object: unstructuredContent}
	unstructuredObj.SetGroupVersionKind(object.GVK(typedObj))
	return c.reconcileFound(req, &status.UnstructuredAdapter[T]{Unstructured: *unstructuredObj})
}

func (c *refController[T]) toAdditionalMetricLabels(obj status.Object) map[string]string {
	u := opunstructured.ToPartialUnstructured(obj, lo.Values(c.options.MetricFields)...)
	return lo.Assign(
		lo.MapEntries(c.options.MetricFields, func(k string, v string) (string, string) {
			elem, _, _ := unstructured.NestedString(u, lo.Filter(strings.Split(v, "."), func(s string, _ int) bool { return s != "" })...)
			return refToPrometheusLabel(k), elem
		}),
		lo.SliceToMap(c.options.MetricLabels, func(label string) (string, string) { return refToPrometheusLabel(label), obj.GetLabels()[label] }),
	)
}

func (c *refController[T]) toAdditionalGaugeMetricLabels(obj status.Object) map[string]string {
	u := opunstructured.ToPartialUnstructured(obj, lo.Values(c.options.GaugeMetricFields)...)
	return lo.Assign(
		lo.MapEntries(c.options.GaugeMetricFields, func(k string, v string) (string, string) {
			elem, _, _ := unstructured.NestedString(u, lo.Filter(strings.Split(v, "."), func(s string, _ int) bool { return s != "" })...)
			return refToPrometheusLabel(k), elem
		}),
		c.toAdditionalMetricLabels(obj), lo.SliceToMap(c.options.GaugeMetricLabels, func(label string) (string, string) { return refToPrometheusLabel(label), obj.GetLabels()[label] }),
	)
}

func refToPrometheusLabel(k string) string {
	for _, char := range []string{"/", ".", "-"} {
		k = strings.ReplaceAll(k, char, "_")
	}
	return k
}

func (c *refController[T]) cleanupNotFound(req reconcile.Request, o status.Object) error {
	c.observedConditions.Delete(req)
	c.observedGaugeLabels.Delete(req)
	for _, m := range []pmetrics.GaugeMetric{c.ConditionCount, c.ConditionCurrentStatusSeconds, c.TerminationCurrentTimeSeconds} {
		m.DeletePartialMatch(map[string]string{status.MetricLabelNamespace: req.Namespace, status.MetricLabelName: req.Name})
	}
	if obj, ok := c.terminatingObjects.LoadAndDelete(req); ok {
		c.TerminationDuration.Observe(time.Since(obj.(status.Object).GetDeletionTimestamp().Time).Seconds(), c.toAdditionalMetricLabels(obj.(status.Object)))
	}
	if finalizers, ok := c.observedFinalizers.LoadAndDelete(req); ok {
		for _, finalizer := range finalizers.([]string) {
			c.eventRecorder.Event(o, corev1.EventTypeNormal, "Finalized", fmt.Sprintf("Finalized %s", finalizer))
		}
	}
	return nil
}

func (c *refController[T]) reconcileFound(req reconcile.Request, o status.Object) (reconcile.Result, error) {
	observedFinalizers, _ := c.observedFinalizers.Swap(req, o.GetFinalizers())
	if observedFinalizers != nil {
		for _, finalizer := range lo.Without(observedFinalizers.([]string), o.GetFinalizers()...) {
			c.eventRecorder.Event(o, corev1.EventTypeNormal, "Finalized", fmt.Sprintf("Finalized %s", finalizer))
		}
	}
	if o.GetDeletionTimestamp() != nil {
		c.TerminationCurrentTimeSeconds.Set(time.Since(o.GetDeletionTimestamp().Time).Seconds(), lo.Assign(map[string]string{
			status.MetricLabelNamespace: req.Namespace,
			status.MetricLabelName:      req.Name,
		}, c.toAdditionalGaugeMetricLabels(o)))
		c.terminatingObjects.Store(req, o)
	}

	currentConditions := o.StatusConditions()
	observedConditions := status.ConditionSet{}
	if v, ok := c.observedConditions.Load(req); ok {
		observedConditions = v.(status.ConditionSet)
	}
	observedGaugeLabels := map[string]string{}
	if v, ok := c.observedGaugeLabels.Load(req); ok {
		observedGaugeLabels = v.(map[string]string)
	}
	c.observedConditions.Store(req, currentConditions)
	c.observedGaugeLabels.Store(req, c.toAdditionalGaugeMetricLabels(o))

	conditionLabels := func(condition status.Condition) map[string]string {
		return map[string]string{
			status.MetricLabelNamespace:       req.Namespace,
			status.MetricLabelName:            req.Name,
			pmetrics.LabelType:                condition.Type,
			status.MetricLabelConditionStatus: string(condition.Status),
			pmetrics.LabelReason:              condition.Reason,
		}
	}
	for _, condition := range o.GetConditions() {
		c.ConditionCount.Set(1, lo.Assign(conditionLabels(condition), c.toAdditionalGaugeMetricLabels(o)))
		c.ConditionCurrentStatusSeconds.Set(time.Since(condition.LastTransitionTime.Time).Seconds(), lo.Assign(conditionLabels(condition), c.toAdditionalGaugeMetricLabels(o)))
	}
	for _, observedCondition := range observedConditions.List() {
		if currentCondition := currentConditions.Get(observedCondition.Type); currentCondition == nil || currentCondition.Status != observedCondition.Status || currentCondition.Reason != observedCondition.Reason || !maps.Equal(c.toAdditionalGaugeMetricLabels(o), observedGaugeLabels) {
			c.ConditionCount.DeletePartialMatch(lo.Assign(conditionLabels(observedCondition), lo.Ternary(!maps.Equal(c.toAdditionalGaugeMetricLabels(o), observedGaugeLabels), observedGaugeLabels, nil)))
			c.ConditionCurrentStatusSeconds.DeletePartialMatch(lo.Assign(conditionLabels(observedCondition), lo.Ternary(!maps.Equal(c.toAdditionalGaugeMetricLabels(o), observedGaugeLabels), observedGaugeLabels, nil)))
		}
	}
	for _, condition := range currentConditions.List() {
		observedCondition := observedConditions.Get(condition.Type)
		if observedCondition.GetStatus() == condition.GetStatus() {
			continue
		}
		c.ConditionTransitionsTotal.Inc(lo.Assign(map[string]string{
			pmetrics.LabelType:                condition.Type,
			status.MetricLabelConditionStatus: string(condition.Status),
			pmetrics.LabelReason:              condition.Reason,
		}, c.toAdditionalMetricLabels(o)))
		if observedCondition == nil {
			continue
		}
		c.ConditionDuration.Observe(condition.LastTransitionTime.Time.Sub(observedCondition.LastTransitionTime.Time).Seconds(), lo.Assign(map[string]string{
			pmetrics.LabelType:                  observedCondition.Type,
			status.MetricLabelConditionStatus:   string(observedCondition.Status),
			status.MetricLabelToConditionStatus: string(condition.Status),
		}, c.toAdditionalMetricLabels(o)))
		c.eventRecorder.Event(o, corev1.EventTypeNormal, condition.Type, fmt.Sprintf("Status condition transitioned, Type: %s, Status: %s -> %s, Reason: %s%s",
			condition.Type, observedCondition.Status, condition.Status, condition.Reason,
			lo.Ternary(condition.Message != "", fmt.Sprintf(", Message: %s", condition.Message), ""),
		))
	}
	return reconcile.Result{RequeueAfter: time.Second * 10}, nil
}

// eventCapture records events with the object reference a real recorder would derive from the object
type eventCapture struct {
	events []string
}

func (e *eventCapture) Event(obj runtime.Object, eventtype, reason, message string) {
	ref, err := reference.GetReference(scheme.Scheme, obj)
	Expect(err).ToNot(HaveOccurred())
	e.events = append(e.events, fmt.Sprintf("%s %s %s/%s uid=%s rv=%s: %s %s %s", ref.APIVersion, ref.Kind, ref.Namespace, ref.Name, ref.UID, ref.ResourceVersion, eventtype, reason, message))
}

func (e *eventCapture) Eventf(obj runtime.Object, eventtype, reason, messageFmt string, args ...interface{}) {
	e.Event(obj, eventtype, reason, fmt.Sprintf(messageFmt, args...))
}

func (e *eventCapture) AnnotatedEventf(obj runtime.Object, _ map[string]string, eventtype, reason, messageFmt string, args ...interface{}) {
	e.Event(obj, eventtype, reason, fmt.Sprintf(messageFmt, args...))
}

// flatten turns gathered metrics into "name{sorted labels}[/bucket|sum|count]" -> value
func flatten(g prometheus.Gatherer) map[string]float64 {
	families, err := g.Gather()
	Expect(err).ToNot(HaveOccurred())
	out := map[string]float64{}
	for _, family := range families {
		for _, m := range family.GetMetric() {
			pairs := lo.Map(m.GetLabel(), func(l *dto.LabelPair, _ int) string { return l.GetName() + "=" + l.GetValue() })
			sort.Strings(pairs)
			key := fmt.Sprintf("%s{%s}", family.GetName(), strings.Join(pairs, ","))
			switch {
			case m.GetGauge() != nil:
				out[key] = m.GetGauge().GetValue()
			case m.GetCounter() != nil:
				out[key] = m.GetCounter().GetValue()
			case m.GetHistogram() != nil:
				out[key+"/count"] = float64(m.GetHistogram().GetSampleCount())
				out[key+"/sum"] = m.GetHistogram().GetSampleSum()
				for _, b := range m.GetHistogram().GetBucket() {
					out[fmt.Sprintf("%s/le=%g", key, b.GetUpperBound())] = float64(b.GetCumulativeCount())
				}
			default:
				Fail("unexpected metric type in " + family.GetName())
			}
		}
	}
	return out
}

// cond is a flavor-independent condition description
type cond struct {
	Type, Status, Reason, Message string
	LTT                           time.Time
}

// flavor adapts the test sequence to an object type and controller configuration
type flavor struct {
	newObject     func(name string) client.Object
	setConditions func(client.Object, []cond)
	// newControllers returns the reference and the real controller, registering their metrics in the given registries
	newControllers func(kubeClient client.Client, refRecorder, recorder record.EventRecorder, refRegistry, registry *prometheus.Registry) (reconcile.Reconciler, reconcile.Reconciler)
}

func typedFlavor(opts ...option.Function[status.Option]) flavor {
	return flavor{
		newObject: func(name string) client.Object {
			return &test.CustomObject{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}, Spec: test.CustomSpec{Field1: "field-" + name}}
		},
		setConditions: func(o client.Object, conds []cond) {
			o.(*test.CustomObject).Status.Conditions = lo.Map(conds, func(c cond, _ int) status.Condition {
				return status.Condition{Type: c.Type, Status: metav1.ConditionStatus(c.Status), Reason: c.Reason, Message: c.Message, LastTransitionTime: metav1.NewTime(c.LTT), ObservedGeneration: 1}
			})
		},
		newControllers: func(kubeClient client.Client, refRecorder, recorder record.EventRecorder, refRegistry, registry *prometheus.Registry) (reconcile.Reconciler, reconcile.Reconciler) {
			ref := newRefController[*test.CustomObject](kubeClient, refRecorder, refRegistry, opts...)
			return ref, withRegistry(registry, func() reconcile.Reconciler {
				return status.NewController[*test.CustomObject](kubeClient, recorder, opts...)
			})
		},
	}
}

func genericFlavor[T client.Object](newObject func(name string) T, setConditions func(T, []cond), opts ...option.Function[status.Option]) flavor {
	return flavor{
		newObject:     func(name string) client.Object { return newObject(name) },
		setConditions: func(o client.Object, conds []cond) { setConditions(o.(T), conds) },
		newControllers: func(kubeClient client.Client, refRecorder, recorder record.EventRecorder, refRegistry, registry *prometheus.Registry) (reconcile.Reconciler, reconcile.Reconciler) {
			ref := &refGenericObjectController[T]{refController: newRefController[*status.UnstructuredAdapter[T]](kubeClient, refRecorder, refRegistry, opts...)}
			return ref, withRegistry(registry, func() reconcile.Reconciler {
				return status.NewGenericObjectController[T](kubeClient, recorder, opts...)
			})
		},
	}
}

func withRegistry[R any](registry *prometheus.Registry, f func() R) R {
	original := metrics.Registry
	metrics.Registry = registry
	defer func() { metrics.Registry = original }()
	return f()
}

func nodeFlavor(opts ...option.Function[status.Option]) flavor {
	return genericFlavor(func(name string) *corev1.Node {
		n := benchmarkNode(name)
		n.Status.Conditions = nil
		return n
	}, func(n *corev1.Node, conds []cond) {
		n.Status.Conditions = lo.Map(conds, func(c cond, _ int) corev1.NodeCondition {
			return corev1.NodeCondition{Type: corev1.NodeConditionType(c.Type), Status: corev1.ConditionStatus(c.Status), Reason: c.Reason, Message: c.Message, LastTransitionTime: metav1.NewTime(c.LTT), LastHeartbeatTime: metav1.Now()}
		})
	}, opts...)
}

// testGenericFlavor uses TestGenericObject, whose status matches CapacityBuffer's ([]metav1.Condition)
func testGenericFlavor(opts ...option.Function[status.Option]) flavor {
	return genericFlavor(func(name string) *TestGenericObject {
		return &TestGenericObject{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID("uid-" + name), Generation: 3}}
	}, func(o *TestGenericObject, conds []cond) {
		o.Status.Conditions = lo.Map(conds, func(c cond, _ int) metav1.Condition {
			return metav1.Condition{Type: c.Type, Status: metav1.ConditionStatus(c.Status), Reason: c.Reason, Message: c.Message, LastTransitionTime: metav1.NewTime(c.LTT), ObservedGeneration: 2}
		})
	}, opts...)
}

type equivalenceHarness struct {
	flavor
	kubeClient                *mapClient
	ref, controller           reconcile.Reconciler
	refRegistry, registry     *prometheus.Registry
	refRecorder, recorder     *eventCapture
	refEventsSeen, eventsSeen int
	objects                   map[string]client.Object
}

func newEquivalenceHarness(f flavor) *equivalenceHarness {
	h := &equivalenceHarness{
		flavor:      f,
		kubeClient:  newMapClient(),
		refRegistry: prometheus.NewRegistry(),
		registry:    prometheus.NewRegistry(),
		refRecorder: &eventCapture{},
		recorder:    &eventCapture{},
		objects:     map[string]client.Object{},
	}
	h.ref, h.controller = f.newControllers(h.kubeClient, h.refRecorder, h.recorder, h.refRegistry, h.registry)
	return h
}

// update applies a mutation of the named object (creating it if needed) to the client
func (h *equivalenceHarness) update(name string, mutate func(client.Object)) {
	o, ok := h.objects[name]
	if !ok {
		o = h.newObject(name)
		h.objects[name] = o
	}
	mutate(o)
	h.kubeClient.apply(o)
}

func (h *equivalenceHarness) delete(name string) {
	h.kubeClient.delete(client.ObjectKeyFromObject(h.objects[name]))
	delete(h.objects, name)
}

// reconcile reconciles the named objects with both controllers and expects identical results, metrics and events
func (h *equivalenceHarness) reconcile(names ...string) {
	GinkgoHelper()
	for _, name := range names {
		req := reconcile.Request{NamespacedName: types.NamespacedName{Namespace: lo.Ternary(strings.HasPrefix(name, "node"), "", "default"), Name: name}}
		refResult, refErr := h.ref.Reconcile(context.Background(), req)
		result, err := h.controller.Reconcile(context.Background(), req)
		Expect(fmt.Sprint(err)).To(Equal(fmt.Sprint(refErr)))
		Expect(result).To(Equal(refResult))
	}
	expected, actual := flatten(h.refRegistry), flatten(h.registry)
	Expect(expected).ToNot(BeEmpty())
	Expect(slices.Sorted(maps.Keys(actual))).To(Equal(slices.Sorted(maps.Keys(expected))))
	for k, v := range expected {
		// Time-based values (current_status_seconds, termination current_time_seconds and duration) are computed
		// with time.Since, microseconds apart; everything else must match exactly.
		Expect(math.Abs(actual[k]-v)).To(BeNumerically("<", 0.05), "%s: expected %v, got %v", k, v, actual[k])
	}
	Expect(append([]string{}, h.recorder.events[h.eventsSeen:]...)).To(Equal(append([]string{}, h.refRecorder.events[h.refEventsSeen:]...)))
	h.refEventsSeen, h.eventsSeen = len(h.refRecorder.events), len(h.recorder.events)
}

// runEquivalenceSequence drives a lifecycle through both controllers. Objects named "a"/"b" (or "node-a"/"node-b").
func runEquivalenceSequence(f flavor, prefix string) {
	h := newEquivalenceHarness(f)
	a, b := prefix+"a", prefix+"b"
	// Sub-second timestamps exercise the RFC3339 (second precision) round trip of the generic controller
	t0 := time.Now().Add(-time.Hour).Add(123456789 * time.Nanosecond)
	at := func(d time.Duration) time.Time { return t0.Add(d) }
	labels := func(o client.Object, kv ...string) {
		l := lo.Assign(o.GetLabels())
		for i := 0; i < len(kv); i += 2 {
			if kv[i+1] == "" {
				delete(l, kv[i])
			} else {
				l[kv[i]] = kv[i+1]
			}
		}
		o.SetLabels(l)
	}
	steady := func(names ...string) {
		GinkgoHelper()
		// Long enough that a time-based gauge which wasn't refreshed falls outside the tolerance
		time.Sleep(150 * time.Millisecond)
		h.reconcile(names...)
		h.reconcile(names...)
	}

	// Create: "a" lacks Bar, which typed objects initialize to Unknown on every read
	h.update(a, func(o client.Object) {
		labels(o, "karpenter.sh/nodepool", "np-1", "karpenter.k8s.aws/ec2nodeclass", "nc-1", "gauge-label", "g-1", "unrelated", "u-1")
		o.SetAnnotations(map[string]string{"owner": "team-1"})
		o.SetFinalizers([]string{"example.com/f1", "example.com/f2"})
		h.setConditions(o, []cond{
			{Type: status.ConditionReady, Status: "True", Reason: "Ready", LTT: at(0)},
			{Type: ConditionTypeFoo, Status: "True", Reason: "FooReady", Message: "all good", LTT: at(1500 * time.Millisecond)},
		})
	})
	h.update(b, func(o client.Object) {
		labels(o, "karpenter.sh/nodepool", "np-2")
		h.setConditions(o, []cond{
			{Type: status.ConditionReady, Status: "True", Reason: "Ready", LTT: at(0)},
			{Type: ConditionTypeFoo, Status: "True", Reason: "FooReady", LTT: at(0)},
			{Type: ConditionTypeBar, Status: "True", Reason: "BarReady", LTT: at(0)},
		})
	})
	h.reconcile(a, b)
	steady(a, b)

	// Status transition
	h.update(a, func(o client.Object) {
		h.setConditions(o, []cond{
			{Type: status.ConditionReady, Status: "True", Reason: "Ready", LTT: at(0)},
			{Type: ConditionTypeFoo, Status: "False", Reason: "Broken", Message: "it broke", LTT: at(600250 * time.Millisecond)},
		})
	})
	h.reconcile(a, b)
	steady(a)

	// Reason and message change without a status change, plus Bar and a Ready transition
	h.update(a, func(o client.Object) {
		h.setConditions(o, []cond{
			{Type: status.ConditionReady, Status: "False", Reason: "NotReady", LTT: at(900500 * time.Millisecond)},
			{Type: ConditionTypeFoo, Status: "False", Reason: "StillBroken", Message: "still broken", LTT: at(600250 * time.Millisecond)},
			{Type: ConditionTypeBar, Status: "False", Reason: "BarBroken", LTT: at(900500 * time.Millisecond)},
		})
	})
	h.reconcile(a, b)
	steady(a, b)

	// Metric label change, and an unrelated label/annotation change
	h.update(a, func(o client.Object) { labels(o, "karpenter.sh/nodepool", "np-3", "gauge-label", "g-2") })
	h.reconcile(a)
	steady(a)
	h.update(a, func(o client.Object) {
		labels(o, "unrelated", "u-2")
		o.SetAnnotations(map[string]string{"owner": "team-2"})
	})
	h.reconcile(a)
	h.update(a, func(o client.Object) {
		labels(o, "karpenter.sh/initialized", "true", "karpenter.k8s.aws/ec2nodeclass", "")
	})
	h.reconcile(a, b)
	steady(a)

	// Condition removed, then a duplicate condition type
	h.update(a, func(o client.Object) {
		h.setConditions(o, []cond{
			{Type: status.ConditionReady, Status: "False", Reason: "NotReady", LTT: at(900500 * time.Millisecond)},
			{Type: ConditionTypeBar, Status: "False", Reason: "BarBroken", LTT: at(900500 * time.Millisecond)},
		})
	})
	h.reconcile(a)
	steady(a)
	h.update(a, func(o client.Object) {
		h.setConditions(o, []cond{
			{Type: status.ConditionReady, Status: "True", Reason: "Ready", LTT: at(1000 * time.Second)},
			{Type: ConditionTypeBar, Status: "False", Reason: "BarBroken", LTT: at(900500 * time.Millisecond)},
			{Type: ConditionTypeBar, Status: "True", Reason: "BarDuplicate", LTT: at(950 * time.Second)},
			{Type: ConditionTypeBaz, Status: "Unknown", Reason: "", LTT: at(950 * time.Second)},
		})
	})
	h.reconcile(a)
	steady(a)
	h.update(a, func(o client.Object) {
		h.setConditions(o, []cond{
			{Type: status.ConditionReady, Status: "True", Reason: "Ready", LTT: at(1000 * time.Second)},
			{Type: ConditionTypeFoo, Status: "True", Reason: "FooReady", LTT: at(1000 * time.Second)},
			{Type: ConditionTypeBar, Status: "True", Reason: "BarDuplicate", LTT: at(950 * time.Second)},
		})
	})
	h.reconcile(a)
	steady(a)

	// Termination: deletion timestamp, a finalizer removed, then the object is gone
	h.update(a, func(o client.Object) {
		o.SetDeletionTimestamp(lo.ToPtr(metav1.NewTime(time.Now().Add(-30*time.Minute - 250*time.Millisecond))))
	})
	h.reconcile(a, b)
	steady(a)
	h.update(a, func(o client.Object) {
		o.SetFinalizers([]string{"example.com/f2"})
		labels(o, "karpenter.sh/nodepool", "np-4")
	})
	h.reconcile(a)
	steady(a)
	h.delete(a)
	h.reconcile(a, b)
	h.reconcile(a)

	// Recreated with the same name
	h.update(a, func(o client.Object) {
		labels(o, "karpenter.sh/nodepool", "np-1")
		h.setConditions(o, []cond{{Type: status.ConditionReady, Status: "Unknown", Reason: "Starting", LTT: at(2000 * time.Second)}})
	})
	h.reconcile(a, b)
	steady(a, b)
	h.delete(b)
	h.reconcile(a, b)
	steady(a)
}

var _ = Describe("Equivalence with the original implementation", func() {
	It("should emit the same metrics and events for typed objects", func() {
		runEquivalenceSequence(typedFlavor(status.WithLabels("karpenter.sh/nodepool", "karpenter.k8s.aws/ec2nodeclass"), status.WithGaugeLabels("gauge-label")), "")
	})
	It("should emit the same metrics and events for typed objects with fields", func() {
		runEquivalenceSequence(typedFlavor(status.WithLabels("karpenter.sh/nodepool"), status.WithFields(map[string]string{"field1": ".spec.field1"}), status.WithGaugeFields(map[string]string{"owner": ".metadata.annotations.owner"})), "")
	})
	It("should emit the same metrics and events for generic objects (CapacityBuffer-like)", func() {
		runEquivalenceSequence(testGenericFlavor(status.WithLabels("karpenter.sh/nodepool"), status.WithGaugeLabels("gauge-label")), "")
	})
	It("should emit the same metrics and events for generic objects with fields", func() {
		runEquivalenceSequence(testGenericFlavor(status.WithLabels("karpenter.sh/nodepool"), status.WithFields(map[string]string{"gen": ".metadata.generation", "owner": ".metadata.annotations.owner"}), status.WithGaugeFields(map[string]string{"uid": ".metadata.uid"})), "")
	})
	It("should emit the same metrics and events for Nodes", func() {
		runEquivalenceSequence(nodeFlavor(status.WithLabels("karpenter.k8s.aws/ec2nodeclass", "karpenter.sh/nodepool", "karpenter.sh/initialized")), "node-")
	})
	It("should emit the same metrics and events for Nodes with fields", func() {
		runEquivalenceSequence(nodeFlavor(status.WithLabels("karpenter.sh/nodepool"), status.WithGaugeFields(map[string]string{"provider_id": ".spec.providerID", "kubelet": ".status.nodeInfo.kubeletVersion"})), "node-")
	})
})
