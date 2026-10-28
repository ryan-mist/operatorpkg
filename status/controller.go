package status

import (
	"context"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"sync"
	"time"

	opunstructured "github.com/awslabs/operatorpkg/unstructured"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	pmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/awslabs/operatorpkg/object"
	"github.com/awslabs/operatorpkg/option"
	"github.com/samber/lo"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/tools/record"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type Controller[T Object] struct {
	gvk                           schema.GroupVersionKind
	additionalMetricLabels        []string
	additionalGaugeMetricLabels   []string
	additionalMetricFields        map[string]string
	additionalGaugeMetricFields   map[string]string
	kubeClient                    client.Client
	eventRecorder                 record.EventRecorder
	observedConditions            sync.Map // map[reconcile.Request]ConditionSet
	observedGaugeLabels           sync.Map // map[reconcile.Request]map[string]string
	observedFinalizers            sync.Map // map[reconcile.Request]Finalizer
	terminatingObjects            sync.Map // map[reconcile.Request]Object
	terminationGaugeLabels        sync.Map // map[reconcile.Request][]map[string]string
	maxConcurrentReconciles       int
	ConditionDuration             pmetrics.ObservationMetric
	ConditionCount                pmetrics.GaugeMetric
	ConditionCurrentStatusSeconds pmetrics.GaugeMetric
	ConditionTransitionsTotal     pmetrics.CounterMetric
	TerminationCurrentTimeSeconds pmetrics.GaugeMetric
	TerminationDuration           pmetrics.ObservationMetric
}

type Option struct {
	MetricLabels            []string
	GaugeMetricLabels       []string
	MetricFields            map[string]string
	GaugeMetricFields       map[string]string
	HistogramBuckets        []float64
	MaxConcurrentReconciles int
}

func WithLabels(labels ...string) func(*Option) {
	return func(o *Option) {
		o.MetricLabels = append(o.MetricLabels, labels...)
	}
}

func WithGaugeLabels(labels ...string) func(*Option) {
	return func(o *Option) {
		o.GaugeMetricLabels = append(o.GaugeMetricLabels, labels...)
	}
}

func WithFields(fields map[string]string) func(*Option) {
	return func(o *Option) {
		o.MetricFields = lo.Assign(o.MetricFields, fields)
	}
}

func WithGaugeFields(fields map[string]string) func(*Option) {
	return func(o *Option) {
		o.GaugeMetricFields = lo.Assign(o.GaugeMetricFields, fields)
	}
}

func WithHistogramBuckets(buckets []float64) func(*Option) {
	return func(o *Option) {
		o.HistogramBuckets = buckets
	}
}

func WitMaxConcurrentReconciles(m int) func(*Option) {
	return func(o *Option) {
		o.MaxConcurrentReconciles = m
	}
}

func NewController[T Object](client client.Client, eventRecorder record.EventRecorder, opts ...option.Function[Option]) *Controller[T] {
	options := option.Resolve(opts...)
	obj := reflect.New(reflect.TypeOf(*new(T)).Elem()).Interface().(runtime.Object)
	obj.GetObjectKind().SetGroupVersionKind(obj.GetObjectKind().GroupVersionKind())
	gvk := object.GVK(obj)

	return &Controller[T]{
		gvk:                         gvk,
		additionalMetricLabels:      options.MetricLabels,
		additionalGaugeMetricLabels: options.GaugeMetricLabels,
		additionalMetricFields:      options.MetricFields,
		additionalGaugeMetricFields: options.GaugeMetricFields,
		kubeClient:                  client,
		eventRecorder:               eventRecorder,
		maxConcurrentReconciles:     lo.Ternary(options.MaxConcurrentReconciles <= 0, 10, options.MaxConcurrentReconciles),
		ConditionDuration: conditionDurationMetric(strings.ToLower(gvk.Kind), options.HistogramBuckets, lo.Map(
			append(options.MetricLabels, lo.Keys(options.MetricFields)...),
			func(k string, _ int) pmetrics.Label { return pmetrics.Label{Name: toPrometheusLabel(k)} })...),
		ConditionCount: conditionCountMetric(strings.ToLower(gvk.Kind), lo.Map(
			append(
				append(lo.Keys(options.MetricFields), lo.Keys(options.GaugeMetricFields)...),
				append(options.MetricLabels, options.GaugeMetricLabels...)...,
			), func(k string, _ int) pmetrics.Label { return pmetrics.Label{Name: toPrometheusLabel(k)} })...),
		ConditionCurrentStatusSeconds: conditionCurrentStatusSecondsMetric(strings.ToLower(gvk.Kind), lo.Map(
			append(
				append(lo.Keys(options.MetricFields), lo.Keys(options.GaugeMetricFields)...),
				append(options.MetricLabels, options.GaugeMetricLabels...)...,
			), func(k string, _ int) pmetrics.Label { return pmetrics.Label{Name: toPrometheusLabel(k)} })...),
		ConditionTransitionsTotal: conditionTransitionsTotalMetric(strings.ToLower(gvk.Kind), lo.Map(
			append(options.MetricLabels, lo.Keys(options.MetricFields)...),
			func(k string, _ int) pmetrics.Label { return pmetrics.Label{Name: toPrometheusLabel(k)} })...),
		TerminationCurrentTimeSeconds: terminationCurrentTimeSecondsMetric(strings.ToLower(gvk.Kind), lo.Map(
			append(
				append(lo.Keys(options.MetricFields), lo.Keys(options.GaugeMetricFields)...),
				append(options.MetricLabels, options.GaugeMetricLabels...)...,
			), func(k string, _ int) pmetrics.Label { return pmetrics.Label{Name: toPrometheusLabel(k)} })...),
		TerminationDuration: terminationDurationMetric(strings.ToLower(gvk.Kind), options.HistogramBuckets, lo.Map(
			append(options.MetricLabels, lo.Keys(options.MetricFields)...),
			func(k string, _ int) pmetrics.Label { return pmetrics.Label{Name: toPrometheusLabel(k)} })...),
	}
}

func (c *Controller[T]) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		For(object.New[T]()).
		WithOptions(controller.Options{MaxConcurrentReconciles: c.maxConcurrentReconciles}).
		Named(fmt.Sprintf("operatorpkg.%s.status", strings.ToLower(c.gvk.Kind))).
		Complete(c)
}

func (c *Controller[T]) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	return c.reconcile(ctx, req, object.New[T]())
}

type GenericObjectController[T client.Object] struct {
	*Controller[*UnstructuredAdapter[T]]
}

func NewGenericObjectController[T client.Object](client client.Client, eventRecorder record.EventRecorder, opts ...option.Function[Option]) *GenericObjectController[T] {
	return &GenericObjectController[T]{
		Controller: NewController[*UnstructuredAdapter[T]](client, eventRecorder, opts...),
	}
}

func (c *GenericObjectController[T]) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		For(object.New[T]()).
		WithOptions(controller.Options{MaxConcurrentReconciles: c.maxConcurrentReconciles}).
		Named(fmt.Sprintf("operatorpkg.%s.status", strings.ToLower(reflect.TypeOf(object.New[T]()).Elem().Name()))).
		Complete(c)
}

func (c *GenericObjectController[T]) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	// Use a typed Get so controller-runtime uses its cache instead of bypassing it.
	// UnstructuredAdapter implements runtime.Unstructured (via embedded unstructured.Unstructured),
	// which causes controller-runtime to skip the cache and hit the API server directly when used
	// with kubeClient.Get.
	typedObj := object.New[T]()
	if err := c.kubeClient.Get(ctx, req.NamespacedName, typedObj); err != nil {
		if errors.IsNotFound(err) {
			return reconcile.Result{}, c.cleanupNotFound(req, NewUnstructuredAdapter[T](typedObj))
		}
		return reconcile.Result{}, fmt.Errorf("getting object, %w", err)
	}
	// Convert to proper unstructured so conditions appear as []interface{} (JSON-compatible form)
	// rather than the typed []metav1.Condition that ToPartialUnstructured would produce from a
	// typed struct. UnstructuredAdapter.GetConditions relies on unstructured.NestedFieldNoCopy
	// which requires []interface{}.
	unstructuredContent, err := runtime.DefaultUnstructuredConverter.ToUnstructured(typedObj)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("converting object to unstructured, %w", err)
	}
	unstructuredObj := &unstructured.Unstructured{Object: unstructuredContent}
	unstructuredObj.SetGroupVersionKind(object.GVK(typedObj))
	return c.reconcileFound(ctx, req, &UnstructuredAdapter[T]{Unstructured: *unstructuredObj})
}

func (c *Controller[T]) toAdditionalMetricLabels(obj Object) map[string]string {
	u := opunstructured.ToPartialUnstructured(obj, lo.Values(c.additionalMetricFields)...)
	return lo.Assign(
		lo.MapEntries(c.additionalMetricFields, func(k string, v string) (string, string) {
			elem, _, _ := unstructured.NestedString(u, lo.Filter(strings.Split(v, "."), func(s string, _ int) bool { return s != "" })...)
			return toPrometheusLabel(k), elem
		}),
		lo.SliceToMap(c.additionalMetricLabels, func(label string) (string, string) { return toPrometheusLabel(label), obj.GetLabels()[label] }),
	)
}

func (c *Controller[T]) toAdditionalGaugeMetricLabels(obj Object) map[string]string {
	u := opunstructured.ToPartialUnstructured(obj, lo.Values(c.additionalGaugeMetricFields)...)
	return lo.Assign(
		lo.MapEntries(c.additionalGaugeMetricFields, func(k string, v string) (string, string) {
			elem, _, _ := unstructured.NestedString(u, lo.Filter(strings.Split(v, "."), func(s string, _ int) bool { return s != "" })...)
			return toPrometheusLabel(k), elem
		}),
		c.toAdditionalMetricLabels(obj), lo.SliceToMap(c.additionalGaugeMetricLabels, func(label string) (string, string) { return toPrometheusLabel(label), obj.GetLabels()[label] }),
	)
}

func toPrometheusLabel(k string) string {
	unsupportedChars := []string{"/", ".", "-"}
	for _, char := range unsupportedChars {
		k = strings.ReplaceAll(k, char, "_")
	}
	return k
}

func (c *Controller[T]) reconcile(ctx context.Context, req reconcile.Request, o Object) (reconcile.Result, error) {
	if err := c.kubeClient.Get(ctx, req.NamespacedName, o); err != nil {
		if errors.IsNotFound(err) {
			return reconcile.Result{}, c.cleanupNotFound(req, o)
		}
		return reconcile.Result{}, fmt.Errorf("getting object, %w", err)
	}
	return c.reconcileFound(ctx, req, o)
}

// cleanupNotFound deletes every gauge series emitted for the object. Each series is deleted by its exact labels, which
// are reconstructed from what the controller observed, rather than with DeletePartialMatch, which scans every series of
// the vec under its write lock. The series of an object are only emitted by reconcileFound, which records them, so an
// object without a record has no series.
func (c *Controller[T]) cleanupNotFound(req reconcile.Request, o Object) error {
	observedConditions, _ := c.observedConditions.LoadAndDelete(req)
	observedGaugeLabels, _ := c.observedGaugeLabels.LoadAndDelete(req)
	if observedConditions != nil {
		additionalLabels, _ := observedGaugeLabels.(map[string]string)
		for _, condition := range observedConditions.(ConditionSet).List() {
			c.deleteConditionGaugeMetrics(req, condition, additionalLabels)
		}
	}
	if labelSets, ok := c.terminationGaugeLabels.LoadAndDelete(req); ok {
		for _, additionalLabels := range labelSets.([]map[string]string) {
			c.TerminationCurrentTimeSeconds.Delete(lo.Assign(map[string]string{
				MetricLabelNamespace: req.Namespace,
				MetricLabelName:      req.Name,
			}, additionalLabels))
		}
	}
	if obj, ok := c.terminatingObjects.LoadAndDelete(req); ok {
		c.TerminationDuration.Observe(time.Since(obj.(Object).GetDeletionTimestamp().Time).Seconds(), c.toAdditionalMetricLabels(obj.(Object)))
	}
	if finalizers, ok := c.observedFinalizers.LoadAndDelete(req); ok {
		for _, finalizer := range finalizers.([]string) {
			c.eventRecorder.Event(o, v1.EventTypeNormal, "Finalized", fmt.Sprintf("Finalized %s", finalizer))
		}
	}
	return nil
}

func (c *Controller[T]) reconcileFound(ctx context.Context, req reconcile.Request, o Object) (reconcile.Result, error) {
	// Detect and record terminations
	observedFinalizers, _ := c.observedFinalizers.Swap(req, o.GetFinalizers())
	if observedFinalizers != nil {
		for _, finalizer := range lo.Without(observedFinalizers.([]string), o.GetFinalizers()...) {
			c.eventRecorder.Event(o, v1.EventTypeNormal, "Finalized", fmt.Sprintf("Finalized %s", finalizer))
		}
	}

	if o.GetDeletionTimestamp() != nil {
		additionalGaugeLabels := c.toAdditionalGaugeMetricLabels(o)
		c.TerminationCurrentTimeSeconds.Set(time.Since(o.GetDeletionTimestamp().Time).Seconds(), lo.Assign(map[string]string{
			MetricLabelNamespace: req.Namespace,
			MetricLabelName:      req.Name,
		}, additionalGaugeLabels))
		c.terminatingObjects.Store(req, o)
		// The termination gauge is only deleted once the object is gone, so remember every label set it was emitted with
		stored, _ := c.terminationGaugeLabels.Load(req)
		labelSets, _ := stored.([]map[string]string)
		if !lo.ContainsBy(labelSets, func(l map[string]string) bool { return maps.Equal(l, additionalGaugeLabels) }) {
			c.terminationGaugeLabels.Store(req, append(labelSets, additionalGaugeLabels))
		}
	}

	// Detect and record condition counts
	currentConditions := o.StatusConditions()
	observedConditions := ConditionSet{}
	if v, ok := c.observedConditions.Load(req); ok {
		observedConditions = v.(ConditionSet)
	}
	observedGaugeLabels := map[string]string{}
	if v, ok := c.observedGaugeLabels.Load(req); ok {
		observedGaugeLabels = v.(map[string]string)
	}
	c.observedConditions.Store(req, currentConditions)
	c.observedGaugeLabels.Store(req, c.toAdditionalGaugeMetricLabels(o))

	for _, condition := range o.GetConditions() {
		c.ConditionCount.Set(1, lo.Assign(map[string]string{
			MetricLabelNamespace:       req.Namespace,
			MetricLabelName:            req.Name,
			pmetrics.LabelType:         condition.Type,
			MetricLabelConditionStatus: string(condition.Status),
			pmetrics.LabelReason:       condition.Reason,
		}, c.toAdditionalGaugeMetricLabels(o)))
		c.ConditionCurrentStatusSeconds.Set(time.Since(condition.LastTransitionTime.Time).Seconds(), lo.Assign(map[string]string{
			MetricLabelNamespace:       req.Namespace,
			MetricLabelName:            req.Name,
			pmetrics.LabelType:         condition.Type,
			MetricLabelConditionStatus: string(condition.Status),
			pmetrics.LabelReason:       condition.Reason,
		}, c.toAdditionalGaugeMetricLabels(o)))
	}

	for _, observedCondition := range observedConditions.List() {
		if currentCondition := currentConditions.Get(observedCondition.Type); currentCondition == nil || currentCondition.Status != observedCondition.Status || currentCondition.Reason != observedCondition.Reason || !maps.Equal(c.toAdditionalGaugeMetricLabels(o), observedGaugeLabels) {
			// The previous reconcile emitted the observed condition with the observed additional labels, so delete exactly that
			// series. This is also what a partial match deleted, as every other series of the object carries the current
			// conditions or the current additional labels.
			c.deleteConditionGaugeMetrics(req, observedCondition, observedGaugeLabels)
		}
	}

	// Detect and record status transitions. This approach is best effort,
	// since we may batch multiple writes within a single reconcile loop.
	// It's exceedingly difficult to atomically track all changes to an
	// object, since the Kubernetes is evenutally consistent by design.
	// Despite this, we can catch the majority of transition by remembering
	// what we saw last, and reporting observed changes.
	//
	// We rejected the alternative of tracking these changes within the
	// condition library itself, since you cannot guarantee that a
	// transition made in memory was successfully persisted.
	//
	// Automatic monitoring systems must assume that these observations are
	// lossy, specifically for when a condition transition rapidly. However,
	// for the common case, we want to alert when a transition took a long
	// time, and our likelyhood of observing this is much higher.
	for _, condition := range currentConditions.List() {
		observedCondition := observedConditions.Get(condition.Type)
		if observedCondition.GetStatus() == condition.GetStatus() {
			continue
		}
		// A condition transitions if it either didn't exist before or it has changed
		c.ConditionTransitionsTotal.Inc(lo.Assign(map[string]string{
			pmetrics.LabelType:         condition.Type,
			MetricLabelConditionStatus: string(condition.Status),
			pmetrics.LabelReason:       condition.Reason,
		}, c.toAdditionalMetricLabels(o)))
		if observedCondition == nil {
			continue
		}
		duration := condition.LastTransitionTime.Time.Sub(observedCondition.LastTransitionTime.Time).Seconds()
		c.ConditionDuration.Observe(duration, lo.Assign(map[string]string{
			pmetrics.LabelType:           observedCondition.Type,
			MetricLabelConditionStatus:   string(observedCondition.Status),
			MetricLabelToConditionStatus: string(condition.Status),
		}, c.toAdditionalMetricLabels(o)))
		c.eventRecorder.Event(o, v1.EventTypeNormal, condition.Type, fmt.Sprintf("Status condition transitioned, Type: %s, Status: %s -> %s, Reason: %s%s",
			condition.Type,
			observedCondition.Status,
			condition.Status,
			condition.Reason,
			lo.Ternary(condition.Message != "", fmt.Sprintf(", Message: %s", condition.Message), ""),
		))
	}
	return reconcile.Result{RequeueAfter: time.Second * 10}, nil
}

func (c *Controller[T]) deleteConditionGaugeMetrics(req reconcile.Request, condition Condition, additionalLabels map[string]string) {
	labels := lo.Assign(map[string]string{
		MetricLabelNamespace:       req.Namespace,
		MetricLabelName:            req.Name,
		pmetrics.LabelType:         condition.Type,
		MetricLabelConditionStatus: string(condition.Status),
		pmetrics.LabelReason:       condition.Reason,
	}, additionalLabels)
	c.ConditionCount.Delete(labels)
	c.ConditionCurrentStatusSeconds.Delete(labels)
}
