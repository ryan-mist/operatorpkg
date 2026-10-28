package status

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"slices"
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
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/samber/lo"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	unchangedObjects              sync.Map // map[reconcile.Request]*unchangedObject
	terminationGaugeLabels        sync.Map // map[reconcile.Request][]map[string]string
	orphanedDeprecatedConditions  sync.Map // map[reconcile.Request][]Condition
	emitDeprecatedMetrics         bool
	untrackedDeprecatedSeries     bool
	maxConcurrentReconciles       int
	ConditionDuration             pmetrics.ObservationMetric
	ConditionCount                pmetrics.GaugeMetric
	ConditionCurrentStatusSeconds pmetrics.GaugeMetric
	ConditionTransitionsTotal     pmetrics.CounterMetric
	TerminationCurrentTimeSeconds pmetrics.GaugeMetric
	TerminationDuration           pmetrics.ObservationMetric
}

type Option struct {
	// Current list of deprecated metrics
	// - operator_status_condition_transitions_total
	// - operator_status_condition_transition_seconds
	// - operator_status_condition_current_status_seconds
	// - operator_status_condition_count
	// - operator_termination_current_time_seconds
	// - operator_termination_duration_seconds
	EmitDeprecatedMetrics   bool
	MetricLabels            []string
	GaugeMetricLabels       []string
	MetricFields            map[string]string
	GaugeMetricFields       map[string]string
	HistogramBuckets        []float64
	MaxConcurrentReconciles int
}

func EmitDeprecatedMetrics(o *Option) {
	o.EmitDeprecatedMetrics = true
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
	// The deprecated metrics are shared by every controller in the process. If a previous controller for this kind left
	// series behind, this controller doesn't know their labels, so it falls back to DeletePartialMatch for them.
	untrackedDeprecatedSeries := options.EmitDeprecatedMetrics && lo.SomeBy([]pmetrics.GaugeMetric{ConditionCount, ConditionCurrentStatusSeconds, TerminationCurrentTimeSeconds}, func(m pmetrics.GaugeMetric) bool {
		return hasSeriesForKind(m, gvk)
	})

	return &Controller[T]{
		gvk:                         gvk,
		additionalMetricLabels:      options.MetricLabels,
		additionalGaugeMetricLabels: options.GaugeMetricLabels,
		additionalMetricFields:      options.MetricFields,
		additionalGaugeMetricFields: options.GaugeMetricFields,
		kubeClient:                  client,
		eventRecorder:               eventRecorder,
		emitDeprecatedMetrics:       options.EmitDeprecatedMetrics,
		untrackedDeprecatedSeries:   untrackedDeprecatedSeries,
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
	// conditionsField locates .status.conditions in T for the partial conversion, nil if T must be fully converted
	conditionsField *conditionsField
	// labelKeys are the object labels the metrics read
	labelKeys []string
}

func NewGenericObjectController[T client.Object](client client.Client, eventRecorder record.EventRecorder, opts ...option.Function[Option]) *GenericObjectController[T] {
	c := &GenericObjectController[T]{
		Controller: NewController[*UnstructuredAdapter[T]](client, eventRecorder, opts...),
	}
	c.labelKeys = lo.Uniq(append(append([]string{}, c.additionalMetricLabels...), c.additionalGaugeMetricLabels...))
	// Metric fields can read any path of the object, so they need the full conversion
	if len(c.additionalMetricFields) == 0 && len(c.additionalGaugeMetricFields) == 0 {
		c.conditionsField = findConditionsField(reflect.TypeOf(object.New[T]()))
	}
	return c
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
	if c.reconcileUnchanged(req, typedObj.GetResourceVersion()) {
		return reconcile.Result{RequeueAfter: requeueInterval}, nil
	}
	unstructuredObj, err := c.toUnstructured(typedObj)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("converting object to unstructured, %w", err)
	}
	unstructuredObj.SetGroupVersionKind(object.GVK(typedObj))
	return c.reconcileFound(ctx, req, &UnstructuredAdapter[T]{Unstructured: *unstructuredObj})
}

// toUnstructured converts the fields of the object that the controller reads. The conditions must be in their
// JSON-compatible form ([]interface{} rather than the typed []metav1.Condition that ToPartialUnstructured would
// produce from a typed struct) since UnstructuredAdapter.GetConditions relies on unstructured.NestedFieldNoCopy.
// Converting the whole object gets that right but is the most expensive part of a reconcile for large objects like
// Nodes, so only the conditions are run through the converter, and only the metadata the controller reads (identity,
// finalizers, deletion timestamp and the configured labels) is copied from the typed object. Every value the
// controller reads, and the object reference used for events, is the same as with the full conversion. The smaller
// object is also what observedConditions and terminatingObjects retain.
func (c *GenericObjectController[T]) toUnstructured(obj T) (*unstructured.Unstructured, error) {
	if c.conditionsField == nil {
		content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
		if err != nil {
			return nil, err
		}
		return &unstructured.Unstructured{Object: content}, nil
	}
	u := &unstructured.Unstructured{Object: map[string]interface{}{}}
	u.SetNamespace(obj.GetNamespace())
	u.SetName(obj.GetName())
	u.SetUID(obj.GetUID())
	u.SetResourceVersion(obj.GetResourceVersion())
	u.SetGeneration(obj.GetGeneration())
	u.SetDeletionTimestamp(obj.GetDeletionTimestamp())
	if finalizers := obj.GetFinalizers(); len(finalizers) > 0 {
		u.SetFinalizers(finalizers)
	}
	if objLabels := obj.GetLabels(); len(objLabels) > 0 && len(c.labelKeys) > 0 {
		labels := map[string]string{}
		for _, k := range c.labelKeys {
			if v, ok := objLabels[k]; ok {
				labels[k] = v
			}
		}
		if len(labels) > 0 {
			u.SetLabels(labels)
		}
	}
	conditions, ok := c.conditionsField.get(reflect.ValueOf(obj))
	if !ok {
		return u, nil
	}
	converted, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&struct {
		Conditions interface{} `json:"conditions"`
	}{Conditions: conditions.Interface()})
	if err != nil {
		return nil, err
	}
	u.Object["status"] = map[string]interface{}{"conditions": converted["conditions"]}
	return u, nil
}

// conditionsField is the location of .status.conditions in a struct
type conditionsField struct {
	status, conditions         int
	statusIsPointer, omitempty bool
}

// get returns .status.conditions of v, a pointer to the struct, and false if the full conversion wouldn't have it
func (f *conditionsField) get(v reflect.Value) (reflect.Value, bool) {
	status := v.Elem().Field(f.status)
	if f.statusIsPointer {
		if status.IsNil() {
			return reflect.Value{}, false
		}
		status = status.Elem()
	}
	conditions := status.Field(f.conditions)
	if conditions.IsNil() || (f.omitempty && conditions.Len() == 0) {
		return reflect.Value{}, false
	}
	return conditions, true
}

// findConditionsField returns where .status.conditions is in t, a pointer to a struct, if the object can be
// converted partially with the same result as the full conversion. That is the case for plain structs with
// .status.conditions as a slice field: no custom unstructured or JSON conversion on the object or its status, no
// inlined fields that could hide a status, and metav1.ObjectMeta as the metadata.
func findConditionsField(t reflect.Type) *conditionsField {
	if t.Kind() != reflect.Pointer || t.Elem().Kind() != reflect.Struct || hasCustomConversion(t.Elem()) {
		return nil
	}
	t = t.Elem()
	f := &conditionsField{status: -1, conditions: -1}
	for i := range t.NumField() {
		field := t.Field(i)
		switch name, _ := jsonFieldName(field); name {
		case "":
			if field.Type != reflect.TypeOf(metav1.TypeMeta{}) {
				return nil
			}
		case "metadata":
			if field.Type != reflect.TypeOf(metav1.ObjectMeta{}) {
				return nil
			}
		case "status":
			f.status = i
		}
	}
	if f.status == -1 {
		return nil
	}
	status := t.Field(f.status).Type
	if status.Kind() == reflect.Pointer {
		f.statusIsPointer = true
		status = status.Elem()
	}
	if status.Kind() != reflect.Struct || hasCustomConversion(status) {
		return nil
	}
	for i := range status.NumField() {
		switch name, omitempty := jsonFieldName(status.Field(i)); name {
		case "":
			return nil
		case "conditions":
			f.conditions, f.omitempty = i, omitempty
		}
	}
	if f.conditions == -1 || status.Field(f.conditions).Type.Kind() != reflect.Slice {
		return nil
	}
	return f
}

// jsonFieldName returns the key runtime.DefaultUnstructuredConverter uses for a struct field ("" if inlined)
func jsonFieldName(field reflect.StructField) (string, bool) {
	tag := field.Tag.Get("json")
	items := strings.Split(tag, ",")
	name := items[0]
	if name == "" && !field.Anonymous {
		name = field.Name
	}
	return name, lo.Contains(items[1:], "omitempty")
}

// hasCustomConversion mirrors the converter's check for types that convert themselves (json.Marshaler, which
// value.UnstructuredConverter embeds)
func hasCustomConversion(t reflect.Type) bool {
	return t.Implements(jsonMarshalerType) || reflect.PointerTo(t).Implements(jsonMarshalerType)
}

var jsonMarshalerType = reflect.TypeOf((*json.Marshaler)(nil)).Elem()

func (c *Controller[T]) toAdditionalMetricLabels(obj Object) map[string]string {
	u := opunstructured.ToPartialUnstructured(obj, lo.Values(c.additionalMetricFields)...)
	// GetLabels copies the whole label map for unstructured objects, so read it once rather than once per label
	objLabels := obj.GetLabels()
	return lo.Assign(
		lo.MapEntries(c.additionalMetricFields, func(k string, v string) (string, string) {
			elem, _, _ := unstructured.NestedString(u, lo.Filter(strings.Split(v, "."), func(s string, _ int) bool { return s != "" })...)
			return toPrometheusLabel(k), elem
		}),
		lo.SliceToMap(c.additionalMetricLabels, func(label string) (string, string) { return toPrometheusLabel(label), objLabels[label] }),
	)
}

// toAdditionalGaugeMetricLabels extends additionalMetricLabels, the result of toAdditionalMetricLabels(obj), with the gauge-only labels
func (c *Controller[T]) toAdditionalGaugeMetricLabels(obj Object, additionalMetricLabels map[string]string) map[string]string {
	u := opunstructured.ToPartialUnstructured(obj, lo.Values(c.additionalGaugeMetricFields)...)
	objLabels := obj.GetLabels()
	return lo.Assign(
		lo.MapEntries(c.additionalGaugeMetricFields, func(k string, v string) (string, string) {
			elem, _, _ := unstructured.NestedString(u, lo.Filter(strings.Split(v, "."), func(s string, _ int) bool { return s != "" })...)
			return toPrometheusLabel(k), elem
		}),
		additionalMetricLabels, lo.SliceToMap(c.additionalGaugeMetricLabels, func(label string) (string, string) { return toPrometheusLabel(label), objLabels[label] }),
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
	if c.reconcileUnchanged(req, o.GetResourceVersion()) {
		return reconcile.Result{RequeueAfter: requeueInterval}, nil
	}
	return c.reconcileFound(ctx, req, o)
}

// requeueInterval refreshes the time-based gauges (current_status_seconds, termination current_time_seconds)
const requeueInterval = time.Second * 10

// unchangedObject is what the last reconcile of an object emitted that depends on time
type unchangedObject struct {
	resourceVersion   string
	deletionTimestamp *metav1.Time
	conditions        []Condition
	gaugeLabels       map[string]string
}

// reconcileUnchanged handles a reconcile of an object whose resourceVersion is the one the last reconcile saw, which is
// most reconciles since the controller requeues every object every 10s. An object with the same resourceVersion has
// the same content, so reconcileFound would emit nothing but the time-based gauges: the condition count series it
// would set already exist with the same value, no condition or label changed so nothing is deleted, no condition
// transitioned and no finalizer was removed so no histogram, counter or event is emitted, and the observed state it
// would store is equal to what is stored. This only refreshes those gauges, with the same labels and values
// reconcileFound would use. reconcileFound only records objects for which that holds (see recordUnchanged).
func (c *Controller[T]) reconcileUnchanged(req reconcile.Request, resourceVersion string) bool {
	v, ok := c.unchangedObjects.Load(req)
	if !ok || resourceVersion == "" || v.(*unchangedObject).resourceVersion != resourceVersion {
		return false
	}
	u := v.(*unchangedObject)
	if u.deletionTimestamp != nil {
		c.setGaugeMetric(c.TerminationCurrentTimeSeconds, TerminationCurrentTimeSeconds, time.Since(u.deletionTimestamp.Time).Seconds(), map[string]string{
			MetricLabelNamespace: req.Namespace,
			MetricLabelName:      req.Name,
		}, u.gaugeLabels)
	}
	for _, condition := range u.conditions {
		c.setGaugeMetric(c.ConditionCurrentStatusSeconds, ConditionCurrentStatusSeconds, time.Since(condition.LastTransitionTime.Time).Seconds(), map[string]string{
			MetricLabelNamespace:       req.Namespace,
			MetricLabelName:            req.Name,
			pmetrics.LabelType:         condition.Type,
			MetricLabelConditionStatus: string(condition.Status),
			pmetrics.LabelReason:       condition.Reason,
		}, u.gaugeLabels)
	}
	return true
}

// recordUnchanged remembers what reconcileUnchanged needs after a full reconcile, but only when a second full
// reconcile of the same resourceVersion would provably emit only the time-based gauges:
//   - StatusConditions() didn't change the conditions. Typed objects initialize missing conditions to Unknown with
//     the current time, so every reconcile would emit a fresh current_status_seconds for them.
//   - No condition type is repeated. The comparison with the observed conditions only looks at the first condition of
//     a type, so a repeated type is a "transition" (and a delete of its series) on every reconcile.
//   - The reconcile didn't delete a series it emitted, which the next full reconcile would re-create.
func (c *Controller[T]) recordUnchanged(req reconcile.Request, o Object, conditionsBefore, conditions []Condition, gaugeLabels map[string]string, deletedEmitted bool) {
	if o.GetResourceVersion() == "" || deletedEmitted || !slices.EqualFunc(conditionsBefore, conditions, conditionsEqual) || hasRepeatedType(conditions) {
		c.unchangedObjects.Delete(req)
		return
	}
	c.unchangedObjects.Store(req, &unchangedObject{
		resourceVersion:   o.GetResourceVersion(),
		deletionTimestamp: o.GetDeletionTimestamp(),
		conditions:        conditions,
		gaugeLabels:       gaugeLabels,
	})
}

func hasRepeatedType(conditions []Condition) bool {
	for i := range conditions {
		for j := range i {
			if conditions[i].Type == conditions[j].Type {
				return true
			}
		}
	}
	return false
}

// readOnlyStatusConditions is implemented by objects whose StatusConditions() never changes their conditions
type readOnlyStatusConditions interface {
	readOnlyStatusConditions()
}

func conditionsEqual(a, b Condition) bool {
	return a.Type == b.Type && a.Status == b.Status && a.Reason == b.Reason && a.Message == b.Message &&
		a.ObservedGeneration == b.ObservedGeneration && a.LastTransitionTime.Equal(&b.LastTransitionTime)
}

// cleanupNotFound deletes every gauge series emitted for the object. Each series is deleted by its exact labels, which
// are reconstructed from what the controller observed, rather than with DeletePartialMatch, which scans every series of
// the vec under its write lock. The series of an object are only emitted by reconcileFound, which records them, so an
// object without a record has no series.
func (c *Controller[T]) cleanupNotFound(req reconcile.Request, o Object) error {
	c.unchangedObjects.Delete(req)
	observedConditions, _ := c.observedConditions.LoadAndDelete(req)
	observedGaugeLabels, _ := c.observedGaugeLabels.LoadAndDelete(req)
	if observedConditions != nil {
		additionalLabels, _ := observedGaugeLabels.(map[string]string)
		for _, condition := range observedConditions.(ConditionSet).List() {
			c.deleteConditionGaugeMetrics(req, condition, additionalLabels, true)
		}
	}
	if conditions, ok := c.orphanedDeprecatedConditions.LoadAndDelete(req); ok {
		for _, condition := range conditions.([]Condition) {
			c.deleteDeprecatedConditionGaugeMetrics(req, condition)
		}
	}
	if labelSets, ok := c.terminationGaugeLabels.LoadAndDelete(req); ok {
		for _, additionalLabels := range labelSets.([]map[string]string) {
			c.deleteGaugeMetric(c.TerminationCurrentTimeSeconds, TerminationCurrentTimeSeconds, map[string]string{
				MetricLabelNamespace: req.Namespace,
				MetricLabelName:      req.Name,
			}, additionalLabels, true)
		}
	}
	if c.untrackedDeprecatedSeries {
		for _, deprecated := range []pmetrics.GaugeMetric{ConditionCount, ConditionCurrentStatusSeconds, TerminationCurrentTimeSeconds} {
			deprecated.DeletePartialMatch(map[string]string{
				MetricLabelNamespace: req.Namespace,
				MetricLabelName:      req.Name,
				pmetrics.LabelKind:   c.gvk.Kind,
				pmetrics.LabelGroup:  c.gvk.Group,
			})
		}
	}
	if obj, ok := c.terminatingObjects.LoadAndDelete(req); ok {
		c.observeHistogram(c.TerminationDuration, TerminationDuration, time.Since(obj.(Object).GetDeletionTimestamp().Time).Seconds(), map[string]string{}, c.toAdditionalMetricLabels(obj.(Object)))
	}
	if finalizers, ok := c.observedFinalizers.LoadAndDelete(req); ok {
		for _, finalizer := range finalizers.([]string) {
			c.eventRecorder.Event(o, v1.EventTypeNormal, "Finalized", fmt.Sprintf("Finalized %s", finalizer))
		}
	}
	return nil
}

func (c *Controller[T]) reconcileFound(ctx context.Context, req reconcile.Request, o Object) (reconcile.Result, error) {
	// The object can't change during the reconcile, so compute its labels once. The maps are shared by every metric call
	// below, which is safe since they are only read (setGaugeMetric etc. copy them with lo.Assign).
	additionalMetricLabels := c.toAdditionalMetricLabels(o)
	additionalGaugeMetricLabels := c.toAdditionalGaugeMetricLabels(o, additionalMetricLabels)

	// Detect and record terminations
	observedFinalizers, _ := c.observedFinalizers.Swap(req, o.GetFinalizers())
	if observedFinalizers != nil {
		for _, finalizer := range lo.Without(observedFinalizers.([]string), o.GetFinalizers()...) {
			c.eventRecorder.Event(o, v1.EventTypeNormal, "Finalized", fmt.Sprintf("Finalized %s", finalizer))
		}
	}

	if o.GetDeletionTimestamp() != nil {
		c.setGaugeMetric(c.TerminationCurrentTimeSeconds, TerminationCurrentTimeSeconds, time.Since(o.GetDeletionTimestamp().Time).Seconds(), map[string]string{
			MetricLabelNamespace: req.Namespace,
			MetricLabelName:      req.Name,
		}, additionalGaugeMetricLabels)
		c.terminatingObjects.Store(req, o)
		// The termination gauge is only deleted once the object is gone, so remember every label set it was emitted with
		storeUnique(&c.terminationGaugeLabels, req, additionalGaugeMetricLabels, maps.Equal)
	}

	// Detect and record condition counts
	// UnstructuredAdapter.StatusConditions only reflects the conditions, other objects may initialize missing ones
	var conditionsBefore []Condition
	_, readOnlyConditions := o.(readOnlyStatusConditions)
	if !readOnlyConditions {
		conditionsBefore = slices.Clone(o.GetConditions())
	}
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
	c.observedGaugeLabels.Store(req, additionalGaugeMetricLabels)
	gaugeLabelsChanged := !maps.Equal(additionalGaugeMetricLabels, observedGaugeLabels)

	conditions := o.GetConditions()
	for _, condition := range conditions {
		c.setGaugeMetric(c.ConditionCount, ConditionCount, 1, map[string]string{
			MetricLabelNamespace:       req.Namespace,
			MetricLabelName:            req.Name,
			pmetrics.LabelType:         condition.Type,
			MetricLabelConditionStatus: string(condition.Status),
			pmetrics.LabelReason:       condition.Reason,
		}, additionalGaugeMetricLabels)
		c.setGaugeMetric(c.ConditionCurrentStatusSeconds, ConditionCurrentStatusSeconds, time.Since(condition.LastTransitionTime.Time).Seconds(), map[string]string{
			MetricLabelNamespace:       req.Namespace,
			MetricLabelName:            req.Name,
			pmetrics.LabelType:         condition.Type,
			MetricLabelConditionStatus: string(condition.Status),
			pmetrics.LabelReason:       condition.Reason,
		}, additionalGaugeMetricLabels)
	}

	// Set if a delete below may remove a series this reconcile just emitted, which the next reconcile re-creates
	deletedEmitted := false
	for _, observedCondition := range observedConditions.List() {
		if currentCondition := currentConditions.Get(observedCondition.Type); currentCondition == nil || currentCondition.Status != observedCondition.Status || currentCondition.Reason != observedCondition.Reason || gaugeLabelsChanged {
			// The previous reconcile emitted the observed condition with the observed additional labels, so delete exactly that
			// series. The deprecated metrics don't have the additional labels, so, as when this was a partial match including the
			// old additional labels, they are only deleted if the additional labels didn't change (or are only kind and group,
			// which the deprecated metrics carry with this controller's values). Otherwise they are deleted with the object.
			deleteDeprecated := !gaugeLabelsChanged || lo.Every([]string{pmetrics.LabelKind, pmetrics.LabelGroup}, lo.Keys(observedGaugeLabels))
			c.deleteConditionGaugeMetrics(req, observedCondition, observedGaugeLabels, deleteDeprecated)
			// With labels changed and named only kind and group, the deprecated delete is also the one of the series
			// just set for the condition if it didn't change
			deletedEmitted = deletedEmitted || (c.emitDeprecatedMetrics && deleteDeprecated && gaugeLabelsChanged)
			if c.emitDeprecatedMetrics && !deleteDeprecated {
				storeUnique(&c.orphanedDeprecatedConditions, req, observedCondition, sameSeries)
			}
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
		c.incCounterMetric(c.ConditionTransitionsTotal, ConditionTransitionsTotal, map[string]string{
			pmetrics.LabelType:         condition.Type,
			MetricLabelConditionStatus: string(condition.Status),
			pmetrics.LabelReason:       condition.Reason,
		}, additionalMetricLabels)
		if observedCondition == nil {
			continue
		}
		duration := condition.LastTransitionTime.Time.Sub(observedCondition.LastTransitionTime.Time).Seconds()
		c.observeHistogram(c.ConditionDuration, ConditionDuration, duration, map[string]string{
			pmetrics.LabelType:           observedCondition.Type,
			MetricLabelConditionStatus:   string(observedCondition.Status),
			MetricLabelToConditionStatus: string(condition.Status),
		}, additionalMetricLabels)
		c.eventRecorder.Event(o, v1.EventTypeNormal, condition.Type, fmt.Sprintf("Status condition transitioned, Type: %s, Status: %s -> %s, Reason: %s%s",
			condition.Type,
			observedCondition.Status,
			condition.Status,
			condition.Reason,
			lo.Ternary(condition.Message != "", fmt.Sprintf(", Message: %s", condition.Message), ""),
		))
	}
	c.recordUnchanged(req, o, lo.Ternary(readOnlyConditions, conditions, conditionsBefore), conditions, additionalGaugeMetricLabels, deletedEmitted)
	return reconcile.Result{RequeueAfter: requeueInterval}, nil
}

func (c *Controller[T]) incCounterMetric(current pmetrics.CounterMetric, deprecated pmetrics.CounterMetric, labels, additionalLabels map[string]string) {
	current.Inc(lo.Assign(labels, additionalLabels))
	if c.emitDeprecatedMetrics {
		labels[pmetrics.LabelKind] = c.gvk.Kind
		labels[pmetrics.LabelGroup] = c.gvk.Group
		deprecated.Inc(labels)
	}
}

func (c *Controller[T]) setGaugeMetric(current pmetrics.GaugeMetric, deprecated pmetrics.GaugeMetric, value float64, labels, additionalLabels map[string]string) {
	current.Set(value, lo.Assign(labels, additionalLabels))
	if c.emitDeprecatedMetrics {
		labels[pmetrics.LabelKind] = c.gvk.Kind
		labels[pmetrics.LabelGroup] = c.gvk.Group
		deprecated.Set(value, labels)
	}
}

func (c *Controller[T]) deleteGaugeMetric(current pmetrics.GaugeMetric, deprecated pmetrics.GaugeMetric, labels, additionalLabels map[string]string, deleteDeprecated bool) {
	current.Delete(lo.Assign(labels, additionalLabels))
	if c.emitDeprecatedMetrics && deleteDeprecated {
		labels[pmetrics.LabelKind] = c.gvk.Kind
		labels[pmetrics.LabelGroup] = c.gvk.Group
		deprecated.Delete(labels)
	}
}

func (c *Controller[T]) deleteConditionGaugeMetrics(req reconcile.Request, condition Condition, additionalLabels map[string]string, deleteDeprecated bool) {
	for _, m := range []struct{ current, deprecated pmetrics.GaugeMetric }{
		{c.ConditionCount, ConditionCount},
		{c.ConditionCurrentStatusSeconds, ConditionCurrentStatusSeconds},
	} {
		c.deleteGaugeMetric(m.current, m.deprecated, map[string]string{
			MetricLabelNamespace:       req.Namespace,
			MetricLabelName:            req.Name,
			pmetrics.LabelType:         condition.Type,
			MetricLabelConditionStatus: string(condition.Status),
			pmetrics.LabelReason:       condition.Reason,
		}, additionalLabels, deleteDeprecated)
	}
}

func (c *Controller[T]) deleteDeprecatedConditionGaugeMetrics(req reconcile.Request, condition Condition) {
	for _, deprecated := range []pmetrics.GaugeMetric{ConditionCount, ConditionCurrentStatusSeconds} {
		deprecated.Delete(map[string]string{
			MetricLabelNamespace:       req.Namespace,
			MetricLabelName:            req.Name,
			pmetrics.LabelType:         condition.Type,
			MetricLabelConditionStatus: string(condition.Status),
			pmetrics.LabelReason:       condition.Reason,
			pmetrics.LabelKind:         c.gvk.Kind,
			pmetrics.LabelGroup:        c.gvk.Group,
		})
	}
}

// storeUnique appends v to the slice stored for req unless the slice already holds an equal value
func storeUnique[V any](m *sync.Map, req reconcile.Request, v V, equal func(V, V) bool) {
	stored, _ := m.Load(req)
	values, _ := stored.([]V)
	if !lo.ContainsBy(values, func(e V) bool { return equal(e, v) }) {
		m.Store(req, append(values, v))
	}
}

// sameSeries reports whether two conditions are emitted as the same condition gauge series
func sameSeries(a, b Condition) bool {
	return a.Type == b.Type && a.Status == b.Status && a.Reason == b.Reason
}

// hasSeriesForKind reports whether a gauge holds any series labeled with the given kind and group
func hasSeriesForKind(m pmetrics.GaugeMetric, gvk schema.GroupVersionKind) bool {
	gauge, ok := m.(*pmetrics.PrometheusGauge)
	if !ok {
		return true
	}
	ch := make(chan prometheus.Metric)
	go func() {
		gauge.Collect(ch)
		close(ch)
	}()
	found := false
	for metric := range ch {
		pb := &dto.Metric{}
		if err := metric.Write(pb); err != nil {
			found = true
			continue
		}
		labels := lo.SliceToMap(pb.GetLabel(), func(l *dto.LabelPair) (string, string) { return l.GetName(), l.GetValue() })
		found = found || (labels[pmetrics.LabelKind] == gvk.Kind && labels[pmetrics.LabelGroup] == gvk.Group)
	}
	return found
}

func (c *Controller[T]) observeHistogram(current pmetrics.ObservationMetric, deprecated pmetrics.ObservationMetric, value float64, labels, additionalLabels map[string]string) {
	current.Observe(value, lo.Assign(labels, additionalLabels))
	if c.emitDeprecatedMetrics {
		labels[pmetrics.LabelKind] = c.gvk.Kind
		labels[pmetrics.LabelGroup] = c.gvk.Group
		deprecated.Observe(value, labels)
	}
}
