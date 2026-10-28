package status_test

import (
	"context"
	"fmt"
	"maps"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"time"

	pmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/awslabs/operatorpkg/option"
	"github.com/awslabs/operatorpkg/status"
	"github.com/awslabs/operatorpkg/test"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// mapClient serves Gets from an in-memory map so that tests and benchmarks exercise the controller rather than a fake
// API server.
type mapClient struct {
	client.Client
	objects map[types.NamespacedName]client.Object
}

func (m *mapClient) Get(_ context.Context, key client.ObjectKey, obj client.Object, _ ...client.GetOption) error {
	stored, ok := m.objects[key]
	if !ok {
		return errors.NewNotFound(schema.GroupResource{}, key.Name)
	}
	reflect.ValueOf(obj).Elem().Set(reflect.ValueOf(stored.DeepCopyObject()).Elem())
	return nil
}

// statusController is a status controller of any object type, along with the per-kind gauges it emits
type statusController struct {
	reconcile.Reconciler
	collectors                        []prometheus.Collector
	count, currentStatus, termination *prometheus.GaugeVec
}

func newStatusController[T status.Object](c *status.Controller[T], r reconcile.Reconciler) *statusController {
	return &statusController{
		Reconciler: r,
		collectors: []prometheus.Collector{
			c.ConditionDuration.(*pmetrics.PrometheusHistogram).HistogramVec,
			c.ConditionCount.(*pmetrics.PrometheusGauge).GaugeVec,
			c.ConditionCurrentStatusSeconds.(*pmetrics.PrometheusGauge).GaugeVec,
			c.ConditionTransitionsTotal.(*pmetrics.PrometheusCounter).CounterVec,
			c.TerminationCurrentTimeSeconds.(*pmetrics.PrometheusGauge).GaugeVec,
			c.TerminationDuration.(*pmetrics.PrometheusHistogram).HistogramVec,
		},
		count:         c.ConditionCount.(*pmetrics.PrometheusGauge).GaugeVec,
		currentStatus: c.ConditionCurrentStatusSeconds.(*pmetrics.PrometheusGauge).GaugeVec,
		termination:   c.TerminationCurrentTimeSeconds.(*pmetrics.PrometheusGauge).GaugeVec,
	}
}

func (s *statusController) unregister() {
	for _, c := range s.collectors {
		metrics.Registry.Unregister(c)
	}
}

// objectState is the part of an object that the status controller reads
type objectState struct {
	labels     map[string]string
	conditions []status.Condition
	deleting   bool
}

// objectKind builds objects and controllers of one type
type objectKind struct {
	build         func(name string, s objectState) client.Object
	newController func(c client.Client, opts ...option.Function[status.Option]) *statusController
	// observedConditions returns the conditions the controller sees on the object
	observedConditions func(o client.Object) []status.Condition
}

var (
	deletionTime    = metav1.NewTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	typedObjectKind = objectKind{
		build: func(name string, s objectState) client.Object {
			o := &test.CustomObject{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Labels: maps.Clone(s.labels)}}
			o.Status.Conditions = append([]status.Condition{}, s.conditions...)
			if s.deleting {
				o.DeletionTimestamp = &deletionTime
				o.Finalizers = []string{"test/finalizer"}
			}
			return o
		},
		newController: func(c client.Client, opts ...option.Function[status.Option]) *statusController {
			controller := status.NewController[*test.CustomObject](c, &record.FakeRecorder{}, opts...)
			return newStatusController(controller, controller)
		},
		observedConditions: func(o client.Object) []status.Condition {
			obj := o.DeepCopyObject().(*test.CustomObject)
			return obj.StatusConditions().List()
		},
	}
	genericObjectKind = objectKind{
		build: func(name string, s objectState) client.Object {
			o := &TestGenericObject{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Labels: maps.Clone(s.labels)}}
			o.Status.Conditions = lo.Map(s.conditions, func(c status.Condition, _ int) metav1.Condition { return metav1.Condition(c) })
			if s.deleting {
				o.DeletionTimestamp = &deletionTime
				o.Finalizers = []string{"test/finalizer"}
			}
			return o
		},
		newController: func(c client.Client, opts ...option.Function[status.Option]) *statusController {
			controller := status.NewGenericObjectController[*TestGenericObject](c, &record.FakeRecorder{}, opts...)
			return newStatusController(controller.Controller, controller)
		},
		observedConditions: func(o client.Object) []status.Condition {
			return lo.Map(o.(*TestGenericObject).Status.Conditions, func(c metav1.Condition, _ int) status.Condition { return status.Condition(c) })
		},
	}
)

// statusMetricsScenario drives a controller through random condition, label and lifecycle changes of a few objects
type statusMetricsScenario struct {
	kind       objectKind
	opts       []option.Function[status.Option]
	labelKeys  []string // the object label keys the controller emits on its gauges
	rng        *rand.Rand
	client     *mapClient
	controller *statusController
	model      *statusGaugeModel
}

func newStatusMetricsScenario(seed int64, kind objectKind, metricLabels, gaugeLabels []string) *statusMetricsScenario {
	opts := []option.Function[status.Option]{status.WithLabels(metricLabels...), status.WithGaugeLabels(gaugeLabels...)}
	s := &statusMetricsScenario{
		kind:      kind,
		opts:      opts,
		labelKeys: append(append([]string{}, metricLabels...), gaugeLabels...),
		rng:       rand.New(rand.NewSource(seed)),
		client:    &mapClient{objects: map[types.NamespacedName]client.Object{}},
	}
	s.controller = kind.newController(s.client, opts...)
	s.model = newStatusGaugeModel(s.labelKeys)
	return s
}

var (
	scenarioNames      = []string{"a", "b", "c"}
	scenarioTypes      = []string{status.ConditionReady, test.ConditionTypeFoo, test.ConditionTypeBar, test.ConditionTypeBaz}
	scenarioStatuses   = []metav1.ConditionStatus{metav1.ConditionTrue, metav1.ConditionFalse, metav1.ConditionUnknown}
	scenarioReasons    = []string{"", "R1", "R2"}
	scenarioLabelValue = []string{"", "x", "y"} // "" removes the label
)

// step applies one random change, reconciles the affected object and returns a description of the change
func (s *statusMetricsScenario) step(i int) string {
	name := scenarioNames[s.rng.Intn(len(scenarioNames))]
	key := types.NamespacedName{Namespace: "default", Name: name}
	stored, exists := s.client.objects[key]
	state := objectState{}
	if exists {
		state = objectState{labels: stored.GetLabels(), conditions: s.kind.observedConditions(stored), deleting: stored.GetDeletionTimestamp() != nil}
	}
	desc := ""
	switch r := s.rng.Intn(100); {
	case r < 5:
		// Restart: no in-memory state and no series
		s.controller.unregister()
		s.controller = s.kind.newController(s.client, s.opts...)
		s.model.restart()
		return "restart"
	case r < 10:
		name, key = "ghost", types.NamespacedName{Namespace: "default", Name: "ghost"}
		desc = "reconcile never created"
	case r < 20:
		delete(s.client.objects, key)
		desc = "delete " + name
	case r < 30:
		desc = "requeue " + name
	case r < 38 && exists && !state.deleting:
		state.deleting = true
		s.client.objects[key] = s.kind.build(name, state)
		desc = "terminate " + name
	default:
		desc = s.mutate(&state, i)
		s.client.objects[key] = s.kind.build(name, state)
	}
	if stored, ok := s.client.objects[key]; ok {
		s.model.reconcileFound(name, s.kind.observedConditions(stored), stored.GetLabels(), stored.GetDeletionTimestamp() != nil)
	} else {
		s.model.cleanupNotFound(name)
	}
	lo.Must(s.controller.Reconcile(context.Background(), reconcile.Request{NamespacedName: key}))
	return desc
}

func (s *statusMetricsScenario) mutate(state *objectState, i int) string {
	var changes []string
	for n := 1 + s.rng.Intn(2); n > 0; n-- {
		condition := status.Condition{
			Type:               scenarioTypes[s.rng.Intn(len(scenarioTypes))],
			Status:             scenarioStatuses[s.rng.Intn(len(scenarioStatuses))],
			Reason:             scenarioReasons[s.rng.Intn(len(scenarioReasons))],
			LastTransitionTime: metav1.NewTime(deletionTime.Add(time.Duration(i) * time.Second)),
		}
		switch r := s.rng.Intn(10); {
		case r < 4:
			if j := lo.IndexOf(lo.Map(state.conditions, func(c status.Condition, _ int) string { return c.Type }), condition.Type); j >= 0 {
				state.conditions = append(append(append([]status.Condition{}, state.conditions[:j]...), condition), state.conditions[j+1:]...)
			} else {
				state.conditions = append(state.conditions, condition)
			}
			changes = append(changes, fmt.Sprintf("set %s=%s/%s", condition.Type, condition.Status, condition.Reason))
		case r < 5:
			state.conditions = append(state.conditions, condition)
			changes = append(changes, fmt.Sprintf("append %s=%s/%s", condition.Type, condition.Status, condition.Reason))
		case r < 6 && len(state.conditions) > 0:
			j := s.rng.Intn(len(state.conditions))
			changes = append(changes, "remove "+state.conditions[j].Type)
			state.conditions = append(append([]status.Condition{}, state.conditions[:j]...), state.conditions[j+1:]...)
		default:
			if len(s.labelKeys) == 0 {
				continue
			}
			labelKey, value := s.labelKeys[s.rng.Intn(len(s.labelKeys))], scenarioLabelValue[s.rng.Intn(len(scenarioLabelValue))]
			state.labels = lo.Assign(state.labels)
			if value == "" {
				delete(state.labels, labelKey)
			} else {
				state.labels[labelKey] = value
			}
			changes = append(changes, fmt.Sprintf("label %s=%q", labelKey, value))
		}
	}
	return "update: " + strings.Join(changes, ", ")
}

// series returns the series of each status gauge for the scenario's kind
func (s *statusMetricsScenario) series() map[string][]string {
	result := map[string][]string{
		"count":          gaugeSeries(s.controller.count, nil),
		"current_status": gaugeSeries(s.controller.currentStatus, nil),
		"termination":    gaugeSeries(s.controller.termination, nil),
	}
	return result
}

// gaugeSeries returns the sorted label sets of the series of a gauge that carry the given labels
func gaugeSeries(g *prometheus.GaugeVec, filter map[string]string) []string {
	ch := make(chan prometheus.Metric)
	go func() {
		g.Collect(ch)
		close(ch)
	}()
	series := []string{}
	for m := range ch {
		pb := &dto.Metric{}
		lo.Must0(m.Write(pb))
		labels := lo.SliceToMap(pb.GetLabel(), func(l *dto.LabelPair) (string, string) { return l.GetName(), l.GetValue() })
		if lo.EveryBy(lo.Entries(filter), func(e lo.Entry[string, string]) bool { return labels[e.Key] == e.Value }) {
			series = append(series, strings.Join(lo.Map(pb.GetLabel(), func(l *dto.LabelPair, _ int) string { return l.GetName() + "=" + l.GetValue() }), ","))
		}
	}
	sort.Strings(series)
	return series
}

// statusGaugeModel replays the status controller's gauge updates against its own gauges, deleting stale series with
// DeletePartialMatch exactly as the controller did before it tracked the series it emitted.
type statusGaugeModel struct {
	labelKeys                         []string
	count, currentStatus, termination *prometheus.GaugeVec
	observed                          map[string]modelObservation
}

type modelObservation struct {
	conditions []status.Condition
	labels     map[string]string
}

func newStatusGaugeModel(labelKeys []string) *statusGaugeModel {
	m := &statusGaugeModel{labelKeys: labelKeys}
	m.restart()
	return m
}

func (m *statusGaugeModel) restart() {
	additional := lo.Map(m.labelKeys, func(k string, _ int) string { return toPrometheusLabel(k) })
	conditionLabels := []string{status.MetricLabelNamespace, status.MetricLabelName, pmetrics.LabelType, status.MetricLabelConditionStatus, pmetrics.LabelReason}
	m.count = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "count"}, append(append([]string{}, conditionLabels...), additional...))
	m.currentStatus = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "current_status"}, append(append([]string{}, conditionLabels...), additional...))
	m.termination = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "termination"}, append([]string{status.MetricLabelNamespace, status.MetricLabelName}, additional...))
	m.observed = map[string]modelObservation{}
}

func toPrometheusLabel(k string) string {
	return strings.NewReplacer("/", "_", ".", "_", "-", "_").Replace(k)
}

func (m *statusGaugeModel) cleanupNotFound(name string) {
	delete(m.observed, name)
	object := map[string]string{status.MetricLabelNamespace: "default", status.MetricLabelName: name}
	for _, g := range []*prometheus.GaugeVec{m.count, m.currentStatus, m.termination} {
		g.DeletePartialMatch(object)
	}
}

func (m *statusGaugeModel) reconcileFound(name string, conditions []status.Condition, objectLabels map[string]string, deleting bool) {
	object := map[string]string{status.MetricLabelNamespace: "default", status.MetricLabelName: name}
	labels := lo.SliceToMap(m.labelKeys, func(k string) (string, string) { return toPrometheusLabel(k), objectLabels[k] })
	if deleting {
		m.termination.With(lo.Assign(object, labels)).Set(1)
	}
	observed := m.observed[name]
	m.observed[name] = modelObservation{conditions: conditions, labels: labels}
	conditionLabels := func(c status.Condition) map[string]string {
		return lo.Assign(object, map[string]string{pmetrics.LabelType: c.Type, status.MetricLabelConditionStatus: string(c.Status), pmetrics.LabelReason: c.Reason})
	}
	for _, c := range conditions {
		m.count.With(lo.Assign(conditionLabels(c), labels)).Set(1)
		m.currentStatus.With(lo.Assign(conditionLabels(c), labels)).Set(1)
	}
	labelsChanged := !maps.Equal(labels, observed.labels)
	for _, o := range observed.conditions {
		current, found := lo.Find(conditions, func(c status.Condition) bool { return c.Type == o.Type })
		if found && current.Status == o.Status && current.Reason == o.Reason && !labelsChanged {
			continue
		}
		partial := lo.Assign(conditionLabels(o), lo.Ternary(labelsChanged, observed.labels, nil))
		m.count.DeletePartialMatch(partial)
		m.currentStatus.DeletePartialMatch(partial)
	}
}

func (m *statusGaugeModel) series() map[string][]string {
	result := map[string][]string{
		"count":          gaugeSeries(m.count, nil),
		"current_status": gaugeSeries(m.currentStatus, nil),
		"termination":    gaugeSeries(m.termination, nil),
	}
	return result
}

var _ = Describe("Gauge deletion", func() {
	DescribeTable("should delete the same series as DeletePartialMatch over random changes",
		func(kind objectKind, metricLabels, gaugeLabels []string) {
			// A registry remembers the label names of unregistered metrics, so use a new one for each set of labels
			metrics.Registry = prometheus.NewRegistry()
			DeferCleanup(func() { metrics.Registry = registry })
			for seed := int64(0); seed < 25; seed++ {
				s := newStatusMetricsScenario(seed, kind, metricLabels, gaugeLabels)
				var history []string
				for i := 0; i < 200; i++ {
					history = append(history, s.step(i))
					Expect(s.series()).To(Equal(s.model.series()), "seed %d, steps:\n%s", seed, strings.Join(history, "\n"))
				}
				s.controller.unregister()
			}
		},
		Entry("typed object with metric and gauge labels", typedObjectKind, []string{"operator.pkg/key1"}, []string{"operator.pkg/key2"}),
		Entry("typed object without labels", typedObjectKind, nil, nil),
		Entry("generic object with labels", genericObjectKind, []string{"operator.pkg/key1", "operator.pkg/key2"}, nil),
	)
})
