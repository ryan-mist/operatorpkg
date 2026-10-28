package status_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/awslabs/operatorpkg/option"
	"github.com/awslabs/operatorpkg/status"
	"github.com/awslabs/operatorpkg/test"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/lo"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// BenchmarkStatusController measures a reconcile of one object while the controller tracks many: a steady-state
// requeue, a condition transition, and the deletion of the object. The typed object is shaped like a NodeClaim
// (deprecated metrics, 2 labels, 7 conditions) and the generic one like a Node (3 labels, 5 conditions).
func BenchmarkStatusController(b *testing.B) {
	lo.Must0(SchemeBuilder.AddToScheme(scheme.Scheme))
	for _, kind := range []struct {
		name       string
		kind       objectKind
		opts       []option.Function[status.Option]
		labels     map[string]string
		conditions []string
	}{
		{"typed", typedObjectKind, []option.Function[status.Option]{status.EmitDeprecatedMetrics, status.WithLabels("operator.pkg/nodeclass", "operator.pkg/nodepool")},
			map[string]string{"operator.pkg/nodeclass": "default", "operator.pkg/nodepool": "default"},
			[]string{status.ConditionReady, test.ConditionTypeFoo, test.ConditionTypeBar, "Launched", "Registered", "Initialized", "Drifted"}},
		{"generic", genericObjectKind, []option.Function[status.Option]{status.WithLabels("operator.pkg/nodeclass", "operator.pkg/nodepool", "operator.pkg/initialized")},
			map[string]string{"operator.pkg/nodeclass": "default", "operator.pkg/nodepool": "default", "operator.pkg/initialized": "true"},
			[]string{status.ConditionReady, "MemoryPressure", "DiskPressure", "PIDPressure", "Drifted"}},
	} {
		for _, objects := range []int{1000, 5000} {
			for _, op := range []string{"steady", "transition", "delete"} {
				b.Run(fmt.Sprintf("kind=%s/objects=%d/op=%s", kind.name, objects, op), func(b *testing.B) {
					orig := metrics.Registry
					metrics.Registry = prometheus.NewRegistry()
					defer func() { metrics.Registry = orig }()
					resetDeprecatedGauges()
					kubeClient := &mapClient{objects: map[types.NamespacedName]client.Object{}}
					controller := kind.kind.newController(kubeClient, kind.opts...)
					defer controller.unregister()

					ltt := metav1.NewTime(time.Now().Add(-time.Hour))
					state := func(drifted bool) objectState {
						return objectState{labels: kind.labels, conditions: lo.Map(kind.conditions, func(t string, _ int) status.Condition {
							if t == "Drifted" {
								return status.Condition{Type: t, Status: lo.Ternary(drifted, metav1.ConditionTrue, metav1.ConditionFalse), Reason: lo.Ternary(drifted, "Drifted", "NotDrifted"), LastTransitionTime: ltt}
							}
							return status.Condition{Type: t, Status: metav1.ConditionTrue, Reason: t, LastTransitionTime: ltt}
						})}
					}
					requests := make([]reconcile.Request, objects)
					for i := range requests {
						requests[i] = reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: fmt.Sprintf("object-%d", i)}}
						kubeClient.objects[requests[i].NamespacedName] = kind.kind.build(requests[i].Name, state(false))
						lo.Must(controller.Reconcile(context.Background(), requests[i]))
					}
					drifted := make([]bool, objects)

					b.ReportAllocs()
					b.ResetTimer()
					for n := 0; n < b.N; n++ {
						i := n % objects
						var stored client.Object
						switch op {
						case "transition":
							drifted[i] = !drifted[i]
							kubeClient.objects[requests[i].NamespacedName] = kind.kind.build(requests[i].Name, state(drifted[i]))
						case "delete":
							b.StopTimer()
							stored = kubeClient.objects[requests[i].NamespacedName]
							delete(kubeClient.objects, requests[i].NamespacedName)
							b.StartTimer()
						}
						lo.Must(controller.Reconcile(context.Background(), requests[i]))
						if op == "delete" {
							// Restore the object so that the controller keeps tracking the same number of objects
							b.StopTimer()
							kubeClient.objects[requests[i].NamespacedName] = stored
							lo.Must(controller.Reconcile(context.Background(), requests[i]))
							b.StartTimer()
						}
					}
				})
			}
		}
	}
}
