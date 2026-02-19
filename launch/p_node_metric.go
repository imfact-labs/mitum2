package launch

import (
	"context"

	isaacnetwork "github.com/imfact-labs/mitum2/isaac/network"
	"github.com/imfact-labs/mitum2/network/quicstream"
	"github.com/imfact-labs/mitum2/util/ps"
)

var (
	PNameNodeMetric            = ps.Name("node-metric")
	MetricsCollectorContextKey = quicstream.MetricsCollectorContextKey
)

func PNodeMetric(pctx context.Context) (context.Context, error) {
	collector := isaacnetwork.NewNetworkMetricsCollector()

	return context.WithValue(pctx, MetricsCollectorContextKey, collector), nil
}
