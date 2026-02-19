package quicstream

import (
	"context"
	"io"

	"github.com/imfact-labs/mitum2/util"
)

// MetricsCollectorContextKey is used to propagate the optional metrics collector
// through contexts that are shared by the networking stack.
var MetricsCollectorContextKey = util.ContextKey("network-metrics-collector")

// MetricsCollector represents the subset of metrics hooks used by the network
// stack. Implementations are expected to be concurrency-safe.
type MetricsCollector interface {
	RecordQuicBytesSent(uint64)
	RecordQuicBytesReceived(uint64)
	RecordQuicStreamOpened()
	RecordQuicStreamClosed()
	RecordQuicConnectionOpened()
	RecordQuicConnectionClosed()
	RecordMemberlistBroadcast()
	RecordMemberlistMessageReceived()
	SetMemberlistMembers(int)
}

// WithMetricsCollector returns a derived context containing the provided
// collector.
func WithMetricsCollector(ctx context.Context, collector MetricsCollector) context.Context {
	if ctx == nil || collector == nil {
		return ctx
	}

	return context.WithValue(ctx, MetricsCollectorContextKey, collector)
}

// GetMetricsCollector extracts the MetricsCollector stored in the context, if
// any.
func GetMetricsCollector(ctx context.Context) MetricsCollector {
	if ctx == nil {
		return nil
	}

	if collector, ok := ctx.Value(MetricsCollectorContextKey).(MetricsCollector); ok {
		return collector
	}

	return nil
}

func wrapMetricsIO(ctx context.Context, reader io.Reader, writer io.WriteCloser) (io.Reader, io.WriteCloser) {
	collector := GetMetricsCollector(ctx)

	switch {
	case collector == nil:
		return reader, writer
	case reader == nil:
		return reader, &metricsWriteCloser{WriteCloser: writer, collector: collector}
	case writer == nil:
		return &metricsReader{reader: reader, collector: collector}, writer
	default:
		return &metricsReader{reader: reader, collector: collector},
			&metricsWriteCloser{WriteCloser: writer, collector: collector}
	}
}

type metricsReader struct {
	reader    io.Reader
	collector MetricsCollector
}

func (r *metricsReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 && r.collector != nil {
		r.collector.RecordQuicBytesReceived(uint64(n))
	}

	return n, err
}

type metricsWriteCloser struct {
	io.WriteCloser
	collector MetricsCollector
}

func (w *metricsWriteCloser) Write(p []byte) (int, error) {
	n, err := w.WriteCloser.Write(p)
	if n > 0 && w.collector != nil {
		w.collector.RecordQuicBytesSent(uint64(n))
	}

	return n, err
}
