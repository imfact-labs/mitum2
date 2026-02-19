package isaacnetwork

import (
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/imfact-labs/mitum2/network/quicstream"
	"github.com/imfact-labs/mitum2/util"
	"github.com/imfact-labs/mitum2/util/hint"
	"github.com/imfact-labs/mitum2/util/localtime"
)

var NodeMetricsHint = hint.MustNewHint("node-metrics-v0.0.1")

const (
	defaultRetention     = 10 * time.Minute
	defaultSampleLimit   = 1 << 10
	defaultInterval      = time.Minute
	defaultIntervalLabel = "1m"
)

type NodeMetrics struct {
	hint.BaseHinter
	Timestamp  time.Time                  `json:"timestamp"`
	Uptime     time.Duration              `json:"uptime"`
	Cumulative CumulativeMetrics          `json:"cumulative"`
	Intervals  map[string]IntervalMetrics `json:"intervals"`
}

func (m NodeMetrics) MarshalJSON() ([]byte, error) {
	type alias struct {
		hint.BaseHinter
		Timestamp  localtime.Time             `json:"timestamp"`
		Uptime     util.ReadableDuration      `json:"uptime"`
		Cumulative CumulativeMetrics          `json:"cumulative"`
		Intervals  map[string]IntervalMetrics `json:"intervals"`
	}

	return util.MarshalJSON(alias{
		BaseHinter: m.BaseHinter,
		Timestamp:  localtime.New(m.Timestamp),
		Uptime:     util.ReadableDuration(m.Uptime),
		Cumulative: m.Cumulative,
		Intervals:  m.Intervals,
	})
}

func (m *NodeMetrics) UnmarshalJSON(b []byte) error {
	e := util.StringError("unmarshal NodeMetrics")

	type alias struct {
		hint.BaseHinter
		Timestamp  time.Time                  `json:"timestamp"`
		Uptime     *util.ReadableDuration     `json:"uptime"`
		Cumulative CumulativeMetrics          `json:"cumulative"`
		Intervals  map[string]IntervalMetrics `json:"intervals"`
	}

	var u alias

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e.Wrap(err)
	}

	m.BaseHinter = u.BaseHinter
	m.Timestamp = u.Timestamp
	m.Cumulative = u.Cumulative
	m.Intervals = u.Intervals

	durArgs := [][2]interface{}{
		{u.Uptime, &m.Uptime},
	}

	for i := range durArgs {
		v := durArgs[i][0].(*util.ReadableDuration) //nolint:forcetypeassert //...
		t := durArgs[i][1].(*time.Duration)         //nolint:forcetypeassert //...

		if reflect.ValueOf(v).IsZero() {
			continue
		}

		if err := util.SetInterfaceValue(time.Duration(*v), t); err != nil {
			return err
		}
	}

	return nil
}

type CumulativeMetrics struct {
	QuicBytesSent          uint64 `json:"quic_bytes_sent"`
	QuicBytesReceived      uint64 `json:"quic_bytes_received"`
	MemberlistBroadcasts   uint64 `json:"memberlist_broadcasts"`
	MemberlistMessagesRecv uint64 `json:"memberlist_messages_recv"`
}

type IntervalMetrics struct {
	QuicBytesSent          uint64  `json:"quic_bytes_sent"`
	QuicBytesReceived      uint64  `json:"quic_bytes_received"`
	QuicBytesPerSecSent    float64 `json:"quic_bytes_per_sec_sent"`
	QuicBytesPerSecRecv    float64 `json:"quic_bytes_per_sec_recv"`
	MemberlistBroadcasts   uint64  `json:"memberlist_broadcasts"`
	MemberlistMessagesRecv uint64  `json:"memberlist_messages_recv"`
	MemberlistMsgsPerSec   float64 `json:"memberlist_msgs_per_sec"`
	ActiveConnections      int64   `json:"active_connections"`
	ActiveStreams          int64   `json:"active_streams"`
	MemberlistMembers      int64   `json:"memberlist_members"`
}

type metricsSample struct {
	at                   time.Time
	quicBytesSent        uint64
	quicBytesRecv        uint64
	memberlistBroadcasts uint64
	memberlistMessages   uint64
}

type NetworkMetricsCollector struct {
	quicBytesSent      atomic.Uint64
	quicBytesRecv      atomic.Uint64
	memberlistBcasts   atomic.Uint64
	memberlistMsgsRecv atomic.Uint64
	activeConnections  atomic.Int64
	activeStreams      atomic.Int64
	memberlistMembers  atomic.Int64

	startTime time.Time
	clock     func() time.Time

	sampleLock sync.Mutex
	samples    []metricsSample
}

var _ quicstream.MetricsCollector = (*NetworkMetricsCollector)(nil)

func NewNetworkMetricsCollector() *NetworkMetricsCollector {
	now := time.Now().UTC()

	return &NetworkMetricsCollector{
		startTime: now,
		clock:     func() time.Time { return time.Now().UTC() },
		samples:   []metricsSample{{at: now}},
	}
}

func (c *NetworkMetricsCollector) GetSnapshot(interval string) NodeMetrics {
	if c == nil {
		return NodeMetrics{}
	}

	now := c.clock()
	sample := metricsSample{
		at:                   now,
		quicBytesSent:        c.quicBytesSent.Load(),
		quicBytesRecv:        c.quicBytesRecv.Load(),
		memberlistBroadcasts: c.memberlistBcasts.Load(),
		memberlistMessages:   c.memberlistMsgsRecv.Load(),
	}

	intervalSpecs := c.intervalSpecs(interval)

	c.sampleLock.Lock()
	c.samples = append(c.samples, sample)
	c.pruneSamplesLocked(now)

	intervalResult := make(map[string]IntervalMetrics, len(intervalSpecs))
	for _, spec := range intervalSpecs {
		baseline := c.baselineLocked(spec.duration, now)
		intervalResult[spec.label] = c.buildIntervalMetrics(spec.duration, baseline, sample)
	}
	c.sampleLock.Unlock()

	return NodeMetrics{
		BaseHinter: hint.NewBaseHinter(NodeMetricsHint),
		Timestamp:  now,
		Uptime:     now.Sub(c.startTime),
		Cumulative: CumulativeMetrics{
			QuicBytesSent:          sample.quicBytesSent,
			QuicBytesReceived:      sample.quicBytesRecv,
			MemberlistBroadcasts:   sample.memberlistBroadcasts,
			MemberlistMessagesRecv: sample.memberlistMessages,
		},
		Intervals: intervalResult,
	}
}

func (c *NetworkMetricsCollector) RecordQuicBytesSent(n uint64) {
	if c == nil || n == 0 {
		return
	}

	c.quicBytesSent.Add(n)
}

func (c *NetworkMetricsCollector) RecordQuicBytesReceived(n uint64) {
	if c == nil || n == 0 {
		return
	}

	c.quicBytesRecv.Add(n)
}

func (c *NetworkMetricsCollector) RecordQuicStreamOpened() {
	if c == nil {
		return
	}

	c.activeStreams.Add(1)
}

func (c *NetworkMetricsCollector) RecordQuicStreamClosed() {
	if c == nil {
		return
	}

	c.activeStreams.Add(-1)
}

func (c *NetworkMetricsCollector) RecordQuicConnectionOpened() {
	if c == nil {
		return
	}

	c.activeConnections.Add(1)
}

func (c *NetworkMetricsCollector) RecordQuicConnectionClosed() {
	if c == nil {
		return
	}

	c.activeConnections.Add(-1)
}

func (c *NetworkMetricsCollector) RecordMemberlistBroadcast() {
	if c == nil {
		return
	}

	c.memberlistBcasts.Add(1)
}

func (c *NetworkMetricsCollector) RecordMemberlistMessageReceived() {
	if c == nil {
		return
	}

	c.memberlistMsgsRecv.Add(1)
}

func (c *NetworkMetricsCollector) SetMemberlistMembers(n int) {
	if c == nil {
		return
	}

	c.memberlistMembers.Store(int64(n))
}

type intervalSpec struct {
	label    string
	duration time.Duration
}

func (c *NetworkMetricsCollector) intervalSpecs(requested string) []intervalSpec {
	specs := []intervalSpec{
		{label: "1s", duration: time.Second},
		{label: defaultIntervalLabel, duration: time.Minute},
		{label: "5m", duration: 5 * time.Minute},
	}

	if requested == "" {
		return specs
	}

	alreadyIncluded := false
	for i := range specs {
		if specs[i].label == requested {
			alreadyIncluded = true
			break
		}
	}

	if alreadyIncluded {
		return specs
	}

	if d, err := time.ParseDuration(requested); err == nil && d > 0 {
		specs = append(specs, intervalSpec{label: requested, duration: d})
	}

	return specs
}

func (c *NetworkMetricsCollector) pruneSamplesLocked(now time.Time) {
	cutoff := now.Add(-defaultRetention)

	var idx int
	for idx = 0; idx < len(c.samples); idx++ {
		if c.samples[idx].at.After(cutoff) {
			break
		}
	}

	if idx > 0 && idx < len(c.samples) {
		c.samples = append([]metricsSample{}, c.samples[idx:]...)
	}

	if len(c.samples) > defaultSampleLimit {
		c.samples = c.samples[len(c.samples)-defaultSampleLimit:]
	}
}

func (c *NetworkMetricsCollector) baselineLocked(duration time.Duration, now time.Time) metricsSample {
	target := now.Add(-duration)

	for i := len(c.samples) - 1; i >= 0; i-- {
		s := c.samples[i]
		if !s.at.After(target) {
			return s
		}
	}

	if len(c.samples) > 0 {
		return c.samples[0]
	}

	return metricsSample{at: now}
}

func (c *NetworkMetricsCollector) buildIntervalMetrics(
	duration time.Duration,
	baseline, current metricsSample,
) IntervalMetrics {
	elapsed := current.at.Sub(baseline.at)
	if elapsed <= 0 {
		elapsed = duration
	}

	bytesSent := current.quicBytesSent - baseline.quicBytesSent
	bytesRecv := current.quicBytesRecv - baseline.quicBytesRecv
	bcasts := current.memberlistBroadcasts - baseline.memberlistBroadcasts
	msgs := current.memberlistMessages - baseline.memberlistMessages
	elapsedSec := math.Max(elapsed.Seconds(), 1)

	return IntervalMetrics{
		QuicBytesSent:          bytesSent,
		QuicBytesReceived:      bytesRecv,
		QuicBytesPerSecSent:    float64(bytesSent) / elapsedSec,
		QuicBytesPerSecRecv:    float64(bytesRecv) / elapsedSec,
		MemberlistBroadcasts:   bcasts,
		MemberlistMessagesRecv: msgs,
		MemberlistMsgsPerSec:   float64(msgs) / elapsedSec,
		ActiveConnections:      c.activeConnections.Load(),
		ActiveStreams:          c.activeStreams.Load(),
		MemberlistMembers:      c.memberlistMembers.Load(),
	}
}
