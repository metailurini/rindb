package rindb

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

var (
	tracer            = otel.Tracer("rindb")
	sstableTracer     = otel.Tracer("rindb/sstable")
	sstableMgmtTracer = otel.Tracer("rindb/sstablemgmt")
	walTracer         = otel.Tracer("rindb/wal")

	// SSTable metrics
	sstableMeter = otel.Meter("rindb/sstable")

	flushLatency    metric.Float64Histogram
	flushIOSize     metric.Int64Counter
	getValueLatency metric.Float64Histogram
	getValueIOSize  metric.Int64Counter

	// SSTable manager metrics
	sstableMgmtMeter = otel.Meter("rindb/sstablemgmt")

	addSSTableLatency   metric.Float64Histogram
	addSSTableCalls     metric.Int64Counter
	compactLatency      metric.Float64Histogram
	compactCalls        metric.Int64Counter
	getRelevantLatency  metric.Float64Histogram
	getRelevantCalls    metric.Int64Counter
	getRelevantSSTables metric.Int64Counter

	// WAL metrics
	walMeter = otel.Meter("rindb/wal")

	walLoadDuration       metric.Float64Histogram
	walAppendDuration     metric.Float64Histogram
	walAppendManyDuration metric.Float64Histogram
	walRecordsCounter     metric.Int64Counter
	walBytesCounter       metric.Int64Counter
)

func init() {
	flushLatency, _ = sstableMeter.Float64Histogram("rindb.sstable.flush.latency", metric.WithUnit("ms"))
	flushIOSize, _ = sstableMeter.Int64Counter("rindb.sstable.flush.io_bytes", metric.WithUnit("By"))
	getValueLatency, _ = sstableMeter.Float64Histogram("rindb.sstable.get_value.latency", metric.WithUnit("ms"))
	getValueIOSize, _ = sstableMeter.Int64Counter("rindb.sstable.get_value.io_bytes", metric.WithUnit("By"))

	addSSTableLatency, _ = sstableMgmtMeter.Float64Histogram("rindb.sstablemgmt.add.latency", metric.WithUnit("ms"))
	addSSTableCalls, _ = sstableMgmtMeter.Int64Counter("rindb.sstablemgmt.add.calls")
	compactLatency, _ = sstableMgmtMeter.Float64Histogram("rindb.sstablemgmt.compact.latency", metric.WithUnit("ms"))
	compactCalls, _ = sstableMgmtMeter.Int64Counter("rindb.sstablemgmt.compact.calls")
	getRelevantLatency, _ = sstableMgmtMeter.Float64Histogram("rindb.sstablemgmt.get_relevant.latency", metric.WithUnit("ms"))
	getRelevantCalls, _ = sstableMgmtMeter.Int64Counter("rindb.sstablemgmt.get_relevant.calls")
	getRelevantSSTables, _ = sstableMgmtMeter.Int64Counter("rindb.sstablemgmt.get_relevant.sstables")

	walLoadDuration, _ = walMeter.Float64Histogram("rindb.wal.load.duration", metric.WithUnit("s"))
	walAppendDuration, _ = walMeter.Float64Histogram("rindb.wal.append.duration", metric.WithUnit("s"))
	walAppendManyDuration, _ = walMeter.Float64Histogram("rindb.wal.append_many.duration", metric.WithUnit("s"))
	walRecordsCounter, _ = walMeter.Int64Counter("rindb.wal.records")
	walBytesCounter, _ = walMeter.Int64Counter("rindb.wal.bytes")
}
