package rindb

import (
	"log"

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
	walRecordsCounter     metric.Int64UpDownCounter
	walBytesCounter       metric.Int64UpDownCounter

	// Table cache metrics
	tableCacheMeter = otel.Meter("rindb/tablecache")
	cacheHits       metric.Int64Counter
	cacheMisses     metric.Int64Counter
	cacheOpens      metric.Int64Counter
	cacheCloses     metric.Int64Counter
	cacheEvicts     metric.Int64Counter
	cachePromotions metric.Int64Counter

	// RinDB metrics
	rindbMeter = otel.Meter("rindb")

	getCalls                    metric.Int64Counter
	putCalls                    metric.Int64Counter
	removeCalls                 metric.Int64Counter
	iRangeCalls                 metric.Int64Counter
	flushCount                  metric.Int64Counter
	closeBackgroundWaitDuration metric.Float64Histogram
)

func must[T any](v T, err error) T {
	if err != nil {
		log.Printf("telemetry init: %v", err)
		panic(err)
	}
	return v
}

func init() {
	flushLatency = must(sstableMeter.Float64Histogram("rindb.sstable.flush.latency", metric.WithUnit("ms")))
	flushIOSize = must(sstableMeter.Int64Counter("rindb.sstable.flush.io_bytes", metric.WithUnit("By")))
	getValueLatency = must(sstableMeter.Float64Histogram("rindb.sstable.get_value.latency", metric.WithUnit("ms")))
	getValueIOSize = must(sstableMeter.Int64Counter("rindb.sstable.get_value.io_bytes", metric.WithUnit("By")))

	addSSTableLatency = must(sstableMgmtMeter.Float64Histogram("rindb.sstablemgmt.add.latency", metric.WithUnit("ms")))
	addSSTableCalls = must(sstableMgmtMeter.Int64Counter("rindb.sstablemgmt.add.calls"))
	compactLatency = must(sstableMgmtMeter.Float64Histogram("rindb.sstablemgmt.compact.latency", metric.WithUnit("ms")))
	compactCalls = must(sstableMgmtMeter.Int64Counter("rindb.sstablemgmt.compact.calls"))
	getRelevantLatency = must(sstableMgmtMeter.Float64Histogram("rindb.sstablemgmt.get_relevant.latency", metric.WithUnit("ms")))
	getRelevantCalls = must(sstableMgmtMeter.Int64Counter("rindb.sstablemgmt.get_relevant.calls"))
	getRelevantSSTables = must(sstableMgmtMeter.Int64Counter("rindb.sstablemgmt.get_relevant.sstables"))

	walLoadDuration = must(walMeter.Float64Histogram("rindb.wal.load.duration", metric.WithUnit("ms")))
	walAppendDuration = must(walMeter.Float64Histogram("rindb.wal.append.duration", metric.WithUnit("ms")))
	walAppendManyDuration = must(walMeter.Float64Histogram("rindb.wal.append_many.duration", metric.WithUnit("ms")))
	walRecordsCounter = must(walMeter.Int64UpDownCounter("rindb.wal.records"))
	walBytesCounter = must(walMeter.Int64UpDownCounter("rindb.wal.bytes"))

	cacheHits = must(tableCacheMeter.Int64Counter("rindb.tablecache.hits"))
	cacheMisses = must(tableCacheMeter.Int64Counter("rindb.tablecache.misses"))
	cacheOpens = must(tableCacheMeter.Int64Counter("rindb.tablecache.opens"))
	cacheCloses = must(tableCacheMeter.Int64Counter("rindb.tablecache.closes"))
	cacheEvicts = must(tableCacheMeter.Int64Counter("rindb.tablecache.evicts"))
	cachePromotions = must(tableCacheMeter.Int64Counter("rindb.tablecache.promotions"))

	getCalls = must(rindbMeter.Int64Counter("rindb.get.calls"))
	putCalls = must(rindbMeter.Int64Counter("rindb.put.calls"))
	removeCalls = must(rindbMeter.Int64Counter("rindb.remove.calls"))
	iRangeCalls = must(rindbMeter.Int64Counter("rindb.irange.calls"))
	flushCount = must(rindbMeter.Int64Counter("rindb.flush.count"))
	closeBackgroundWaitDuration = must(rindbMeter.Float64Histogram("rindb.close.background_wait.duration", metric.WithUnit("ms")))

}
