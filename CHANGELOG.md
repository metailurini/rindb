# Changelog

## [0.4.0](https://github.com/metailurini/rindb/compare/v0.3.0...v0.4.0) (2025-08-19)


### Features

* Enhance README with new features and build instructions ([843b702](https://github.com/metailurini/rindb/commit/843b702b3a087619931de4fe06fb4efa1298e2c5))

## [0.3.0](https://github.com/metailurini/rindb/compare/v0.2.1...v0.3.0) (2025-08-19)


### Features

* Add RangeIterator tests ([d6cef1c](https://github.com/metailurini/rindb/commit/d6cef1c027f486de8d84ddfb08704225e9752a9a))
* Add sequence number to records ([0e9c16e](https://github.com/metailurini/rindb/commit/0e9c16ef615cf42fb3306ce1c3becfa51759a592))
* Add SequenceNumber to RecordImpl ([f7ab319](https://github.com/metailurini/rindb/commit/f7ab319cdc9f0d4f89eaffe0f33424ec8f828774))
* Add telemetry sampling rate configuration ([fadc14c](https://github.com/metailurini/rindb/commit/fadc14c3e80152ec6bf0fbe720934cea73a53f52))
* Complete range query support and dynamic compaction triggers ([d6a8d5c](https://github.com/metailurini/rindb/commit/d6a8d5c6a7a6832e339da8505c07194bc9bcaa93))
* Extract Sequence Number Calculation ([64b8080](https://github.com/metailurini/rindb/commit/64b8080928ec3b910ac786a7e7e8b3d44bbb5b84))
* Implement PushFront for LinkedList and enhance SSTableManager ([215bcb1](https://github.com/metailurini/rindb/commit/215bcb171293059d9b1d498e7c6bd82da127cfb5))
* Introduce functional options for WAL and SSTable initialization ([7f1c301](https://github.com/metailurini/rindb/commit/7f1c301e9cd491399b46f36f41e202fb5406c8c8))
* Update roadmap for range queries and observability ([ecda286](https://github.com/metailurini/rindb/commit/ecda286a4de4228169e4d642ddfc084865cdbbfd))


### Bug Fixes

* avoid nested lock in stats and improve CLI output ([434237f](https://github.com/metailurini/rindb/commit/434237fd359cef866d65608144454343fdb020e5))
* Correctly link new node in PushFront ([6aca202](https://github.com/metailurini/rindb/commit/6aca202173537337cb84c2071f37927fc17d2310))
* ensure OpenTelemetry providers shut down ([0cbd693](https://github.com/metailurini/rindb/commit/0cbd693f1f17a2dfcff3b1a96b37fa374170c47d))
* harden io load sampling ([1bf3383](https://github.com/metailurini/rindb/commit/1bf3383a764d30bae62d2936d5e7bae80c927d88))
* Improve error handling and parsing in SSTable management ([df8e63e](https://github.com/metailurini/rindb/commit/df8e63e956f527bffb61aed635e3dc0eb1483a20))
* Optimize sparse index lookup ([476203f](https://github.com/metailurini/rindb/commit/476203f0950326301bb59ce5c31b1c037cc11563))
* Panic on empty memtable flush ([46e82c9](https://github.com/metailurini/rindb/commit/46e82c97c18b23b5587ba4e6a02131f5645a51bc))
* prevent duplicate span name diagnostics ([994611c](https://github.com/metailurini/rindb/commit/994611c56a5e37ffd7a965795acdb2c09b630222))
* propagate iterator errors in range scans ([329cb1f](https://github.com/metailurini/rindb/commit/329cb1f8ec8a750b10d750b47c5449a0e8905f46))
* Standardize record creation ([491a7a5](https://github.com/metailurini/rindb/commit/491a7a5d4879be49ba73f7d4cdcd51f1f6be5e36))
* stop I/O sampler with wait group ([7c852b9](https://github.com/metailurini/rindb/commit/7c852b90a3bfe53e0cb7fa2d8f4936d68f41babc))
* use INFO logger for otel init ([d7a0954](https://github.com/metailurini/rindb/commit/d7a09540fce30f36f9cd5c895c1ea47a0278d2d7))
