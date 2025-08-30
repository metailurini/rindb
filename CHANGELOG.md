# Changelog

## [0.6.0](https://github.com/metailurini/rindb/compare/v0.5.0...v0.6.0) (2025-08-30)


### Features

* add checksum verification ([96e3010](https://github.com/metailurini/rindb/commit/96e301046ee6a7c379886545db5b236b0ba9264f))
* add sstable builder ([da84a7f](https://github.com/metailurini/rindb/commit/da84a7fd95162432deb73fb7f77ca2217cfe6165))
* allow configuring SSTable builder size ([900644c](https://github.com/metailurini/rindb/commit/900644cf10c02efbfb3393a17bbb18d9f93f33aa))


### Bug Fixes

* clean file on build error ([eb5f2d8](https://github.com/metailurini/rindb/commit/eb5f2d86154c62b72f2f542e112e07c5539dd82b))
* clean SSTable file before building ([6594466](https://github.com/metailurini/rindb/commit/65944668443b001ed519d697777b33645e5c457e))
* enforce strict key ordering in SSTableBuilder ([50715c8](https://github.com/metailurini/rindb/commit/50715c8007615d7ef805e6e8996a5e03e4b6d9f2))
* expose builder output size ([f1c460a](https://github.com/metailurini/rindb/commit/f1c460a4e13a7839c72f806fc3676b3175dd65c8))
* handle empty SSTable bloom filter ([e7755f2](https://github.com/metailurini/rindb/commit/e7755f20b090c3c1c03db4bd9ccbe30fb68d47d8))
* keep bloom filter in memory ([55c5a32](https://github.com/metailurini/rindb/commit/55c5a329c8a828143040825b6e80fb73feb3d7bc))
* log cleanup failures and tidy write error test ([e83c067](https://github.com/metailurini/rindb/commit/e83c0675cfdb69963faafb331a0c3332c4faaf13))
* prevent SSTableBuilder reuse after build ([4bff650](https://github.com/metailurini/rindb/commit/4bff650aa2adf3097bb67bc546ac6c255436747e))
* restore snapshot-aware compaction ([71d7a91](https://github.com/metailurini/rindb/commit/71d7a91867bf7f6192eeb4cc853c986195081400))
* retain snapshot-visible versions ([4b63caf](https://github.com/metailurini/rindb/commit/4b63caf07afdf854d4cdb258bcd1cfa5a5df82b8))
* return error on empty SSTable build ([dabbb88](https://github.com/metailurini/rindb/commit/dabbb88d1d9eba6e6e28ba4f6d094946810232f2))
* sync filesystem after SSTable commit ([401619e](https://github.com/metailurini/rindb/commit/401619e2dbef753208395a88f495ef2d84339bc3))


### Performance Improvements

* compute checksums without extra allocations ([abe32d0](https://github.com/metailurini/rindb/commit/abe32d0e3d0bf2c322f9da77f0c7bb7b4a9019e7))

## [0.5.0](https://github.com/metailurini/rindb/compare/v0.4.0...v0.5.0) (2025-08-29)


### Features

* Add Bytes.Clone method ([50feb4d](https://github.com/metailurini/rindb/commit/50feb4db9a751ef9b18f8ef79f3805e63a1ff5f0))
* add sequence filtering to reads ([2f36dd1](https://github.com/metailurini/rindb/commit/2f36dd161c9bd607e86c4fd3962ec8129c2c7d8e))
* add snapshot release method ([be69f66](https://github.com/metailurini/rindb/commit/be69f66802fbfa1e209b6dce0041efdc63e4d0e6))
* add snapshot support ([0ce1e2e](https://github.com/metailurini/rindb/commit/0ce1e2e08445651488645738a5e53fba8fca89a0))
* add snapshot-aware cleanup ([b73bcb8](https://github.com/metailurini/rindb/commit/b73bcb8211d0f63385491323b953b599e78bb69d))
* add versioned memtable with internal keys ([f011fb1](https://github.com/metailurini/rindb/commit/f011fb13f990e2b8227649362cdba607adaf7095))
* honor snapshots during SSTable merge ([f6da10f](https://github.com/metailurini/rindb/commit/f6da10fcbd58bb87a31573b4371963784287d78b))
* Introduce mergeSSTablesV2 for optimized merging ([af63c70](https://github.com/metailurini/rindb/commit/af63c701b61859b8827e076c20fb46d0971ad682))
* introduce merging iterator ([1f03ae6](https://github.com/metailurini/rindb/commit/1f03ae65c5893152b59369a4d6ca5dd160b438a7))


### Bug Fixes

* aggregate merge iterator close errors ([9d26330](https://github.com/metailurini/rindb/commit/9d2633092ae4894fedc56121efe441ef579b2f2d))
* avoid unused context in snapshot methods ([6d3f85b](https://github.com/metailurini/rindb/commit/6d3f85b7342e365d2be8b4b8e147b0520ead5838))
* Correctly handle tombstones in RangeIterator ([23a1a86](https://github.com/metailurini/rindb/commit/23a1a86a67388d100c214ba4530a7ea444459034))
* ensure consistent snapshot cleanup ([e93f8e1](https://github.com/metailurini/rindb/commit/e93f8e1fb4f500aca9bc0d7e6e8ad2e3d5068e29))
* ensure merging iterator closed on error ([3a67d14](https://github.com/metailurini/rindb/commit/3a67d142028641f0854d2e1b54c8a2f88aa39ab0))
* export Go toolchain ([24cd89a](https://github.com/metailurini/rindb/commit/24cd89ab4e41bb41dc30782d090e5d558796aeb6))
* guard file operations against concurrent access ([d19b3f3](https://github.com/metailurini/rindb/commit/d19b3f3563121128fd7cc570ee6782fb3b528bb6))
* guard higher-level compaction removal and add tests ([b03ee17](https://github.com/metailurini/rindb/commit/b03ee178484142cdb271b7119a201a1c65e7da87))
* guard sstable reads with filesystem helpers ([042cac3](https://github.com/metailurini/rindb/commit/042cac3264464313f2d38a8c9b0c8a23222ef7bd))
* handle empty sources and cleanup on overlap errors ([7840273](https://github.com/metailurini/rindb/commit/78402736277145e9503cd2dc4863c685d44e2085))
* Improve sstable tail offset calculation ([ca097f7](https://github.com/metailurini/rindb/commit/ca097f7cc69b1c1f4a0ec245b6a94a8798079095))
* init WAL metrics on load ([bd27f8a](https://github.com/metailurini/rindb/commit/bd27f8a0f676d28029749c3f193e8e7d198f31c1))
* make Snapshot.Release thread-safe ([cc1a658](https://github.com/metailurini/rindb/commit/cc1a6586e3c6d8b3565de3310ea99dd21d00f9a9))
* refine internal key ordering and range bounds ([41f72ee](https://github.com/metailurini/rindb/commit/41f72eef4debd4c414766808fc75ecddf9189f5f))
* remove overlapping SSTables from level1 list ([58b8bf9](https://github.com/metailurini/rindb/commit/58b8bf94455b3f47335337d6d821f9e093ccc6f8))
* retain latest memtable entries during cleanup ([db96aba](https://github.com/metailurini/rindb/commit/db96aba497b60ae818d597a0012090642bf8aa2e))
* set Go toolchain in Makefile ([1c7eb58](https://github.com/metailurini/rindb/commit/1c7eb5892a4efb09ae59807146270d630aaf6703))
* track and cleanup opened sstables ([d01d557](https://github.com/metailurini/rindb/commit/d01d557d449271c33b0934e9c3490eaa6491e7c9))
* track WAL metrics as net values ([9da852c](https://github.com/metailurini/rindb/commit/9da852c409681463d6f78d6c2265018f37d2071f))


### Performance Improvements

* minimize snapshot min sequence updates ([c7551d4](https://github.com/metailurini/rindb/commit/c7551d4441c778e9251d3721bbae257666bb879a))
* optimize snapshot release with binary search ([618b8aa](https://github.com/metailurini/rindb/commit/618b8aac82ae73256d131858cd1aebbbb9d91d68))

## [0.4.0](https://github.com/metailurini/rindb/compare/v0.3.0...v0.4.0) (2025-08-24)


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
