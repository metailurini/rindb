# Changelog

## [0.16.3](https://github.com/metailurini/rindb/compare/v0.16.2...v0.16.3) (2025-10-04)


### Performance Improvements

* make io load monitor recordwrite lock-free ([1e917e2](https://github.com/metailurini/rindb/commit/1e917e2a051ab638e0632943ea342da66870e9a6))

## [0.16.2](https://github.com/metailurini/rindb/compare/v0.16.1...v0.16.2) (2025-10-02)


### Bug Fixes

* Ensure zero-copy safety by returning cloned data ([a56dca7](https://github.com/metailurini/rindb/commit/a56dca737aae9bfa4c2647a54da376241cc5aae5))

## [0.16.1](https://github.com/metailurini/rindb/compare/v0.16.0...v0.16.1) (2025-09-30)


### Bug Fixes

* reuse murmur3 base hash for bloom filter ([2e779c7](https://github.com/metailurini/rindb/commit/2e779c7bc852d8bd6e1f660a8fad5858dd8b77d0))


### Performance Improvements

* accelerate skip list level sampling ([70ad814](https://github.com/metailurini/rindb/commit/70ad814568b53e25d31558476f46b84912c3054e))
* enable zero-copy record reads ([35d42a3](https://github.com/metailurini/rindb/commit/35d42a378ddf082d25a240bea792bb01e9456c5f))

## [0.16.0](https://github.com/metailurini/rindb/compare/v0.15.2...v0.16.0) (2025-09-30)


### Features

* integrate mmap-backed sstable reads ([7a8a503](https://github.com/metailurini/rindb/commit/7a8a5038ab5c4a3768bb15fa9f6eeff6fd36d50e))


### Bug Fixes

* disable telemetry during tests ([09e5fc7](https://github.com/metailurini/rindb/commit/09e5fc778d101a26db8d0be67d3c4f1fd8c6bc0c))
* harden mmap slice bounds handling ([0bb7f46](https://github.com/metailurini/rindb/commit/0bb7f4620d75160fb7ffcd5428b9d04dadd962cf))
* improve mmap error handling and coverage ([70eff83](https://github.com/metailurini/rindb/commit/70eff83425ef78e8b22780ff97552e130cfd82f6))
* resolve benchmark workflow PR lookup ([e304f8b](https://github.com/metailurini/rindb/commit/e304f8be0a814ecbaee78924f65991ff73c12780))

## [0.15.2](https://github.com/metailurini/rindb/compare/v0.15.1...v0.15.2) (2025-09-29)


### Bug Fixes

* Ensure discarded candidate is available for RangeIterator.Prev() ([b043aa0](https://github.com/metailurini/rindb/commit/b043aa0c361e932fedeabea2151b7ef0a7febcbc))
* keep descending iter prev history consistent ([e44fbbd](https://github.com/metailurini/rindb/commit/e44fbbd7a78716812b63d0a880eff28179d88a82))
* preserve snapshot history for descending ranges ([07f27fb](https://github.com/metailurini/rindb/commit/07f27fb96bc3b85887bbb21c9e8da663fc77bc21))

## [0.15.1](https://github.com/metailurini/rindb/compare/v0.15.0...v0.15.1) (2025-09-28)


### Bug Fixes

* target master branch in benchmark workflow ([7240d33](https://github.com/metailurini/rindb/commit/7240d337733af3c84e3f25150dd7ec9f842d0072))

## [0.15.0](https://github.com/metailurini/rindb/compare/v0.14.2...v0.15.0) (2025-09-28)


### Features

* Add descending range iteration support ([18b20d9](https://github.com/metailurini/rindb/commit/18b20d909aa8a3c9ee209489f6a13122b9d5d468))
* add descending seed for sstable IRange ([533e14c](https://github.com/metailurini/rindb/commit/533e14cb7a0cac110b7791504cacc6d4f45f27cf))
* add descending skiplist and memtable cursors ([b1939f1](https://github.com/metailurini/rindb/commit/b1939f1e0d7069e9e6f1af5eb13cb07e0143a0f1))
* add range order options ([e31bd4c](https://github.com/metailurini/rindb/commit/e31bd4ca5a926d3575cd6af0388abe40b112a3e1))
* document descending range order ([ad315d7](https://github.com/metailurini/rindb/commit/ad315d7e925b27a51f3d71cda69274d2bc10d0e7))
* support descending range iteration ([686badc](https://github.com/metailurini/rindb/commit/686badce451a5bbe36f8661acaab0bb4e135a1be))


### Bug Fixes

* align iterator tail priming ([11a0d75](https://github.com/metailurini/rindb/commit/11a0d7524e5d75d62b70115341cc39e4a2adeffa))
* align range peek backward behaviour ([c4c5b19](https://github.com/metailurini/rindb/commit/c4c5b19e5e5b5a86223d7dcb19e8994838635287))
* avoid duplicate cleanup in range iterator setup ([6d97dbd](https://github.com/metailurini/rindb/commit/6d97dbd29c7cec7f6fe4bf686a026e455b37a7d1))
* collapse descending runs without rewinding entire iter ([b0fbfef](https://github.com/metailurini/rindb/commit/b0fbfefc6cc54c61cdd31667595cef59d118a426))
* Correct RangeIterator behavior for RangeDesc order ([d57bb77](https://github.com/metailurini/rindb/commit/d57bb77ff65c41d46f6a61c2fa3852ca31aae0da))
* Correctly handle tombstoned records in RangeIterator.collapseDescendingRun ([ad247c4](https://github.com/metailurini/rindb/commit/ad247c49aeca6dab7e366344fcf147453befe1e7))
* ensure descending Last skips tombstoned keys ([46a2cfc](https://github.com/metailurini/rindb/commit/46a2cfcef42d165b8e0173cf8690f4abc173dbba))
* make merging iterator last non-destructive ([1cdef45](https://github.com/metailurini/rindb/commit/1cdef45c0a62c300f216bf7f6692ee39cf95dd01))
* make merging iterator Last non-destructive ([d9d078e](https://github.com/metailurini/rindb/commit/d9d078e202706fea46e4d703cf8fe6e2ec1d9df0))
* normalize table cache errors ([60350f8](https://github.com/metailurini/rindb/commit/60350f803c94e2d601ce051959abb42c7ee5215b))
* optimize iterator tail priming ([6645183](https://github.com/metailurini/rindb/commit/66451839343e6eb7ce103a6ec07e8e691334878a))
* remove unused table cache error ([460cb89](https://github.com/metailurini/rindb/commit/460cb894130f3b27d1844340e0d62ff70308d697))
* satisfy staticcheck after range iterator cleanup ([942faa8](https://github.com/metailurini/rindb/commit/942faa8bd0b64315a3cd412e9636c2a629cf831d))
* seed descending sstable ranges correctly ([32fe1d9](https://github.com/metailurini/rindb/commit/32fe1d9bd4b0c72cf428e3baa8cdf67476d8d509))
* stabilize descending iterator anchors ([edbc4b0](https://github.com/metailurini/rindb/commit/edbc4b0f748ddb728e0485371d00e9046f7ae6de))
* stabilize descending range iteration ([aa38775](https://github.com/metailurini/rindb/commit/aa387753fd3536086457b38d13dbe9fb2e63fc72))
* stabilize descending range iteration ([5bda6ef](https://github.com/metailurini/rindb/commit/5bda6efe075e2a5cfef2f0bbb0522b20982fab2d))
* stop descending range on EOF ([b12a900](https://github.com/metailurini/rindb/commit/b12a900f2e010f5734ee24ea079262e62a93796e))
* tidy reverse iteration review feedback ([b8b8f7a](https://github.com/metailurini/rindb/commit/b8b8f7a8bcecb5da5bdbaedc4165030d441bc6b1))

## [0.14.2](https://github.com/metailurini/rindb/compare/v0.14.1...v0.14.2) (2025-09-23)


### Bug Fixes

* normalize table cache errors ([86cc98a](https://github.com/metailurini/rindb/commit/86cc98a5568eb36b1f0ce552963b9cc79438b7fe))
* remove unused table cache error ([0f1e4c3](https://github.com/metailurini/rindb/commit/0f1e4c3a0d692edd7b49d6f05bee863d74ff9179))

## [0.14.1](https://github.com/metailurini/rindb/compare/v0.14.0...v0.14.1) (2025-09-20)


### Bug Fixes

* Prevent duplicate surfacing of records at direction change ([6e9d8b0](https://github.com/metailurini/rindb/commit/6e9d8b014946f234ad9fc6dbc08bba7a30fddadc))
* simplify crossing anchor helpers ([461d3d5](https://github.com/metailurini/rindb/commit/461d3d5d3c008154360d493792c2386e12340aa7))

## [0.14.0](https://github.com/metailurini/rindb/compare/v0.13.0...v0.14.0) (2025-09-20)


### Features

* add iter walk flag to diffharness ([f5ccec6](https://github.com/metailurini/rindb/commit/f5ccec6ea29d6cee5c19abaf74aa7d82c152f11e))
* add iterator engine interface ([73dcee0](https://github.com/metailurini/rindb/commit/73dcee02f2740861354e734af16cbda516365f58))
* Make HasNext/HasPrev idempotent with prefetching ([c00e848](https://github.com/metailurini/rindb/commit/c00e848e439df61549ec79485b1e4c69cdd3263d))


### Bug Fixes

* Adjust RangeWithSeq boundary in SQLite oracle ([9aa947c](https://github.com/metailurini/rindb/commit/9aa947cd61a8100eea75423b51f13c47f4984d55))
* combine prev rewind cases ([ae6b375](https://github.com/metailurini/rindb/commit/ae6b375be9f9f4adbdc988efa475940610369f55))
* Correct iterator state management ([05e3b1a](https://github.com/metailurini/rindb/commit/05e3b1af73f62df28b2c3fa498699c1a07c65b79))
* Improve error handling for sstable iteration ([1c79926](https://github.com/metailurini/rindb/commit/1c79926878526c505f698cd451caf4159064394c))
* Improve error handling in MergingIterator ([8a0a4cb](https://github.com/metailurini/rindb/commit/8a0a4cbe9598458c0248413b289a5b0997c7d043))
* requeue record after prev hits eoi ([7ea9038](https://github.com/metailurini/rindb/commit/7ea9038c65d15afcde43b2749c0f0dcc484b196c))
* retain snapshot-visible versions during cleanup ([f2e3ab2](https://github.com/metailurini/rindb/commit/f2e3ab2b46b9e7916f0c47d54b31f06beaf7145d))
* return latest version when iterating backwards ([58b722d](https://github.com/metailurini/rindb/commit/58b722dab0a085ef69f0562842deb13c418bac5d))

## [0.13.0](https://github.com/metailurini/rindb/compare/v0.12.1...v0.13.0) (2025-09-14)


### Features

* Add support for reverse range scanning ([7948545](https://github.com/metailurini/rindb/commit/7948545355be7566b841f2cda39f1dbb89bee1b0))
* Introduce testing conventions and validation tools ([f9dce71](https://github.com/metailurini/rindb/commit/f9dce715251f7252a2f73abb90f457d64a585741))


### Bug Fixes

* refine iterator rewind semantics ([5153670](https://github.com/metailurini/rindb/commit/51536705d18b7658a03529b0207f81f780588ffc))

## [0.12.1](https://github.com/metailurini/rindb/compare/v0.12.0...v0.12.1) (2025-09-14)


### Bug Fixes

* avoid spurious commits in diffharness ops ([bf83ce4](https://github.com/metailurini/rindb/commit/bf83ce49ba0f8ce7feb9e930269a7601bdb034dc))
* prevent key tracker leaks and nondeterministic ops ([86de030](https://github.com/metailurini/rindb/commit/86de030ff4c85d6a1fd934315737015abd396928))

## [0.12.0](https://github.com/metailurini/rindb/compare/v0.11.0...v0.12.0) (2025-09-13)


### Features

* replay diffharness operations on restart ([2b7750b](https://github.com/metailurini/rindb/commit/2b7750bc757e5f33c455075737f6a34b10cc37fd))


### Bug Fixes

* harden diffharness replay ([2524b9d](https://github.com/metailurini/rindb/commit/2524b9da034b1553758b2e36a83a619991073b5d))

## [0.11.0](https://github.com/metailurini/rindb/compare/v0.10.0...v0.11.0) (2025-09-12)


### Features

* add sqlite oracle for fuzzing harness ([d450906](https://github.com/metailurini/rindb/commit/d4509065e214e07a32bc6291a9aa8111c1bdf9c1))


### Bug Fixes

* avoid truncated range checks and advance offset reader ([57832ea](https://github.com/metailurini/rindb/commit/57832ea7ff322ee662c64dd9ff5e9968e18958c4))
* Correct offset reader behavior on read errors ([86eb2bc](https://github.com/metailurini/rindb/commit/86eb2bc85ff2cdd2863aab7bcb4afed405962a73))
* Improve internal key decoding ([da48f61](https://github.com/metailurini/rindb/commit/da48f6183db076a2020e84b7e4835a55f822c551))
* prevent sstable reads past data section ([0c5e630](https://github.com/metailurini/rindb/commit/0c5e6305d84227e37d7a202a96f361badb39c984))

## [0.10.0](https://github.com/metailurini/rindb/compare/v0.9.0...v0.10.0) (2025-09-10)


### Features

* write index metadata to footer ([d027902](https://github.com/metailurini/rindb/commit/d027902d581026da4cf3b466d8ca5925baace39d))


### Bug Fixes

* clarify index alignment check ([1f6c801](https://github.com/metailurini/rindb/commit/1f6c801b868da578bc656a63adc4e58d60d0fbf8))
* replace footer buffers and verify padding ([85b6603](https://github.com/metailurini/rindb/commit/85b66036959747d88e22f46876784710fa611ede))
* reuse offset reader in sparse index loading ([3b56904](https://github.com/metailurini/rindb/commit/3b56904b79e63a3bf7f5930f223a9553314e3803))
* validate empty sstable index ([0151bcc](https://github.com/metailurini/rindb/commit/0151bccb8b3236691cf26617ffddee757ccb99ce))
* validate footer metadata ([566fdd0](https://github.com/metailurini/rindb/commit/566fdd0c5e20266be6b8e45e2c0010241c3bfff9))
* validate index alignment in NewSSTable ([ecd3c4b](https://github.com/metailurini/rindb/commit/ecd3c4bc861d90d58d7a489c6b5a237ed2f8dff0))

## [0.9.0](https://github.com/metailurini/rindb/compare/v0.8.1...v0.9.0) (2025-09-08)


### Features

* remove obsolete manifest files ([f52e7f8](https://github.com/metailurini/rindb/commit/f52e7f87226b079ddfd47f2d3ff207082e5e6bd6))


### Bug Fixes

* handle manifest cleanup errors ([666caa0](https://github.com/metailurini/rindb/commit/666caa016628853571740196647549a38cf30a27))

## [0.8.1](https://github.com/metailurini/rindb/compare/v0.8.0...v0.8.1) (2025-09-07)


### Bug Fixes

* Improve error handling for missing SSTables ([b7af8aa](https://github.com/metailurini/rindb/commit/b7af8aa7f6fae047db4445878023c7248a71d69b))

## [0.8.0](https://github.com/metailurini/rindb/compare/v0.7.0...v0.8.0) (2025-09-07)


### Features

* add expiry for table cache tombstones ([557d856](https://github.com/metailurini/rindb/commit/557d856902019392312aa2c7152d827973b04ce8))
* add semaphore-based FD limiter ([23b2c6c](https://github.com/metailurini/rindb/commit/23b2c6c3b2ded02b80e6bbbc8c5bccef04c5edde))
* cache sstable handles ([01dc784](https://github.com/metailurini/rindb/commit/01dc7842ecd4c6a19cfee395e9d2bc087ca8b9ab))
* expose table cache configuration and stats ([6a75b70](https://github.com/metailurini/rindb/commit/6a75b70ade80d6bee643309325c66ff22b0f3b57))
* Implement SSTable Table Cache ([dd29624](https://github.com/metailurini/rindb/commit/dd29624aeb8d62636b49a78a40f58f0b17abda44))


### Bug Fixes

* avoid metrics in TryRef ([93f6f26](https://github.com/metailurini/rindb/commit/93f6f26a0489668b1f596090e273ec9efd32943e))
* clear corrupt quarantine on delete ([691cc38](https://github.com/metailurini/rindb/commit/691cc38dd4d613c175fccd7c7f76e9fedcecab67))
* drop dead pinned-victim branch ([745286c](https://github.com/metailurini/rindb/commit/745286cfdbaebb1eeb79b6ddbcba251d45e90e72))
* enforce FD limiter invariants ([d5c8002](https://github.com/metailurini/rindb/commit/d5c8002a770963b24feccb7f4bfc577970c9c1e0))
* evict over-budget entries on unpin ([de3e1e8](https://github.com/metailurini/rindb/commit/de3e1e84edcd2ea5b142fa0f3f91bb52673ab6a0))
* handle pinned cache overflow ([7e833df](https://github.com/metailurini/rindb/commit/7e833dfecf6482712a2ccdf1242c57af0e4c5f32))
* keep SSTable handles cached during search ([d7322c1](https://github.com/metailurini/rindb/commit/d7322c1d82f6db969d461b5e50f554d7c1444892))
* prevent table cache Close/Unref race ([c93cd4d](https://github.com/metailurini/rindb/commit/c93cd4d696b1778ae68ccd8f067aa5bedf3220a9))
* refactor onClose CAS loop ([174a001](https://github.com/metailurini/rindb/commit/174a001ae3f2d9bb7d8a8eb72204eddcd273c095))
* release sstable handles and tighten cache shutdown ([67d4c1d](https://github.com/metailurini/rindb/commit/67d4c1ded6a8165db4c913178c68a5f41f6efce3))
* respect context cancellation in table cache ([39eac6e](https://github.com/metailurini/rindb/commit/39eac6efd111ecd3e297ce6046d94d23cee9c308))
* track segment bytes and drain busy handles ([f9011f0](https://github.com/metailurini/rindb/commit/f9011f0baf6e7220bc1f2ae96d79a714b1eaeb20))
* track TryGet misses in table cache stats ([926f7bd](https://github.com/metailurini/rindb/commit/926f7bd88b781c0dbc4f4479ed0aaf4af7a44ef6))
* unref handle in table cache TryRef ([8826d79](https://github.com/metailurini/rindb/commit/8826d796ce6342acde2a93e52a123e0d79ae53d1))
* unref table cache entries instead of releasing ([1829483](https://github.com/metailurini/rindb/commit/1829483193de1f2716ae172655bb2606cb8a2432))

## [0.7.0](https://github.com/metailurini/rindb/compare/v0.6.0...v0.7.0) (2025-09-04)


### Features

* add manifest rotation and snapshot support ([c85a5f6](https://github.com/metailurini/rindb/commit/c85a5f6867fd63f31a1ca14cb16643f9d73f4e16))
* add repair mode option and conditional level loading ([d567358](https://github.com/metailurini/rindb/commit/d567358a942b545828f9bcf4d47ee33bb744f884))
* introduce file path helpers ([4396cfc](https://github.com/metailurini/rindb/commit/4396cfc213854aea7c4d96e4e3d3c1311969e371))
* manifest integration plan ([dabd035](https://github.com/metailurini/rindb/commit/dabd035ced4d5d0e6823d4041345851a9216b7d9))
* register SSTables via manifest ([f36bad7](https://github.com/metailurini/rindb/commit/f36bad74e58798886aad131a7e760dfc186cc5f7))
* track file numbers from version edits ([02f22bf](https://github.com/metailurini/rindb/commit/02f22bf585b381b78f84654512d53c8ca0404e61))


### Bug Fixes

* clean up partial manifest rotation on error ([6535f6f](https://github.com/metailurini/rindb/commit/6535f6f5cdf78909d0e7060be2b8088412e3563f))
* delay sstable deletion until metadata is persisted ([0590edd](https://github.com/metailurini/rindb/commit/0590edd995d0251822115155abd33c3c924e0fb6))
* guard against manifest record length overflow ([9368f6c](https://github.com/metailurini/rindb/commit/9368f6ce95ccb7061698906da04d27854731769c))
* guard manifest rotation with sstable manager lock ([4d74e44](https://github.com/metailurini/rindb/commit/4d74e44c2f8f2b1e4a72d8da13579c55ac31465f))
* handle empty inputs in findOverlaps ([a73157f](https://github.com/metailurini/rindb/commit/a73157fbdc31873d22aa0ec7f6ab18c6e839bfda))
* handle WAL directory read errors ([6503657](https://github.com/metailurini/rindb/commit/65036574eec4c0f3594fde1a27feb6fa2a67e9ad))
* improve compaction metadata and tests ([73ef931](https://github.com/metailurini/rindb/commit/73ef931d615c5b7ca5ea4580fa774c25b2ee2586))
* propagate errors when opening SSTables ([ac0b08f](https://github.com/metailurini/rindb/commit/ac0b08fde06d4efdf8f5e7349ee6da5cfe94e10a))
* protect opened file map with mutex ([ddc142a](https://github.com/metailurini/rindb/commit/ddc142a9f1d9e2bb44db5910ef2a7d6a5e03f4ca))
* Provide file number allocator in RecoverVersionSet ([13e86e6](https://github.com/metailurini/rindb/commit/13e86e64b20f4abde5e71b67ef7e2c31f206dbc0))
* remove redundant tombstone check ([9e54764](https://github.com/metailurini/rindb/commit/9e54764f80b47f2b51dbe683b18205795c11a302))
* restore telemetry and concurrency safety ([7e5d193](https://github.com/metailurini/rindb/commit/7e5d193bb18c078262a95b9b31b6bea0b4f04eee))
* retry search on missing sstable ([d320319](https://github.com/metailurini/rindb/commit/d3203196989cd9ac9231060cedfbdbb9c01a47b4))
* reuse existing WAL on startup ([99964d2](https://github.com/metailurini/rindb/commit/99964d23bf5fbe5848fbf9a71745e4d631351e09))
* tidy CURRENT temp file and optimize deletions ([c94fa3f](https://github.com/metailurini/rindb/commit/c94fa3fd48955efe2ad164c69009fedbde4ead2e))
* update compaction metadata and snapshot cleanup ([c4fdbff](https://github.com/metailurini/rindb/commit/c4fdbff38ad13b626b51fde67cb8687d90d56276))
* Use FileSystem accessors for writing ([0e2232a](https://github.com/metailurini/rindb/commit/0e2232a1ad34a9cd2143a0bcbc69aff2a9619035))
* validate AddSSTable sequence and log close errors ([c13a2e9](https://github.com/metailurini/rindb/commit/c13a2e91fe84b7f10fdd6c805bab782b36d32363))
* validate versionset and test repair mode scanning ([a467924](https://github.com/metailurini/rindb/commit/a467924776658d6b9dac3e0109324ae8268b52e6))


### Performance Improvements

* reuse manifest writer buffer ([c59c50c](https://github.com/metailurini/rindb/commit/c59c50cc7849fefe42eaf45bd79ee37d5e12739b))
* rotate manifest in background ([49a39c0](https://github.com/metailurini/rindb/commit/49a39c0c052392c7915664c023f2e8951a331812))

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
