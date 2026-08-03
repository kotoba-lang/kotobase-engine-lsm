# kotobase-engine-lsm

Independent Merkle-LSM implementation of `kotobase-engine-contract/IEngine`.
It depends on the pure `merkle-lsm` mechanism, not on `kotobase-peer`, Prolly,
Arrangement, S3 or a deployment runtime.

Each transaction appends immutable covering-index L0 runs and writes a small
engine manifest above the Merkle-LSM VersionManifest. Reads apply MVCC visibility
at the requested epoch. This initial JVM qualification path loads referenced
runs during restore; range-pruned async provider reads and compaction scheduling
remain separate qualification steps.

```sh
clojure -M:test
clojure -M:lint
```

