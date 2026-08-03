# kotobase-engine-lsm

Independent Merkle-LSM implementation of `kotobase-engine-contract/IEngine`.
It depends on the pure `merkle-lsm` mechanism, not on `kotobase-peer`, Prolly,
Arrangement, S3 or a deployment runtime.

Each transaction appends immutable covering-index L0 runs and writes a small
engine manifest above the Merkle-LSM VersionManifest. Reads apply MVCC visibility
at the requested epoch. The transaction path deterministically compacts an L0
index when `:l0-compaction-threshold` (default 8) is reached. It uses safe epoch
zero, so compaction bounds read fan-in without discarding any historical MVCC
version. Advancing the safe epoch remains the responsibility of a future
reader-pin/retention policy.

This JVM qualification path still loads referenced runs during restore.
Range-pruned async provider reads remain a separate qualification step.

```sh
clojure -M:test
clojure -M:lint
```
