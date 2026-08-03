# kotobase-engine-lsm

Independent Merkle-LSM implementation of `kotobase-engine-contract/IEngine`.
It depends on the pure `merkle-lsm` mechanism, not on `kotobase-peer`, Prolly,
Arrangement, S3 or a deployment runtime.

Each transaction appends immutable covering-index L0 runs and writes a small
engine manifest above the Merkle-LSM VersionManifest. Reads apply MVCC visibility
at the requested epoch. The transaction path deterministically compacts an L0
index when `:l0-compaction-threshold` (default 8) is reached. Hosts may inject
`:reader-pins-fn`; the engine computes a monotonic safe epoch from active query,
replica and legal-hold pins, persists it in both manifests, and rejects snapshot
opens below the retained boundary. With no pins the conservative default stays
at epoch zero and preserves every historical MVCC version.

With `{:lazy? true}`, restore reads only the engine manifest and the
Merkle-LSM VersionManifest. Point scans choose a covering index from the query
pattern and use each run ref's first-component range to load only candidates;
older refs without range metadata remain correctness-safe by being retained.
The JVM regression resolves one entity from 13 EAVT runs with three run GETs.

An asynchronous CLJS/provider coordinator remains a separate qualification
step; the range selection and lazy state shape no longer require full restore.

```sh
clojure -M:test
clojure -M:lint
```
