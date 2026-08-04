# kotobase-engine-lsm

Independent Merkle-LSM implementation of `kotobase-engine-contract/IEngine`.
It depends on the pure `merkle-lsm` mechanism, not on `kotobase-peer`, Prolly,
Arrangement, S3 or a deployment runtime.

Each transaction appends immutable covering-index L0 runs and writes a small
engine manifest above the Merkle-LSM VersionManifest. Reads apply MVCC visibility
at the requested epoch. The transaction path deterministically compacts an L0
index when `:l0-compaction-threshold` (default 8) is reached. Compaction merges
L0 with only overlapping L1 ranges and structurally shares untouched L1 runs;
it does not rewrite the complete database at every threshold. Hosts may inject
`:reader-pins-fn`; the engine computes a monotonic safe epoch from active query,
replica and legal-hold pins, persists it in both manifests, and rejects snapshot
opens below the retained boundary. With no pins the conservative default stays
at epoch zero and preserves every historical MVCC version.

Latency-sensitive hosts can set `:inline-compaction? false`. Transactions then
append L0 runs without doing merge work in the request path. The host checks
`compaction-due?` and calls `compact-state`, or uses the asynchronous provider's
`compact-and-publish!`; maintenance writes a new physical root under head CAS
without advancing the logical epoch. Inline compaction remains the default for
compatibility. A deferred host must schedule maintenance promptly so L0 fan-in
does not grow without bound.

With `{:lazy? true}`, restore reads only the engine manifest and the
Merkle-LSM VersionManifest. Point scans choose a covering index from the query
pattern and use each run ref's first-component range to load only candidates;
older refs without range metadata remain correctness-safe by being retained.
The JVM regression resolves one entity from 13 EAVT runs with three run GETs.

The ClojureScript coordinator keeps provider I/O asynchronous while the engine
mechanism stays deterministic. Restore fetches only the engine and LSM
manifests; point scans batch-fetch range-selected run nodes and children. Cold
writes hydrate existing runs asynchronously, upload every immutable block, and
only then expose the new manifest through linearizable CAS.

```sh
clojure -M:test
clojure -M:lint
npm run test:cljs
```
