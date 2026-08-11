# kotobase-engine-lsm

Independent Merkle-LSM implementation of `kotobase-engine-contract/IEngine`.
It depends on the pure `merkle-lsm` mechanism, not on `kotobase-peer`, Prolly,
Arrangement, S3 or a deployment runtime.

Each transaction appends immutable covering-index L0 runs and writes a bounded
engine manifest above the Merkle-LSM VersionManifest. Format v2 stores history
and idempotency deltas in CID-linked segments whose payloads always cross the
required `encrypt-fn`/`decrypt-fn` boundary; cumulative plaintext EDN maps are
no longer copied into every root. Reads apply MVCC visibility
at the requested epoch. The transaction path deterministically compacts an L0
index when `:l0-compaction-threshold` (default 8) is reached. Hosts may inject
`:reader-pins-fn`; the engine computes a monotonic safe epoch from active query,
replica and legal-hold pins, persists it in both manifests, and rejects snapshot
opens below the retained boundary. With no pins the conservative default stays
at epoch zero and preserves every historical MVCC version.

With `{:lazy? true}`, restore leaves run blocks cold after reading the engine
manifest, Merkle-LSM VersionManifest and sealed metadata chain. Point scans
choose a covering index from the query
pattern and use each run ref's first-component range to load only candidates;
older refs without range metadata remain correctness-safe by being retained.
The JVM regression resolves one entity from 13 EAVT runs with three run GETs.

The ClojureScript coordinator keeps provider I/O asynchronous while the engine
mechanism stays deterministic. Restore fetches the bounded roots and metadata
segments; point scans batch-fetch range-selected run nodes and children. Cold
writes hydrate existing runs asynchronously, upload every immutable block, and
only then expose the new manifest through linearizable CAS. Metadata-chain read
amplification remains a qualification blocker; it is not hidden behind the
bounded-root claim.

```sh
clojure -M:test
clojure -M:lint
npm run test:cljs
```
