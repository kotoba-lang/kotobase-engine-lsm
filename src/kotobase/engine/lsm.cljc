(ns kotobase.engine.lsm
  "Datomic-shaped engine over immutable Merkle-LSM runs."
  (:require [clojure.edn :as edn]
            [ipld.core :as ipld]
            [kotobase.engine.canonical :as canonical]
            [kotobase.engine.contract :as contract]
            [kotobase.engine.profile :as profile]
            [merkle-lsm.compaction :as compaction]
            [merkle-lsm.core :as lsm]))

(def engine-format-version 2)

(def lsm-profile
  (-> profile/merkle-lsm
      (assoc :engine/implementation :kotobase.engine/lsm)
      (update :engine/capabilities conj :typed-edn :durable-manifest
              :append-only-runs)))

(defn- digest [engine value]
  ((:digest-fn engine) (canonical/canonical-string value)))

(defn- encode-component [value]
  (canonical/canonical-string value))

(defn- decode-component [value]
  (canonical/restore-canonical-value (edn/read-string value)))

(defn- encode-datom [{:keys [e a v op]}]
  {:e (encode-component e) :a (encode-component a)
   :v (encode-component v) :op op})

(defn- persist-effects! [put! effects]
  (doseq [{:keys [effect/type cid bytes]} effects
          :when (= :block/put type)]
    (put! cid bytes)))

(defn- run-ref-of [run]
  (or (:ref run) (lsm/run-ref run)))

(defn- level-refs-of [levels-by-index]
  (into {}
        (map (fn [[index levels]]
               [index (into {}
                            (map (fn [[level values]]
                                   [level (mapv run-ref-of values)]))
                            levels)]))
        levels-by-index))

(defn- flatten-levels [levels-by-index]
  (into {}
        (map (fn [[index levels]]
               [index (vec (mapcat val (sort-by key levels)))]))
        levels-by-index))

(defn- compact-levels-if-needed
  "Move due L0 runs into overlapping L1 ranges without rewriting untouched L1."
  [put! database-id safe-epoch target-run-rows threshold levels-by-index]
  (into {}
        (map (fn [[index levels]]
               (let [l0 (vec (:l0 levels))
                     l1 (vec (:l1 levels))]
                 (if (< (count l0) threshold)
                   [index levels]
                   (let [physical-index (if (= :request index) :eavt index)
                         tenant (if (= :request index)
                                  (str database-id "/requests") database-id)
                         result (compaction/compact-with-level
                                 physical-index tenant safe-epoch target-run-rows
                                 l0 l1)]
                     (doseq [run (:compacted result)]
                     (persist-effects! put! (:effects run)))
                     [index (assoc levels :l0 [] :l1 (:all-output result))])))))
        levels-by-index))

(defn- manifest-node [state lsm-root current-request]
  {"engine" "kotobase-engine-lsm"
   "format-version" engine-format-version
   "database-id" (:database-id state)
   "basis-t" (:basis-t state)
   "safe-epoch" (:safe-epoch state 0)
   "lsm-manifest" (ipld/link lsm-root)
   "current-request-edn" (pr-str current-request)})

(defn- put-lsm-manifest!
  "Persist the engine's run directory. The upstream Merkle-LSM manifest
  validator intentionally knows only graph indexes; this engine adds one
  physically-EAVT metadata directory named request, so the envelope owns the
  small extension while retaining the same DAG-CBOR shape."
  [put! database-id epoch safe-epoch indexes]
  (ipld/put-node!
   put!
   (cond-> {"format" "kotobase/version-manifest"
            "version" 1
            "db-id" (str database-id)
            "epoch" epoch
            "safe-epoch" safe-epoch
            "indexes"
            (into (sorted-map)
                  (map (fn [[index levels]]
                         [(name index)
                          (into (sorted-map)
                                (keep (fn [[level refs]]
                                        (when (seq refs)
                                          [(name level) (vec refs)])))
                                levels)]))
                  indexes)
            "statistics" {}})))

(defn- read-field [node key]
  (edn/read-string (get node key)))

(defn- load-run [get-fn ref]
  (let [cid (ipld/link-cid (get ref "cid"))
        node (ipld/decode (get-fn cid))
        rows (or (get node "rows")
                 (mapcat (fn [descriptor]
                           (let [block-cid (ipld/link-cid (get descriptor "cid"))]
                             (get (ipld/decode (get-fn block-cid)) "rows")))
                         (get node "blocks")))]
    {:cid cid :node node :rows (vec rows) :ref ref
     :index (keyword (get node "index"))
     :count (get node "count")}))

(defn- manifest-level-refs [manifest]
  (into {}
        (keep (fn [[index levels]]
                (let [refs (into {}
                                 (keep (fn [[level values]]
                                         (when (seq values)
                                           [(keyword level) (vec values)])))
                                 levels)]
                  (when (seq refs) [(keyword index) refs]))))
        (get manifest "indexes")))

(defn- load-level-refs [get-fn level-refs]
  (into {}
        (map (fn [[index levels]]
               [index (into {}
                            (map (fn [[level refs]]
                                   [level (mapv #(load-run get-fn %) refs)]))
                            levels)]))
        level-refs))

(defn- ordered->eav [index components]
  (case index
    :eavt components
    :aevt [(nth components 1) (nth components 0) (nth components 2)]
    :avet [(nth components 2) (nth components 0) (nth components 1)]
    :vaet [(nth components 2) (nth components 1) (nth components 0)]))

(defn- logical-rows
  ([runs basis-t] (logical-rows runs :eavt basis-t))
  ([runs index basis-t]
   (->> (lsm/visible-rows (get runs index []) basis-t)
       (map (fn [row]
              (let [[e a v] (ordered->eav index (get row "components"))]
                {:e (decode-component e) :a (decode-component a)
                 :v (decode-component v) :t (get row "epoch")
                 :added true})))
       canonical/canonical-datoms)))

(defn- history-rows
  "Derive transaction history from the EAVT MVCC rows instead of duplicating
  it in every engine manifest. Compaction retains exactly the history that is
  queryable at or after safe-epoch."
  [runs basis-t]
  (->> (get runs :eavt [])
       (mapcat #(or (:rows %) (get-in % [:node "rows"])))
       distinct
       (filter #(<= (get % "epoch") basis-t))
       (map (fn [row]
              (let [[e a v] (get row "components")]
                {:e (decode-component e) :a (decode-component a)
                 :v (decode-component v) :t (get row "epoch")
                 :added (= "assert" (get row "op"))})))
       canonical/canonical-datoms))

(defn- request-entry [request-id tx-root epoch]
  {:components [(encode-component request-id)]
   :epoch epoch :op :assert
   :value (encode-component {:request-id request-id
                             :tx-root tx-root :epoch epoch})})

(defn- request-record-from-runs [runs request-id basis-t]
  (some->> (lsm/visible-rows (get runs :request []) basis-t)
           (some (fn [row]
                   (when (= [(encode-component request-id)]
                            (get row "components"))
                     (decode-component (get row "value")))))))

(defn request-record
  "Return REQUEST-ID's durable idempotency record from the metadata LSM.
  Providers may hydrate only the covering request runs before calling this."
  [engine state request-id]
  (or (get-in state [:request-cache request-id])
      (get-in state [:legacy-requests request-id])
      (let [runs (or (:runs state)
                     {:request
                      (mapv #(load-run (:get-fn engine) %)
                            (lsm/select-run-refs-by-first-component
                             (get (:run-refs state) :request [])
                             (encode-component request-id)))})]
        (request-record-from-runs runs request-id (:basis-t state)))))

(defn request-records
  "All durable request records, ordered by epoch. Intended for metadata/log
  APIs; normal transactions use request-record's point lookup."
  [engine state]
  (let [runs (or (:runs state)
                 {:request (mapv #(load-run (:get-fn engine) %)
                                 (get (:run-refs state) :request []))})]
    (->> (lsm/visible-rows (get runs :request []) (:basis-t state))
         (map #(decode-component (get % "value")))
         (concat (vals (:legacy-requests state)))
         (sort-by :epoch)
         vec)))

(defn scan-index
  "Choose a covering index and encoded first-component prefix for PATTERN."
  [pattern]
  (let [[e a v] pattern]
    (cond
      (some? e) [:eavt (encode-component e)]
      (and (some? a) (some? v)) [:avet (encode-component a)]
      (some? a) [:aevt (encode-component a)]
      :else [:eavt ""])))

(defn scan-run-refs
  "Select only run refs whose first-component range can cover PATTERN."
  [refs-by-index pattern]
  (let [[index prefix] (scan-index pattern)]
    [index (lsm/select-run-refs-by-first-component
            (get refs-by-index index []) prefix)]))

(defn run-node-child-cids
  "Return child block CIDs referenced by a decoded run node."
  [node]
  (mapv (comp ipld/link-cid #(get % "cid")) (get node "blocks" [])))

(defn load-run-cached
  "Load a run whose root and child blocks are already in ENGINE's cache."
  [engine ref]
  (load-run (:get-fn engine) ref))

(defn request-run-refs
  "Select request-index refs that can contain REQUEST-ID."
  [state request-id]
  (lsm/select-run-refs-by-first-component
   (get (:run-refs state) :request []) (encode-component request-id)))

(defn- lazy-logical-rows [get-fn refs-by-index basis-t pattern]
  (let [[index selected] (scan-run-refs refs-by-index pattern)]
    (logical-rows {index (mapv #(load-run get-fn %) selected)}
                  index basis-t)))

(defn- matches? [[pe pa pv] {:keys [e a v]}]
  (and (or (nil? pe) (= pe e))
       (or (nil? pa) (= pa a))
       (or (nil? pv) (= pv v))))

(defn- next-safe-epoch [reader-pins-fn state next-epoch]
  (let [current (:safe-epoch state 0)
        pins (vec (reader-pins-fn))
        candidate (compaction/minimum-safe-epoch pins)]
    (when-not (and (integer? candidate) (<= 0 candidate next-epoch))
      (throw (ex-info "reader pins produced an invalid safe epoch"
                      {:type :kotobase.engine/invalid-safe-epoch
                       :safe-epoch candidate :next-epoch next-epoch
                       :pins pins})))
    (max current candidate)))

(defrecord MerkleLsmEngine [put! get-fn digest-fn target-run-rows
                            l0-compaction-threshold reader-pins-fn
                            inline-compaction?]
  contract/IEngine
  (-engine-profile [_] lsm-profile)
  (-empty-state [_ {:keys [database-id]}]
     {:database-id database-id :basis-t 0 :safe-epoch 0
     :levels {} :level-refs {} :runs {} :run-refs {}})

  (-restore-state [_ physical-root opts]
    (let [node (ipld/decode (get-fn physical-root))
          format-version (get node "format-version")
          _ (when-not (and (= "kotobase-engine-lsm" (get node "engine"))
                           (contains? #{1 engine-format-version} format-version))
              (throw (ex-info "unsupported LSM engine manifest"
                              {:type :kotobase.engine/unsupported-manifest})))
          basis-t (get node "basis-t")
          lsm-root (ipld/link-cid (get node "lsm-manifest"))
          lsm-manifest (ipld/decode (get-fn lsm-root))
          level-refs (manifest-level-refs lsm-manifest)
          refs-by-index (flatten-levels level-refs)
          levels (when-not (:lazy? opts) (load-level-refs get-fn level-refs))
          current (read-field node "current-request-edn")
          legacy? (= 1 format-version)
          legacy-requests (when legacy?
                            (cond-> (read-field node "requests-edn")
                              current (assoc (:request-id current) current)))]
      (cond->
       {:database-id (get node "database-id") :basis-t basis-t
       :safe-epoch (get node "safe-epoch" 0)
       :levels levels :level-refs level-refs
       :runs (when levels (flatten-levels levels))
       :run-refs refs-by-index :lsm-root lsm-root
       :current-request (when current
                          (assoc current :physical-root physical-root))
       :physical-root physical-root}
        legacy? (assoc :legacy-requests legacy-requests))))

  (-transact [this state {:keys [database-id request-id tx-data]}]
    (when-not (= database-id (:database-id state))
      (throw (ex-info "transaction database does not match state"
                      {:type :kotobase.engine/database-mismatch})))
    (let [tx (canonical/normalize-tx tx-data)
          tx-root (digest this tx)]
      (if-let [prior (request-record this state request-id)]
        (if (= tx-root (:tx-root prior))
          {:state state
           :receipt {:database-id database-id :epoch (:epoch prior)
                     :request-id request-id :tx-root tx-root
                     :physical-root (or (:physical-root prior)
                                        (:physical-root state)) :engine lsm-profile
                     :status :replayed}}
          (throw (ex-info "request-id was already used for another transaction"
                          {:type :kotobase.engine/idempotency-conflict})))
        (let [epoch (inc (:basis-t state))
              safe-epoch (next-safe-epoch reader-pins-fn state epoch)
              current-levels
              (when (or inline-compaction? (some? (:levels state)))
                (or (:levels state)
                    (when (:runs state)
                      (into {} (map (fn [[index runs]]
                                      [index {:l0 runs}]))
                            (:runs state)))
                    (load-level-refs get-fn (:level-refs state))))
              data-runs (lsm/build-index-run-ranges
                         database-id epoch target-run-rows
                         (mapv encode-datom tx))
              legacy-request-entries
              (mapv (fn [[legacy-id record]]
                      (request-entry legacy-id (:tx-root record)
                                     (:epoch record)))
                    (:legacy-requests state))
              request-runs
              (lsm/build-run-ranges
               :eavt (str database-id "/requests") target-run-rows
               (conj legacy-request-entries
                     (request-entry request-id tx-root epoch)))
              new-runs (assoc data-runs :request request-runs)
              _ (doseq [runs (vals new-runs) run runs]
                  (persist-effects! put! (:effects run)))
              appended-levels (when current-levels
                                (reduce-kv
                                 (fn [levels index runs]
                                   (update-in levels [index :l0] (fnil into []) runs))
                                 current-levels new-runs))
              all-levels (when appended-levels
                           (if inline-compaction?
                             (compact-levels-if-needed
                              put! database-id safe-epoch target-run-rows
                              l0-compaction-threshold appended-levels)
                             appended-levels))
              new-run-refs (into {} (map (fn [[index runs]]
                                           [index (mapv run-ref-of runs)]))
                                 new-runs)
              all-level-refs
              (if all-levels
                (level-refs-of all-levels)
                (reduce-kv (fn [refs index values]
                             (update-in refs [index :l0] (fnil into []) values))
                           (:level-refs state) new-run-refs))
              all-runs (when all-levels (flatten-levels all-levels))
              lsm-root (put-lsm-manifest!
                        put! database-id epoch safe-epoch all-level-refs)
              next-state (-> state (assoc :basis-t epoch
                                          :safe-epoch safe-epoch
                                          :levels all-levels
                                          :level-refs all-level-refs
                                          :runs all-runs
                                          :run-refs (flatten-levels all-level-refs)
                                          :lsm-root lsm-root)
                             (dissoc :legacy-requests :request-cache))
              current {:request-id request-id :tx-root tx-root :epoch epoch}
              physical-root (ipld/put-node! put!
                                            (manifest-node next-state lsm-root current))
              record (assoc current :physical-root physical-root)
              final-state (-> next-state
                              (assoc :physical-root physical-root)
                              (assoc :current-request record))]
          {:state final-state
           :receipt {:database-id database-id :epoch epoch
                     :request-id request-id :tx-root tx-root
                     :physical-root physical-root :engine lsm-profile
                     :status :committed}}))))

  (-open-snapshot [_ state selector]
    (let [basis (:basis-t state)
          safe-epoch (:safe-epoch state 0)
          as-of (get selector :as-of basis)]
      (when-not (and (integer? as-of) (<= safe-epoch as-of basis))
        (throw (ex-info "snapshot :as-of is outside the database basis"
                        {:type :kotobase.engine/invalid-snapshot-selector
                         :safe-epoch safe-epoch :basis-t basis :as-of as-of})))
      {:database-id (:database-id state) :basis-t as-of :runs (:runs state)
       :run-refs (:run-refs state)
       :history? (true? (:history selector))
       :physical-root (:physical-root state)}))

  (-scan [_ {:keys [runs run-refs basis-t history?]} pattern _opts]
    (->> (if history?
           (history-rows runs basis-t)
           (if runs
             (logical-rows runs basis-t)
             (lazy-logical-rows get-fn run-refs basis-t pattern)))
         (filter #(matches? pattern %)) vec))

  (-history [_ {:keys [runs basis-t]} _opts]
    (history-rows runs basis-t))

  (-checkpoint [this {:keys [database-id basis-t runs run-refs physical-root]}
                _opts]
    (let [rows (if runs
                 (logical-rows runs basis-t)
                 (lazy-logical-rows get-fn run-refs basis-t [nil nil nil]))]
      {:database-id database-id :epoch basis-t
       :logical-checkpoint-root
       (digest this (canonical/checkpoint-datoms rows))
       :physical-root physical-root :engine lsm-profile})))

(defn compaction-due?
  "True when at least one index has reached ENGINE's L0 threshold."
  [engine state]
  (let [levels (or (:levels state) (:level-refs state))]
    (some #(>= (count (:l0 %)) (:l0-compaction-threshold engine))
          (vals levels))))

(defn compact-state
  "Compact a due L0 set without advancing the logical epoch.

  The returned physical root is a maintenance candidate. Hosts must publish it
  with CAS against the input state's physical root, just like a transaction."
  [engine state]
  (if-not (compaction-due? engine state)
    {:state state :compacted? false}
    (let [current-levels (or (:levels state)
                             (load-level-refs (:get-fn engine)
                                              (:level-refs state)))
          compacted-levels (compact-levels-if-needed
                          (:put! engine) (:database-id state)
                          (:safe-epoch state 0) (:target-run-rows engine)
                          (:l0-compaction-threshold engine) current-levels)
          compacted-runs (flatten-levels compacted-levels)
          lsm-root (put-lsm-manifest!
                    (:put! engine) (:database-id state) (:basis-t state)
                    (:safe-epoch state 0) (level-refs-of compacted-levels))
          current (:current-request state)
          next-state (assoc state
                            :levels compacted-levels
                            :level-refs (level-refs-of compacted-levels)
                            :runs compacted-runs
                            :run-refs (into {}
                                            (map (fn [[index values]]
                                                   [index (mapv run-ref-of values)]))
                                            compacted-runs)
                            :lsm-root lsm-root)
          physical-root (ipld/put-node!
                         (:put! engine)
                         (manifest-node next-state lsm-root current))
          final-state (cond-> (-> next-state
                                  (assoc :physical-root physical-root))
                        current
                        (assoc :current-request
                               (assoc current :physical-root physical-root)))]
      {:state final-state :compacted? true
       :previous-root (:physical-root state)
       :physical-root physical-root})))

(defn lsm-engine
  [{:keys [put! get-fn digest-fn target-run-rows l0-compaction-threshold
           reader-pins-fn inline-compaction?]
    :or {target-run-rows 4096 l0-compaction-threshold 8
         reader-pins-fn (constantly []) inline-compaction? true}}]
  (doseq [[capability value] [[:put! put!] [:get-fn get-fn]
                              [:digest-fn digest-fn]]]
    (when-not (ifn? value)
      (throw (ex-info "LSM engine requires injected capability"
                      {:type :kotobase.engine/missing-capability
                       :capability capability}))))
  (when-not (pos-int? l0-compaction-threshold)
    (throw (ex-info "LSM compaction threshold must be a positive integer"
                    {:type :kotobase.engine/invalid-compaction-threshold
                     :threshold l0-compaction-threshold})))
  (when-not (ifn? reader-pins-fn)
    (throw (ex-info "LSM engine reader-pins-fn must be callable"
                    {:type :kotobase.engine/missing-capability
                     :capability :reader-pins-fn})))
  (when-not (boolean? inline-compaction?)
    (throw (ex-info "LSM inline-compaction? must be boolean"
                    {:type :kotobase.engine/invalid-inline-compaction
                     :value inline-compaction?})))
  (->MerkleLsmEngine put! get-fn digest-fn target-run-rows
                     l0-compaction-threshold reader-pins-fn
                     inline-compaction?))
