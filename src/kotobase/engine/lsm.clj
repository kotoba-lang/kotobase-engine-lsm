(ns kotobase.engine.lsm
  "Datomic-shaped engine over immutable Merkle-LSM runs."
  (:require [clojure.edn :as edn]
            [ipld.core :as ipld]
            [kotobase.engine.canonical :as canonical]
            [kotobase.engine.contract :as contract]
            [kotobase.engine.profile :as profile]
            [merkle-lsm.compaction :as compaction]
            [merkle-lsm.core :as lsm]))

(def engine-format-version 1)

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

(defn- run-refs [runs]
  (into {}
        (map (fn [[index values]]
               [index {:l0 (mapv run-ref-of values)}]))
        runs))

(defn- compact-runs-if-needed
  "Bound L0 fan-in while retaining every version required at SAFE-EPOCH."
  [put! database-id safe-epoch target-run-rows threshold runs]
  (into {}
        (map (fn [[index values]]
               (if (< (count values) threshold)
                 [index values]
                 (let [compacted (lsm/compact-runs-partitioned
                                  index database-id safe-epoch
                                  target-run-rows values)]
                   (doseq [run compacted]
                     (persist-effects! put! (:effects run)))
                   [index compacted]))))
        runs))

(defn- manifest-node [state lsm-root current-request]
  {"engine" "kotobase-engine-lsm"
   "format-version" engine-format-version
   "database-id" (:database-id state)
   "basis-t" (:basis-t state)
   "safe-epoch" (:safe-epoch state 0)
   "lsm-manifest" (ipld/link lsm-root)
   "history-edn" (pr-str (:history state))
   "requests-edn" (pr-str (:requests state))
   "snapshots-edn" (pr-str (:snapshots state))
   "current-request-edn" (pr-str current-request)})

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

(defn- manifest-run-refs [manifest]
  (into {}
        (keep (fn [[index levels]]
                (let [refs (vec (mapcat val levels))]
                  (when (seq refs) [(keyword index) refs]))))
        (get manifest "indexes")))

(defn- load-run-refs [get-fn refs-by-index]
  (into {}
        (map (fn [[index refs]]
               [index (mapv #(load-run get-fn %) refs)]))
        refs-by-index))

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

(defn- scan-index [pattern]
  (let [[e a v] pattern]
    (cond
      (some? e) [:eavt (encode-component e)]
      (and (some? a) (some? v)) [:avet (encode-component a)]
      (some? a) [:aevt (encode-component a)]
      :else [:eavt ""])))

(defn- lazy-logical-rows [get-fn refs-by-index basis-t pattern]
  (let [[index prefix] (scan-index pattern)
        selected (lsm/select-run-refs-by-first-component
                  (get refs-by-index index []) prefix)]
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
                            l0-compaction-threshold reader-pins-fn]
  contract/IEngine
  (-engine-profile [_] lsm-profile)
  (-empty-state [_ {:keys [database-id]}]
    {:database-id database-id :basis-t 0 :safe-epoch 0
     :runs {} :run-refs {} :history []
     :requests {} :snapshots {}})

  (-restore-state [_ physical-root opts]
    (let [node (ipld/decode (get-fn physical-root))
          _ (when-not (and (= "kotobase-engine-lsm" (get node "engine"))
                           (= engine-format-version (get node "format-version")))
              (throw (ex-info "unsupported LSM engine manifest"
                              {:type :kotobase.engine/unsupported-manifest})))
          basis-t (get node "basis-t")
          lsm-root (ipld/link-cid (get node "lsm-manifest"))
          lsm-manifest (ipld/decode (get-fn lsm-root))
          refs-by-index (manifest-run-refs lsm-manifest)
          current (read-field node "current-request-edn")
          requests (cond-> (read-field node "requests-edn")
                     current (assoc (:request-id current)
                                    (assoc current :physical-root physical-root)))]
      {:database-id (get node "database-id") :basis-t basis-t
       :safe-epoch (get node "safe-epoch" 0)
       :runs (when-not (:lazy? opts) (load-run-refs get-fn refs-by-index))
       :run-refs refs-by-index :lsm-root lsm-root
       :history (read-field node "history-edn") :requests requests
       :snapshots (assoc (read-field node "snapshots-edn") basis-t physical-root)
       :physical-root physical-root}))

  (-transact [this state {:keys [database-id request-id tx-data]}]
    (when-not (= database-id (:database-id state))
      (throw (ex-info "transaction database does not match state"
                      {:type :kotobase.engine/database-mismatch})))
    (let [tx (canonical/normalize-tx tx-data)
          tx-root (digest this tx)]
      (if-let [prior (get-in state [:requests request-id])]
        (if (= tx-root (:tx-root prior))
          {:state state
           :receipt {:database-id database-id :epoch (:epoch prior)
                     :request-id request-id :tx-root tx-root
                     :physical-root (:physical-root prior) :engine lsm-profile
                     :status :replayed}}
          (throw (ex-info "request-id was already used for another transaction"
                          {:type :kotobase.engine/idempotency-conflict})))
        (let [epoch (inc (:basis-t state))
              safe-epoch (next-safe-epoch reader-pins-fn state epoch)
              current-runs (or (:runs state)
                               (load-run-refs get-fn (:run-refs state)))
              new-runs (lsm/build-index-run-ranges
                        database-id epoch target-run-rows
                        (mapv encode-datom tx))
              _ (doseq [runs (vals new-runs) run runs]
                  (persist-effects! put! (:effects run)))
              all-runs (compact-runs-if-needed
                        put! database-id safe-epoch target-run-rows
                        l0-compaction-threshold
                        (merge-with into current-runs new-runs))
              lsm-manifest (lsm/build-manifest
                            {:db-id database-id :epoch epoch
                             :safe-epoch safe-epoch
                             :previous (:lsm-root state)
                             :indexes (run-refs all-runs)})
              _ (persist-effects! put! (:effects lsm-manifest))
              lsm-root (:cid lsm-manifest)
              appended (mapv (fn [{:keys [e a v op]}]
                               {:e e :a a :v v :t epoch
                                :added (= :assert op)}) tx)
              next-state (-> state (assoc :basis-t epoch
                                          :safe-epoch safe-epoch
                                          :runs all-runs
                                          :run-refs
                                          (into {}
                                                (map (fn [[index values]]
                                                       [index (mapv run-ref-of
                                                                    values)]))
                                                all-runs)
                                          :lsm-root lsm-root)
                             (update :history into appended))
              current {:request-id request-id :tx-root tx-root :epoch epoch}
              physical-root (ipld/put-node! put!
                                            (manifest-node next-state lsm-root current))
              record (assoc current :physical-root physical-root)
              final-state (-> next-state
                              (assoc :physical-root physical-root)
                              (assoc-in [:snapshots epoch] physical-root)
                              (assoc-in [:requests request-id] record))]
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
       :history (:history state) :history? (true? (:history selector))
       :physical-root (get-in state [:snapshots as-of])}))

  (-scan [_ {:keys [runs run-refs basis-t history history?]} pattern _opts]
    (->> (if history?
           (->> history (filter #(<= (:t %) basis-t))
                canonical/canonical-datoms)
           (if runs
             (logical-rows runs basis-t)
             (lazy-logical-rows get-fn run-refs basis-t pattern)))
         (filter #(matches? pattern %)) vec))

  (-history [_ {:keys [history basis-t]} _opts]
    (->> history (filter #(<= (:t %) basis-t)) canonical/canonical-datoms))

  (-checkpoint [this {:keys [database-id basis-t runs run-refs physical-root]}
                _opts]
    (let [rows (if runs
                 (logical-rows runs basis-t)
                 (lazy-logical-rows get-fn run-refs basis-t [nil nil nil]))]
      {:database-id database-id :epoch basis-t
       :logical-checkpoint-root
       (digest this (canonical/checkpoint-datoms rows))
       :physical-root physical-root :engine lsm-profile})))

(defn lsm-engine
  [{:keys [put! get-fn digest-fn target-run-rows l0-compaction-threshold
           reader-pins-fn]
    :or {target-run-rows 4096 l0-compaction-threshold 8
         reader-pins-fn (constantly [])}}]
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
  (->MerkleLsmEngine put! get-fn digest-fn target-run-rows
                     l0-compaction-threshold reader-pins-fn))
