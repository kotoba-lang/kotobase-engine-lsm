(ns kotobase.engine.lsm-test
  (:require [clojure.test :refer [deftest is]]
            [ipld.core :as ipld]
            [kotobase.engine.conformance :as conformance]
            [kotobase.engine.contract :as engine]
            [kotobase.engine.lsm :as lsm]
            [kotobase.engine.memory :as memory]))

(def digest-fn #(str "digest:" (hash %)))

(defn fixture
  ([] (fixture {}))
  ([options]
  (let [blocks (atom {})]
    {:blocks blocks
     :engine (lsm/lsm-engine
              (merge {:put! (fn [cid bytes] (swap! blocks assoc cid bytes))
                      :get-fn #(get @blocks %)
                      :digest-fn digest-fn}
                     options))})))

(deftest shared-conformance
  (is (:passed? (conformance/verify (:engine (fixture))))))

(deftest typed-differential-and-restore
  (let [{candidate :engine} (fixture)
        oracle (memory/memory-engine digest-fn)
        database-id "lsm/differential"
        requests [{:database-id database-id :request-id "l1"
                   :tx-data [[:db/add [:e 1] :value {:n 7 :ok true}]]}
                  {:database-id database-id :request-id "l2"
                   :tx-data [[:db/retract [:e 1] :value {:n 7 :ok true}]
                             [:db/add [:e 1] :value #{:new 8}]]}]
        run (fn [eng]
              (reduce (fn [state request]
                        (:state (engine/transact eng state request)))
                      (engine/empty-state eng database-id) requests))
        actual (run candidate)
        expected (run oracle)
        restored (engine/restore-state candidate (:physical-root actual))]
    (doseq [selector [{} {:as-of 1} {:history true}]]
      (is (= (engine/scan oracle (engine/open-snapshot oracle expected selector)
                          [nil nil nil])
             (engine/scan candidate
                          (engine/open-snapshot candidate restored selector)
                          [nil nil nil]))))
    (is (= (:logical-checkpoint-root
            (engine/checkpoint oracle (engine/open-snapshot oracle expected)))
           (:logical-checkpoint-root
            (engine/checkpoint candidate
                               (engine/open-snapshot candidate restored)))))))

(deftest transaction-path-compacts-l0-without-losing-snapshots
  (let [candidate (:engine (fixture {:l0-compaction-threshold 2
                                     :target-run-rows 64}))
        database-id "lsm/compaction"
        states (reductions
                (fn [state epoch]
                  (:state
                   (engine/transact
                    candidate state
                    {:database-id database-id
                     :request-id (str "c" epoch)
                     :tx-data [[:db/add "counter" :value epoch]]})))
                (engine/empty-state candidate database-id)
                (range 1 5))
        final-state (last states)
        restored (engine/restore-state candidate (:physical-root final-state))]
    (is (every? #(<= (count %) 1) (vals (:runs final-state)))
        "the engine replaces threshold-sized L0 sets with compacted runs")
    (doseq [epoch (range 1 5)]
      (is (= (engine/scan candidate
                          (engine/open-snapshot candidate (nth states epoch)
                                                {:as-of epoch})
                          [nil nil nil])
             (engine/scan candidate
                          (engine/open-snapshot candidate restored
                                                {:as-of epoch})
                          [nil nil nil]))
          "safe epoch zero preserves every historical snapshot"))))

(deftest reader-pins-advance-and-persist-safe-epoch
  (let [pins (atom [])
        candidate (:engine
                   (fixture {:l0-compaction-threshold 2
                             :target-run-rows 8
                             :reader-pins-fn #(deref pins)}))
        database-id "lsm/pinned"
        transact-one
        (fn [state n]
          (:state
           (engine/transact
            candidate state
            {:database-id database-id :request-id (str "pin-" n)
             :tx-data (cond-> []
                        (> n 1) (conj [:db/retract "entity" :value (dec n)])
                        true (conj [:db/add "entity" :value n]))})))
        s1 (transact-one (engine/empty-state candidate database-id) 1)
        s2 (transact-one s1 2)
        _ (reset! pins [{:manifest-cid (:physical-root s2)
                         :epoch 1 :epoch-readers [1]}])
        s3 (transact-one s2 3)
        restored (engine/restore-state candidate (:physical-root s3))]
    (is (= 1 (:safe-epoch s3)))
    (is (= 1 (:safe-epoch restored)) "safe epoch survives engine restore")
    (is (= 1 (get (ipld/decode
                   ((:get-fn candidate) (:lsm-root restored)))
                  "safe-epoch"))
        "safe epoch is also committed by the LSM manifest")
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo #"outside the database basis"
         (engine/open-snapshot candidate restored {:as-of 0})))
    (is (= [2]
           (mapv :v
                 (engine/scan candidate
                              (engine/open-snapshot candidate restored
                                                    {:as-of 2})
                              ["entity" :value nil]))))))

(deftest lazy-restore-range-prunes-point-reads
  (let [{writer :engine blocks :blocks}
        (fixture {:target-run-rows 8 :l0-compaction-threshold 1000})
        database-id "lsm/lazy"
        seeded (:state
                (engine/transact
                 writer (engine/empty-state writer database-id)
                 {:database-id database-id :request-id "seed"
                  :tx-data (mapv (fn [n]
                                   [:db/add (str "entity-" n) :value n])
                                 (range 100))}))
        gets (atom 0)
        reader (lsm/lsm-engine
                {:put! (fn [cid bytes] (swap! blocks assoc cid bytes))
                 :get-fn (fn [cid] (swap! gets inc) (get @blocks cid))
                 :digest-fn digest-fn
                 :target-run-rows 8
                 :l0-compaction-threshold 1000})
        restored (engine/restore-state reader (:physical-root seeded)
                                       {:lazy? true})
        restore-gets @gets
        rows (engine/scan reader (engine/open-snapshot reader restored)
                          ["entity-50" :value nil])
        point-gets (- @gets restore-gets)
        eavt-runs (count (get (:run-refs restored) :eavt))
        cold-next (:state
                   (engine/transact
                    reader restored
                    {:database-id database-id :request-id "cold-write"
                     :tx-data [[:db/retract "entity-50" :value 50]
                               [:db/add "entity-101" :value 101]]}))]
    (is (nil? (:runs restored)) "restore retains run refs, not decoded runs")
    (is (= 2 restore-gets) "restore reads only engine and LSM manifests")
    (is (= [50] (mapv :v rows)))
    (is (<= point-gets 3) "the entity range selects a bounded run subset")
    (is (< point-gets eavt-runs)
        "point lookup does not fetch every EAVT run")
    (is (empty? (engine/scan reader (engine/open-snapshot reader cold-next)
                             ["entity-50" :value nil])))
    (is (= [101]
           (mapv :v (engine/scan reader
                                 (engine/open-snapshot reader cold-next)
                                 ["entity-101" :value nil])))
        "a lazy state can hydrate for a mixed cold transaction")))
