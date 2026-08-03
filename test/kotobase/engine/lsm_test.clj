(ns kotobase.engine.lsm-test
  (:require [clojure.test :refer [deftest is]]
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
