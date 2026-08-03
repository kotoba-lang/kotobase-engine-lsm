(ns kotobase.engine.lsm-test
  (:require [clojure.test :refer [deftest is]]
            [kotobase.engine.conformance :as conformance]
            [kotobase.engine.contract :as engine]
            [kotobase.engine.lsm :as lsm]
            [kotobase.engine.memory :as memory]))

(def digest-fn #(str "digest:" (hash %)))

(defn fixture []
  (let [blocks (atom {})]
    {:blocks blocks
     :engine (lsm/lsm-engine
              {:put! (fn [cid bytes] (swap! blocks assoc cid bytes))
               :get-fn #(get @blocks %)
               :digest-fn digest-fn})}))

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
