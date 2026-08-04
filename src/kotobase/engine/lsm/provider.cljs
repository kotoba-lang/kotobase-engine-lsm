(ns kotobase.engine.lsm.provider
  "Asynchronous block/ref coordinator for Worker and R2-style hosts."
  (:require [ipld.core :as ipld]
            [kotobase.engine.contract :as engine]
            [kotobase.engine.lsm :as lsm]
            [kotobase.storage.core :as storage]))

(defn- runtime-of [eng]
  (or (:kotobase.provider/runtime (meta eng))
      (throw (ex-info "engine was not created by engine-from-backend"
                      {:type :kotobase.engine/missing-provider-runtime}))))

(defn engine-from-backend [backend options]
  (storage/validate-backend! backend)
  (let [cache (atom {})
        pending (atom {})
        requests (atom 0)
        put! (fn [cid bytes]
               (swap! cache assoc cid bytes)
               (swap! pending assoc cid bytes))
        get-fn (fn [cid]
                 (or (get @cache cid)
                     (throw (ex-info "block is not in the synchronous cache"
                                     {:type :kotobase.engine/missing-block
                                      :missing-cid cid}))))
        eng (lsm/lsm-engine (merge options {:put! put! :get-fn get-fn}))]
    (with-meta eng
      {:kotobase.provider/runtime
       {:backend backend :cache cache :pending pending :requests requests}})))

(defn request-count [eng]
  @(-> eng runtime-of :requests))

(defn- fetch-cids! [eng cids]
  (let [{:keys [backend cache requests]} (runtime-of eng)
        missing (vec (distinct (remove #(contains? @cache %) cids)))]
    (if (empty? missing)
      (js/Promise.resolve nil)
      (do
        (swap! requests + (count missing))
        (-> (storage/-get-blocks backend missing)
            (.then
             (fn [blocks]
               (when-let [absent (seq (remove #(contains? blocks %) missing))]
                 (throw (ex-info "content-addressed blocks were not found"
                                 {:type :kotobase.engine/block-not-found
                                  :cids (vec absent)})))
               (doseq [cid missing
                       :let [actual (ipld/cid (get blocks cid))]]
                 (when-not (= cid actual)
                   (throw (ex-info "content-addressed block CID mismatch"
                                   {:type :ipld/cid-mismatch
                                    :expected-cid cid :actual-cid actual}))))
               (swap! cache merge blocks)
               nil)))))))

(defn- ref-cids [refs]
  (mapv (comp ipld/link-cid #(get % "cid")) refs))

(defn- hydrate-run-refs! [eng refs]
  (let [cids (ref-cids refs)]
    (-> (fetch-cids! eng cids)
        (.then
         (fn [_]
           (let [cache (:cache (runtime-of eng))
                 children (mapcat (fn [cid]
                                    (lsm/run-node-child-cids
                                     (ipld/decode (get @cache cid))))
                                  cids)]
             (fetch-cids! eng children)))))))

(defn- all-run-refs [state]
  (vec (mapcat val (:run-refs state))))

(defn- request-run-refs [state request-id]
  (lsm/request-run-refs state request-id))

(defn restore-head [eng backend ref-name]
  (storage/validate-backend! backend)
  (-> (storage/-read-ref backend ref-name)
      (.then
       (fn [head]
         (when head
           (let [cid (:cid head)]
             (-> (fetch-cids! eng [cid])
                 (.then
                  (fn [_]
                    (let [cache (:cache (runtime-of eng))
                          node (ipld/decode (get @cache cid))
                          lsm-cid (ipld/link-cid (get node "lsm-manifest"))]
                      (-> (fetch-cids! eng [lsm-cid])
                          (.then (fn [_]
                                   (engine/restore-state eng cid
                                                         {:lazy? true}))))))))))))))

(defn scan! [eng snapshot pattern opts]
  (if (:history? snapshot)
    (let [refs (get (:run-refs snapshot) :eavt [])]
      (-> (hydrate-run-refs! eng refs)
          (.then
           (fn [_]
             (engine/scan eng
                          (assoc snapshot :runs
                                 {:eavt (mapv #(lsm/load-run-cached eng %) refs)})
                          pattern opts)))))
    (let [[_ refs] (lsm/scan-run-refs (:run-refs snapshot) pattern)]
      (-> (hydrate-run-refs! eng refs)
          (.then (fn [_] (engine/scan eng snapshot pattern opts)))))))

(defn history! [eng snapshot opts]
  (let [refs (get (:run-refs snapshot) :eavt [])]
    (-> (hydrate-run-refs! eng refs)
        (.then
         (fn [_]
           (engine/history eng
                           (assoc snapshot :runs
                                  {:eavt (mapv #(lsm/load-run-cached eng %) refs)})
                           opts))))))

(defn request-records! [eng state]
  (let [refs (get (:run-refs state) :request [])]
    (-> (hydrate-run-refs! eng refs)
        (.then
         (fn [_]
           (lsm/request-records
            eng (assoc state :runs
                       {:request (mapv #(lsm/load-run-cached eng %) refs)})))))))

(defn request-record! [eng state request-id]
  (let [refs (request-run-refs state request-id)]
    (-> (hydrate-run-refs! eng refs)
        (.then (fn [_] (lsm/request-record eng state request-id))))))

(defn checkpoint! [eng snapshot opts]
  (-> (hydrate-run-refs! eng (all-run-refs snapshot))
      (.then (fn [_] (engine/checkpoint eng snapshot opts)))))

(defn- flush-pending! [eng]
  (let [{:keys [backend pending]} (runtime-of eng)
        blocks (mapv (fn [[cid bytes]] {:cid cid :bytes bytes}) @pending)]
    (-> (if (seq blocks)
          (storage/-put-blocks! backend blocks)
          (js/Promise.resolve nil))
        (.then (fn [result]
                 (reset! pending {})
                 result)))))

(defn transact-and-publish! [eng backend ref-name state request]
  (storage/validate-backend! backend)
  (let [expected (:physical-root state)
        refs (if (:inline-compaction? eng)
               (all-run-refs state)
               (request-run-refs state (:request-id request)))]
    (-> (hydrate-run-refs! eng refs)
        (.then (fn [_] (engine/transact eng state request)))
        (.then
         (fn [result]
           (-> (flush-pending! eng)
               (.then
                (fn [_]
                  (let [next-root (get-in result [:receipt :physical-root])]
                    (-> (storage/-compare-and-set-ref!
                         backend ref-name expected next-root)
                        (.then
                         (fn [publication]
                           (if (:published? publication)
                             (assoc result :publication publication
                                    :publish-status :published)
                             {:publish-status :conflict
                              :publication publication
                              :expected-root expected
                              :candidate-root next-root
                              :winner-root (:current publication)})))))))))))))

(defn compact-and-publish!
  "Run physical-only L0 maintenance and publish it with head CAS."
  [eng backend ref-name state]
  (storage/validate-backend! backend)
  (if-not (lsm/compaction-due? eng state)
    (js/Promise.resolve {:state state :compacted? false
                         :publish-status :not-due})
    (let [expected (:physical-root state)]
      (-> (hydrate-run-refs! eng (all-run-refs state))
          (.then (fn [_] (lsm/compact-state eng state)))
        (.then
         (fn [result]
           (if-not (:compacted? result)
             (assoc result :publish-status :not-due)
             (-> (flush-pending! eng)
                 (.then
                  (fn [_]
                    (let [next-root (:physical-root result)]
                      (-> (storage/-compare-and-set-ref!
                           backend ref-name expected next-root)
                          (.then
                           (fn [publication]
                             (if (:published? publication)
                               (assoc result :publication publication
                                      :publish-status :published)
                               (assoc result :publication publication
                                      :publish-status :conflict
                                      :expected-root expected
                                      :candidate-root next-root
                                      :winner-root (:current publication)))))))))))))))))
