(ns taoensso.faraday2.tests.requests
  (:require [taoensso.faraday2 :as far]
            [clojure.test :refer :all]
            [taoensso.faraday.utils :as utils]))

;;;; Private var aliases

(def create-table-request #'far/create-table-request)
(def update-table-request #'far/update-table-request)
(def get-item-request #'far/get-item-request)
(def put-item-request #'far/put-item-request)
(def update-item-request #'far/update-item-request)
(def delete-item-request #'far/delete-item-request)
(def batch-get-item-request #'far/batch-get-item-request)
(def batch-request-items #'far/batch-request-items)
(def batch-write-item-request #'far/batch-write-item-request)
(def attr-multi-vs #'far/attr-multi-vs)
(def query-request #'far/query-request)
(def write-request #'far/write-request)
(def scan-request #'far/scan-request)
(def list-stream-request #'far/list-streams-request)
(def describe-stream-request #'far/describe-stream-request)
(def get-shard-iterator-request #'far/get-shard-iterator-request)
(def get-records-request #'far/get-records-request)
(def update-ttl-request #'far/update-ttl-request)

(deftest create-table-request-creation
  (let [req (create-table-request
              :create-table-name [:hash-keydef :n]
              {:range-keydef [:range-keydef :n]
               :throughput   {:read 5 :write 10}
               :lsindexes    [{:name         "local-secondary"
                               :range-keydef [:ls-range-keydef :n]
                               :projection   [:ls-projection]}]
               :gsindexes    [{:name         "global-secondary"
                               :hash-keydef  [:gs-hash-keydef :n]
                               :range-keydef [:gs-range-keydef :n]
                               :projection   :keys-only
                               :throughput   {:read 10 :write 2}}]
               :stream-spec  {:enabled?  true
                              :view-type :new-image}})]
    (is (= "create-table-name" (:TableName req)))

    (is (= {:ReadCapacityUnits 5 :WriteCapacityUnits 10}
           (:ProvisionedThroughput req)))

    (is (= #{{:AttributeName "hash-keydef" :KeyType "HASH"}
             {:AttributeName "range-keydef" :KeyType "RANGE"}}
           (into #{} (:KeySchema req))))

    (let [[lsindex & rest] (:LocalSecondaryIndexes req)]
      (is nil? rest)
      (is (= "local-secondary" (:IndexName lsindex)))
      (is (= {:NonKeyAttributes ["ls-projection"]
              :ProjectionType   "INCLUDE"}
             (:Projection lsindex)))
      (is (= #{{:AttributeName "hash-keydef" :KeyType "HASH"}
               {:AttributeName "ls-range-keydef" :KeyType "RANGE"}}
             (into #{} (:KeySchema lsindex)))))

    (let [[gsindex & rest] (:GlobalSecondaryIndexes req)]
      (is nil? rest)
      (is (= "global-secondary" (:IndexName gsindex)))
      (is (= {:ProjectionType "KEYS_ONLY"}
             (:Projection gsindex)))
      (is (= #{{:AttributeName "gs-range-keydef" :KeyType "RANGE"}
               {:AttributeName "gs-hash-keydef" :KeyType "HASH"}}
             (into #{} (:KeySchema gsindex))))
      (is (= {:ReadCapacityUnits 10 :WriteCapacityUnits 2}
             (:ProvisionedThroughput gsindex))))

    (let [stream-spec (:StreamSpecification req)]
      (is true? (:StreamEnabled stream-spec))
      (is (= "NEW_IMAGE" (:StreamViewType stream-spec)))))

  (is (thrown? AssertionError
               (create-table-request
                 :create-table-name [:hash-keydef :n]
                 {:range-keydef [:range-keydef :n]
                  :billing-mode :pay-per-request
                  :throughput   {:read 5 :write 10}})))

  (is (thrown? AssertionError
               (create-table-request
                 :create-table-name [:hash-keydef :n]
                 {:range-keydef [:range-keydef :n]
                  :billing-mode :pay-per-request
                  :gsindexes    [{:name         "global-secondary"
                                  :hash-keydef  [:gs-hash-keydef :n]
                                  :range-keydef [:gs-range-keydef :n]
                                  :projection   :keys-only
                                  :throughput   {:read 10 :write 2}}]})))

  (let [req (create-table-request
              :create-table-name [:hash-keydef :n]
              {:range-keydef [:range-keydef :n]
               :billing-mode :pay-per-request
               :gsindexes    [{:name         "global-secondary"
                               :hash-keydef  [:gs-hash-keydef :n]
                               :range-keydef [:gs-range-keydef :n]
                               :projection   :keys-only}]})]

    (is nil? (:ProvisionedThroughput req))

    (is (= (utils/enum :pay-per-request) (:BillingMode req)))
    (let [[gsindex & _] (:GlobalSecondaryIndexes req)]
      (is nil? (:ProvisionedThroughput gsindex))))

  (let [req (create-table-request
              :create-table-name [:hash-keydef :n]
              {:range-keydef [:range-keydef :n]
               :gsindexes    [{:name         "global-secondary"
                               :hash-keydef  [:gs-hash-keydef :n]
                               :range-keydef [:gs-range-keydef :n]
                               :projection   :keys-only}]})]

    (is nil? (:ProvisionedThroughput req))
    (is (= (utils/enum :provisioned) (:BillingMode req)))))

(deftest update-tables-request-creation
  (is (= "update-table"
         (:TableName (update-table-request :update-table {} {:throughput {:read 1 :write 1}}))))

  (is (= {:ReadCapacityUnits 15 :WriteCapacityUnits 7}
         (:ProvisionedThroughput (update-table-request :update-table {} {:throughput {:read 15 :write 7}}))))

  (let [req (update-table-request
              :update-table
              {}
              {:gsindexes {:name         "global-secondary"
                           :operation    :create
                           :hash-keydef  [:gs-hash-keydef :n]
                           :range-keydef [:gs-range-keydef :n]
                           :projection   :keys-only
                           :throughput   {:read 10 :write 2}}})]
    (is (= "update-table" (:TableName req)))

    (let [[gsindex & rest] (:GlobalSecondaryIndexUpdates req)
          create-action (:Create gsindex)]
      (is nil? rest)
      (is (= "global-secondary" (:IndexName create-action)))
      (is (= {:ProjectionType "KEYS_ONLY"}
             (:Projection create-action)))
      (is (= #{{:AttributeName "gs-range-keydef" :KeyType "RANGE"}
               {:AttributeName "gs-hash-keydef" :KeyType "HASH"}}
             (into #{} (:KeySchema create-action))))
      (is (= {:ReadCapacityUnits 10 :WriteCapacityUnits 2}
             (:ProvisionedThroughput create-action)))))

  (let [req (update-table-request
              :update-table
              {}
              {:gsindexes {:name       "global-secondary"
                           :operation  :update
                           :throughput {:read 4 :write 2}}})]
    (is (= "update-table" (:TableName req)))

    (let [[gsindex & rest] (:GlobalSecondaryIndexUpdates req)
          update-action (:Update gsindex)]
      (is nil? rest)
      (is (= "global-secondary" (:IndexName update-action)))
      (is (= {:ReadCapacityUnits 4 :WriteCapacityUnits 2}
             (:ProvisionedThroughput update-action)))))

  (let [req (update-table-request
              :update-table
              {}
              {:gsindexes {:name      "global-secondary"
                           :operation :delete}})]
    (is (= "update-table" (:TableName req)))

    (let [[gsindex & rest] (:GlobalSecondaryIndexUpdates req)
          action (:Delete gsindex)]
      (is nil? rest)
      (is (= "global-secondary" (:IndexName action)))))

  (let [req (update-table-request
              :update-table
              {}
              {:stream-spec {:enabled? false}})
        stream-spec (:StreamSpecification req)]
    (is false? (:StreamEnabled stream-spec))
    (is nil? (:StreamViewType stream-spec)))

  (testing "updating billing mode"
    (is (= (utils/enum :pay-per-request)
           (:BillingMode (update-table-request :update-table {} {:billing-mode :pay-per-request}))))

    (is (thrown? AssertionError
                 (update-table-request
                   :update-table
                   {}
                   {:throughput   {:read 4 :write 2}
                    :billing-mode :pay-per-request}))))

  (testing "If main table is pay-per-request, then can't specify throughput when adding a GSI"
    (is (thrown? AssertionError
                 (update-table-request
                   :update-table
                   {:billing-mode {:name :pay-per-request}}
                   {:gsindexes {:name        "new-global-secondary"
                                :operation   :create
                                :hash-keydef [:id :s]
                                :projection  :keys-only
                                :throughput  {:read 10 :write 9}}}))))

  (testing "If parent table is pay-per-request, then no need to specify billing mode on new GSIs"
    (let [req (update-table-request
                :update-table
                {:billing-mode {:name :pay-per-request}}
                {:gsindexes {:name        "new-global-secondary"
                             :operation   :create
                             :hash-keydef [:id :s]
                             :projection  :keys-only}})]
      (is (= "update-table" (:TableName req))))))

(deftest get-item-request-creation
  (is (= "get-item" (:TableName (get-item-request :get-item {:x 1}))))

  (is (not (:ConsistentRead (get-item-request :get-item {:x 1} {:consistent? false}))))

  (is (true? (:ConsistentRead (get-item-request :get-item {:x 1} {:consistent? true}))))

  (let [req (get-item-request :get-item-table-name
                              {:hash "y" :range 2}
                              {:attrs [:j1 :j2]})]
    (is (= {"hash"  {:S "y"}
            "range" {:N "2"}}
           (:Key req)))

    (is (= #{"j1" "j2"}
           (into #{} (:AttributesToGet req))))))

(deftest put-item-request-creation
  (is (= "put-item-table-name" (:TableName (put-item-request :put-item-table-name {:x 1}))))

  (let [req (put-item-request :put-item-table-name
                              {:c1 "hey" :c2 1}
                              {:return   :updated-new
                               :expected {:e1 "expected value"
                                          :e2 false}})]
    (is (= "UPDATED_NEW" (:ReturnValues req)))
    (is (= {"c1" {:S "hey"}
            "c2" {:N "1"}}
           (:Item req)))
    (is (= {"e1" {:Value {:S "expected value"}}
            "e2" {:Value {:BOOL false}}}
           (:Expected req)))))

(deftest update-item-request-creation
  (let [req (update-item-request :update-item
                                 {:x 1}
                                 {:update-map {:y [:put 2]
                                               :z [:add "xyz"]
                                               :a [:delete]}
                                  :expected   {:e1 "expected!"}
                                  :return     :updated-old})]

    (is (= "update-item" (:TableName req)))
    (is (= {"x" {:N "1"}}
           (:Key req)))
    (is (= {"y" {:Action "PUT" :Value {:N "2"}}
            "z" {:Action "ADD" :Value {:S "xyz"}}
            "a" {:Action "DELETE"}}
          (:AttributeUpdates req)))
    (is (= "UPDATED_OLD" (:ReturnValues req)))
    (is (= {"e1" {:Value {:S "expected!"}}}
           (:Expected req))))

  (let [req (update-item-request :update-item
                                 {:x 1}
                                 {:update-expr     "SET #p = :price REMOVE details.tags[2]"
                                  :expr-attr-vals  {":price" 0.89}
                                  :expr-attr-names {"#p" "price"}
                                  :expected        {:e1 "expected!"}
                                  :return          :updated-old})]

    (is (= "update-item" (:TableName req)))
    (is (= {"x" {:N "1"}}
           (:Key req)))
    (is (= "SET #p = :price REMOVE details.tags[2]" (:UpdateExpression req)))
    (is (= {":price" {:N "0.89"}} (:ExpressionAttributeValues req)))
    (is (= {"#p" "price"} (:ExpressionAttributeNames req)))
    (is (= "UPDATED_OLD" (:ReturnValues req)))
    (is (= {"e1" {:Value {:S "expected!"}}}
           (:Expected req)))))

(deftest delete-item-request-creation
  (let [req (delete-item-request :delete-item
                                 {:k1 "val" :r1 -3}
                                 {:return          :all-new
                                  :cond-expr       "another = :a AND #n = :name"
                                  :expr-attr-vals  {":a" 1 ":name" "joe"}
                                  :expr-attr-names {"#n" "name"}
                                  :expected        {:e1 1}})]

    (is (= "delete-item" (:TableName req)))
    (is (= {"k1" {:S "val"}
            "r1" {:N "-3"}}
           (:Key req)))
    (is (= {"e1" {:Value {:N "1"}}}
           (:Expected req)))
    (is (= "another = :a AND #n = :name" (:ConditionExpression req)))
    (is (= 2 (count (:ExpressionAttributeValues req))))
    (is (= {"#n" "name"} (:ExpressionAttributeNames req)))
    (is (= "ALL_NEW" (:ReturnValues req)))))

(deftest batch-get-item-request-creation
  (let [req (batch-get-item-request
              false
              (batch-request-items
                {:t1 {:prim-kvs {:t1-k1 -10}
                      :attrs    [:some-other-guy]}
                 :t2 {:prim-kvs {:t2-k1 ["x" "y" "z"]}}}))]

    (is (= {"t1" {:AttributesToGet ["some-other-guy"]
                  :Keys            [{"t1-k1" {:N "-10"}}]}
            "t2" {:Keys [{"t2-k1" {:S "x"}}
                         {"t2-k1" {:S "y"}}
                         {"t2-k1" {:S "z"}}]}}
           (:RequestItems req)))))

(deftest batch-write-item-request-creation
  (let [req (batch-write-item-request
              false
              {"t1" (mapv
                      #(write-request :put %)
                      (attr-multi-vs {:k ["x" "y"]}))
               "t2" (mapv
                      #(write-request :delete %)
                      (attr-multi-vs {:k [0]}))})]
    (is (= {"t1" [{:PutRequest {:Item {"k" {:S "x"}}}}
                  {:PutRequest {:Item {"k" {:S "y"}}}}]
            "t2" [{:DeleteRequest {:Key {"k" {:N "0"}}}}]}
           (:RequestItems req)))))

(deftest query-request-creation
  (let [req (query-request :query
                           {:name [:eq "Steve"]
                            :age  [:between [10 30]]}
                           {:return :all-projected-attributes
                            :index  :lsindex
                            :order  :desc
                            :limit  2})]
    (is (= "query" (:TableName req)))
    (is (= {"name" {:AttributeValueList [{:S "Steve"}]
                    :ComparisonOperator "EQ"}
            "age"  {:AttributeValueList [{:N "10"}
                                         {:N "30"}]
                    :ComparisonOperator "BETWEEN"}}
           (:KeyConditions req)))
    (is (= "ALL_PROJECTED_ATTRIBUTES" (:Select req)))
    (is (= "lsindex" (:IndexName req)))
    (is false? (:ScanIndexForward req))
    (is (= 2 (:Limit req)))))

(deftest scan-request-creation
  (let [req (scan-request :scan
                          {:attr-conds      {:age [:in [24 27]]}
                           :index           :age-index
                           :proj-expr       "age, #t"
                           :expr-attr-names {"#t" "year"}
                           :return          :count
                           :limit           10})]
    (is (= "scan" (:TableName req)))
    (is (= 10 (:Limit req)))
    (is (= {"age" {:AttributeValueList [{:N "24"}
                                        {:N "27"}]
                   :ComparisonOperator "IN"}}
           (:ScanFilter req)))
    (is (= "COUNT" (:Select req)))
    (is (= "age-index" (:IndexName req)))
    (is (= "age, #t" (:ProjectionExpression req)))
    (is (= {"#t" "year"} (:ExpressionAttributeNames req))))

  (let [req (scan-request :scan
                          {:filter-expr "age < 25"
                           :index       "age-index"
                           :limit       5
                           :consistent? true})]
    (is (= "scan" (:TableName req)))
    (is (= 5 (:Limit req)))
    (is (= "age < 25" (:FilterExpression req)))
    (is (= "age-index" (:IndexName req)))
    (is (= (:ConsistentRead req)))))

(deftest list-streams-request-creation
  (let [req (list-stream-request
              {:table-name "stream-table-name"
               :limit      42
               :start-arn  "arn:aws:dynamodb:ddblocal:0:table/etc"})]
    (is (= "stream-table-name" (:TableName req)))
    (is (= 42 (:Limit req)))
    (is (= "arn:aws:dynamodb:ddblocal:0:table/etc" (:ExclusiveStartStreamArn req)))))

(deftest describe-streams-request-creation
  (let [req (describe-stream-request
              "arn:aws:dynamodb:ddblocal:0:table/etc"
              {:limit          20
               :start-shard-id "01"})]
    (is (= "arn:aws:dynamodb:ddblocal:0:table/etc" (:StreamArn req)))
    (is (= 20 (:Limit req)))
    (is (= "01" (:ExclusiveStartShardId req)))))

(deftest get-shard-iterator-request-creation
  (let [req (get-shard-iterator-request
              "arn:aws:dynamodb:ddblocal:0:table/etc"
              "shardId000"
              :after-sequence-number
              {:seq-num "000001"})]
    (is (= "arn:aws:dynamodb:ddblocal:0:table/etc" (:StreamArn req)))
    (is (= "shardId000" (:ShardId req)))
    (is (= "AFTER_SEQUENCE_NUMBER" (:ShardIteratorType req)))
    (is (= "000001" (:SequenceNumber req)))))

(deftest get-records-request-creation
  (let [req (get-records-request
              "arn:aws:dynamodb:us-west-2:111122223333:table/etcetc"
              {:limit 50})]
    (is (= "arn:aws:dynamodb:us-west-2:111122223333:table/etcetc" (:ShardIterator req)))
    (is (= 50 (:Limit req)))))

(deftest update-ttl-request-creation
  (let [req (update-ttl-request
              {:table-name :my-update-ttl-table
               :enabled?   false})]
    (is "my-update-ttl-table" (:TableName req))
    (let [ttl-spec (:TimeToLiveSpecification req)]
      (is (not (:Enabled ttl-spec)))
      (is "ttl" (:AttributeName ttl-spec))))

  (let [req (update-ttl-request
              {:table-name :my-update-ttl-table
               :enabled?   true
               :key-name   :ttl})]
    (is "my-update-ttl-table" (:TableName req))
    (let [ttl-spec (:TimeToLiveSpecification req)]
      (is (:Enabled ttl-spec))
      (is "ttl" (:AttributeName ttl-spec)))))
