(ns taoensso.faraday2
  "Clojure DynamoDB client using the AWS SDK v2 via https://github.com/cognitect-labs/aws-api"
  (:require [clojure.string :as str]
            [cognitect.aws.client.api :as aws]
            [taoensso.encore :as enc]
            [taoensso.faraday.utils :as utils]
            [taoensso.nippy :as nippy]
            [taoensso.nippy.tools :as nippy-tools])
  (:import [clojure.lang BigInt IPersistentVector Keyword LazySeq]
           [cognitect.aws.client.impl Client]
           [java.io BufferedInputStream]
           [java.nio ByteBuffer]
           [java.util Map Set]
           [taoensso.faraday ConditionalCheckFailedException
                             InternalServerErrorException
                             ItemCollectionSizeLimitExceededException
                             LimitExceededException
                             ProvisionedThroughputExceededException
                             ResourceInUseException
                             ResourceNotFoundException
                             TransactionCanceledException
                             ValidationException]
           [taoensso.nippy.tools WrappedForFreezing]))

(defn- db-client*
  "Returns a Client instance for the supplied client opts:
    (db-client* {:credentials-provider (cognitect.aws.credentials/basic-credentials-provider
                                         {:access-key-id \"ABC\"
                                          :secret-access-key \"XYZ\"})})
    (db-client* {:credentials-provider (cognitect.aws.credentials/profile-credentials-provider
                                         \"myprofile\")})

  See https://github.com/cognitect-labs/aws-api#credentials"
  [client-opts]
  (aws/client (assoc client-opts :api :dynamodb)))

(defn- db-streams-client*
  "Returns a Client instance for the supplied client opts:
    (db-streams-client* {:credentials-provider (cognitect.aws.credentials/basic-credentials-provider
                                                 {:access-key-id \"ABC\"
                                                  :secret-access-key \"XYZ\"})})
    (db-streams-client* {:credentials-provider (cognitect.aws.credentials/profile-credentials-provider
                                                 \"myprofile\")})

  See https://github.com/cognitect-labs/aws-api#credentials"
  [client-opts]
  (aws/client (assoc client-opts :api :streams-dynamodb)))

(defn- db-client ^Client
  [{:keys [client] :as client-opts}]
  (or client (db-client* client-opts)))

(defn- db-streams-client ^Client
  [{:keys [client] :as client-opts}]
  (or client (db-streams-client* client-opts)))

;;;; Exceptions

(defn- error-response->message
  [response]
  (if (:Error response)
    (str (get-in response [:Error :Code]) ": " (get-in response [:Error :Message]))
    (str/join " " (remove nil? [(:cognitect.aws.http/status response)
                                (:cognitect.aws.error/code response)
                                (:cognitect.anomalies/category response)]))))

(defn- exceptions!
  [response]
  (when (or (some-> (:cognitect.aws.http/status response) (>= 400))
            (:cognitect.anomalies/category response))
    (let [message (error-response->message response)]
      (throw (case (:cognitect.aws.error/code response)
               "ConditionalCheckFailedException" (ConditionalCheckFailedException. message response)
               "InternalServerErrorException" (InternalServerErrorException. message response)
               "ItemCollectionSizeLimitExceededException" (ItemCollectionSizeLimitExceededException. message response)
               "LimitExceededException" (LimitExceededException. message response)
               "ProvisionedThroughputExceededException" (ProvisionedThroughputExceededException. message response)
               "ResourceInUseException" (ResourceInUseException. message response)
               "ResourceNotFoundException" (ResourceNotFoundException. message response)
               "TransactionCanceledException" (TransactionCanceledException. message response)
               "ValidationException" (ValidationException. message response)
               (ex-info (str "Anomaly calling Dynamo " message) response))))))

(defn- with-exceptions
  [response]
  (doto response exceptions!))

(def ^:private invoke-with-ex
  (comp with-exceptions aws/invoke))

(def ^:const ex "DynamoDB API exceptions. Use #=(ex _) for `try` blocks, etc."
  {:conditional-check-failed            ConditionalCheckFailedException
   :internal-server-error               InternalServerErrorException
   :item-collection-size-limit-exceeded ItemCollectionSizeLimitExceededException
   :limit-exceeded                      LimitExceededException
   :provisioned-throughput-exceeded     ProvisionedThroughputExceededException
   :resource-in-use                     ResourceInUseException
   :resource-not-found                  ResourceNotFoundException
   :transaction-cancelled               TransactionCanceledException
   :validation-failed                   ValidationException})

;;;; Value coercions

(def ^:private nt-freeze (comp #(ByteBuffer/wrap %) nippy-tools/freeze))

(defn- nt-thaw
  [^BufferedInputStream in]
  (let [ba (doto (byte-array (.available in)) (->> (.read in)))
        serialized? (#'nippy/try-parse-header ba)]
    (if-not serialized?
      ba ; No Nippy header => assume non-serialized binary data (e.g. other client)
      (try ; Header match _may_ have been a fluke (though v. unlikely)
        (nippy-tools/thaw ba)
        (catch Exception _
          ba)))))

(enc/defalias with-thaw-opts nippy-tools/with-thaw-opts)
(enc/defalias freeze nippy-tools/wrap-for-freezing
              "Forces argument of any type to be subject to automatic de/serialization with
              Nippy.")

(defn- freeze? [x] (or (nippy-tools/wrapped-for-freezing? x) (enc/bytes? x)))

(defn- assert-precision [x]
  (let [^BigDecimal dec (if (string? x) (BigDecimal. ^String x) (bigdec x))]
    (assert (<= (.precision dec) 38)
            (str "DynamoDB numbers have <= 38 digits of precision. See `freeze` for "
                 "arbitrary-precision binary serialization."))
    x))

(defn- ddb-num?
  "Is `x` a number type natively storable by DynamoDB? Note that DDB stores _all_
  numbers as exact-value strings with <= 38 digits of precision. For greater
  precision, use `freeze`.
  Ref. http://goo.gl/jzzsIW"
  [x]
  (or (instance? Long x)
      (instance? Double x)
      (instance? Integer x)
      (instance? Float x)
      ;; High-precision types:
      (and (instance? BigInt x) (assert-precision x))
      (and (instance? BigDecimal x) (assert-precision x))
      (and (instance? BigInteger x) (assert-precision x))))

(defn- ddb-num-str->num [^String s]
  ;; In both cases we'll err on the side of caution, assuming the most
  ;; accurate possible type
  (if (.contains s ".")
    (BigDecimal. s)
    (bigint (BigInteger. s))))

(defn- deserialize
  "Returns the Clojure value of given AttributeValue map."
  [m]
  (let [[x type]
        (or
          (some-> (:S m) (vector :s))
          (some-> (:N m) (vector :n))
          (some-> (:NULL m) (vector :null))
          (some-> (:BOOL m) (vector :bool))
          (some-> (:SS m) (vector :ss))
          (some-> (:NS m) (vector :ns))
          (some-> (:BS m) (vector :bs))
          (some-> (:B m) (vector :b))
          (some-> (:M m) (vector :m))
          (some-> (:L m) (vector :l)))]

    (case type
      :s x
      :n (ddb-num-str->num x)
      :null nil
      :bool (boolean x)
      :ss (into #{} x)
      :ns (into #{} (mapv ddb-num-str->num x))
      :bs (into #{} (mapv nt-thaw x))
      :b (nt-thaw x)

      :l (mapv deserialize x)
      :m (zipmap (mapv keyword (keys x))
                 (mapv deserialize (vals x))))))

(defprotocol ISerializable
  "Extensible protocol for mapping Clojure vals to AttributeValue maps."
  (serialize [this]))

(extend-protocol ISerializable
  nil (serialize [_] {:NULL true})
  Boolean (serialize [x] {:BOOL x})
  Long (serialize [x] {:N (str x)})
  Double (serialize [x] {:N (str x)})
  Integer (serialize [x] {:N (str x)})
  Float (serialize [x] {:N (str x)})
  BigInt (serialize [x] {:N (str (assert-precision x))})
  BigDecimal (serialize [x] {:N (str (assert-precision x))})
  BigInteger (serialize [x] {:N (str (assert-precision x))})

  WrappedForFreezing
  (serialize [x] {:B (nt-freeze x)})

  Keyword
  (serialize [kw] (serialize (enc/as-qname kw)))

  String
  (serialize [s] {:S s})

  IPersistentVector
  (serialize [v] {:L (mapv serialize v)})

  Map
  (serialize [m] {:M (persistent!
                       (reduce-kv
                         (fn [acc k v] (assoc! acc (enc/as-qname k) (serialize v)))
                         (transient {})
                         m))})

  Set
  (serialize [s]
    (if (empty? s)
      (throw (Exception. "Invalid DynamoDB value: empty set"))
      (cond
        (enc/revery? enc/stringy? s) {:SS (mapv enc/as-qname s)}
        (enc/revery? ddb-num? s) {:NS (mapv str s)}
        (enc/revery? freeze? s) {:BS (mapv nt-freeze s)}
        :else (throw (Exception. "Invalid DynamoDB value: set of invalid type or more than one type")))))

  LazySeq
  (serialize [s]
    (if (.isRealized s)
      {:L (mapv serialize s)}
      (throw (IllegalArgumentException. "Unrealized lazy sequences are not supported. Realize this sequence before calling Faraday (e.g. doall) or replace the sequence with a non-lazy alternative (e.g. 'mapv' instead of 'map', or use 'into []'). Faraday avoids attempting to realize values that might be infinite, since this could cause strange an unexpected problems that are hard to diagnose.")))))

(extend-type (Class/forName "[B")
  ISerializable
  (serialize [ba] {:B (nt-freeze ba)}))

(defn- enum-op ^String [operator]
  (-> operator {:> "GT" :>= "GE" :< "LT" :<= "LE" := "EQ"} (or operator)
      utils/enum))

(def db-item->clj-item (partial utils/keyword-map deserialize))
(defn clj-item->db-item [item]
  (utils/name-map serialize item))

(defn- cc-units
  [cc]
  (some-> cc :CapacityUnits))

(defn- batch-cc-units
  [ccs]
  (reduce
    (fn [m cc]
      (assoc m (keyword (:TableName cc)) (cc-units cc)))
    {}
    ccs))

;;;; Response Formatting

(defn- format-provisioned-throughput
  [m]
  {:read                (:ReadCapacityUnits m)
   :write               (:WriteCapacityUnits m)
   :last-decrease       (:LastDecreaseDateTime m)
   :last-increase       (:LastIncreaseDateTime m)
   :num-decreases-today (:NumberOfDecreasesToday m)})

(defn- format-billing-mode
  [m]
  {:name        (utils/un-enum (:BillingMode m))
   :last-update (:LastUpdateToPayPerRequestDateTime m)})

(defn- format-key-schema-element
  [m]
  {:name (keyword (:AttributeName m))
   :type (utils/un-enum (:KeyType m))})

(defn- format-attribute-definition
  [m]
  {:name (keyword (:AttributeName m))
   :type (utils/un-enum (:AttributeType m))})

(defn- format-projection
  [m]
  {:projection-type    (:ProjectionType m)
   :non-key-attributes (:NonKeyAttributes m)})

(defn- format-lsi
  [m]
  {:name       (keyword (:IndexName m))
   :size       (:IndexSizeBytes m)
   :item-count (:ItemCount m)
   :key-schema (mapv format-key-schema-element (:KeySchema m))
   :projection (format-projection (:Projection m))})

(defn- format-gsi
  [m]
  (assoc (format-lsi m) :throughput (format-provisioned-throughput (:ProvisionedThroughput m))))

(defn- format-stream-spec
  [m]
  {:enabled?  (:StreamEnabled m)
   :view-type (utils/un-enum (:StreamViewType m))})

(defn- format-table
  [m]
  {:name                (keyword (:TableName m))
   :creation-date       (:CreationDateTime m)
   :item-count          (:ItemCount m)
   :size                (:TableSizeBytes m)
   :throughput          (some-> (:ProvisionedThroughput m) format-provisioned-throughput)
   :billing-mode        (format-billing-mode (or (:BillingModeSummary m) {:BillingMode "PROVISIONED"}))
   :lsindexes           (some->> (:LocalSecondaryIndexes m) (mapv format-lsi))
   :gsindexes           (some->> (:GlobalSecondaryIndexes m) (mapv format-gsi))
   :stream-spec         (some-> (:StreamSpecification m) format-stream-spec)
   :latest-stream-label (:LatestStreamLabel m)
   :latest-stream-arn   (:LatestStreamArn m)
   :status              (utils/un-enum (:TableStatus m))
   :prim-keys
   (let [schema (mapv format-key-schema-element (:KeySchema m))
         defs (mapv format-attribute-definition (:AttributeDefinitions m))]
     (merge-with merge
                 (reduce-kv (fn [m _k v] (assoc m (:name v) {:key-type (:type v)}))
                            {} schema)
                 (reduce-kv (fn [m _k v] (assoc m (:name v) {:data-type (:type v)}))
                            {} defs)))})

(defn- format-item-result
  "Coerce item from attribute value map with cc units in metadata"
  [item cc]
  (with-meta (db-item->clj-item item)
             {:cc-units (cc-units cc)}))

(defn- format-batch-get-item-result
  [m]
  {:items       (utils/keyword-map (partial mapv db-item->clj-item) (:Responses m))
   :cc-units    (batch-cc-units (:ConsumedCapacity m))
   :unprocessed (:UnprocessedKeys m)})

(defn- format-batch-write-item-result
  [m]
  {:cc-units    (batch-cc-units (:ConsumedCapacity m))
   :unprocessed (:UnprocessedItems m)})

(defn- format-query|scan-result
  [m]
  {:items         (mapv db-item->clj-item (:Items m))
   :count         (:Count m)
   :cc-units      (cc-units (:ConsumedCapacity m))
   :last-prim-kvs (some-> (:LastEvaluatedKey m) db-item->clj-item)})

(defn- format-stream
  [m]
  {:stream-arn (:StreamArn m)
   :table-name (:TableName m)})

(defn- format-sequence-number-range
  [m]
  {:starting-seq-num (:StartingSequenceNumber m)
   :ending-seq-num   (:EndingSequenceNumber m)})

(defn- format-shard
  [m]
  {:shard-id        (:ShardId m)
   :parent-shard-id (:ParentShardId m)
   :seq-num-range   (format-sequence-number-range (:SequenceNumberRange m))})

(defn- format-stream-description
  [m]
  {:stream-arn                 (:StreamArn m)
   :stream-label               (:StreamLabel m)
   :stream-status              (utils/un-enum (:StreamStatus m))
   :stream-view-type           (utils/un-enum (:StreamViewType m))
   :creation-request-date-time (:CreationRequestDateTime m)
   :table-name                 (:TableName m)
   :key-schema                 (mapv format-key-schema-element (:KeySchema m))
   :shards                     (mapv format-shard (:Shards m))
   :last-evaluated-shard-id    (:LastEvaluatedShardId m)})

(defn- format-stream-record
  [m]
  {:keys      (db-item->clj-item (:Keys m))
   :old-image (db-item->clj-item (:OldImage m))
   :new-image (db-item->clj-item (:NewImage m))
   :seq-num   (:SequenceNumber m)
   :size      (:SizeBytes m)
   :view-type (utils/un-enum (:StreamViewType m))})

(defn- format-record
  [m]
  {:event-id      (:eventID m)
   :event-name    (utils/un-enum (:eventName m))
   :event-version (:eventVersion m)
   :event-source  (:eventSource m)
   :aws-region    (:awsRegion m)
   :stream-record (format-stream-record (:dynamodb m))})

;;;; Tables

(defn list-tables
  "Returns a lazy sequence of table names."
  [client-opts]
  (let [step
        (fn step [^String offset]
          (lazy-seq
            (let [response (invoke-with-ex (db-client client-opts)
                                           {:op      :ListTables
                                            :request (cond-> {} offset (assoc :ExclusiveStartTableName offset))})
                  last-key (:LastEvaluatedTableName response)
                  chunk (map keyword (:TableNames response))]
              (if last-key
                (concat chunk (step (name last-key)))
                chunk))))]
    (step nil)))

(defn describe-table
  "Returns a map describing a table, or nil if the table doesn't exist."
  [client-opts table]
  (let [response (aws/invoke (db-client client-opts)
                             {:op      :DescribeTable
                              :request {:TableName (name table)}})]
    (when-not (= (:cognitect.aws.error/code response) "ResourceNotFoundException")
      (exceptions! response)
      (format-table (:Table response)))))

(defn- table-status-watch
  "Returns a future to poll for a change to table's status from the one
  passed as a parameter."
  [client-opts table status & [{:keys [poll-ms]
                                :or   {poll-ms 1500}}]]
  (assert (#{:creating :updating :deleting :active} (utils/un-enum status))
          (str "Invalid table status: " status))
  (future
    (loop []
      (let [current-descr (describe-table client-opts table)]
        (if-not (= (:status current-descr) (utils/un-enum status))
          current-descr
          (do (Thread/sleep ^long poll-ms)
              (recur)))))))

(defn- index-status-watch
  "Returns a future to poll for index status.
  `index-type` - e/o #{:gsindexes :lsindexes} (currently only supports :gsindexes)"
  [client-opts table index-type index-name & [{:keys [poll-ms]
                                               :or   {poll-ms 1500}}]]
  (future
    (loop []
      (let [current-descr (describe-table client-opts table)
            [index] (filterv #(= (:name %) (keyword index-name))
                             (index-type current-descr))]
        (cond
          (nil? index) nil

          (or (nil? (:size index))
              (nil? (:item-count index)))
          (do (Thread/sleep ^long poll-ms)
              (recur))

          :else index)))))

(defn- key-schema-element
  "Returns a {:AttributeName string, :KeyType [:one-of [\"HASH\" \"RANGE\"]]}"
  [key-name key-type]
  {:AttributeName (name key-name)
   :KeyType       (str/upper-case (name key-type))})

(defn- key-schema
  "Returns a [{<hash-key> {}}], or
             [{<hash-key> {}} {<range-key> {}}]
   vector for use as a table/index primary key."
  [[hname _ :as _hash-keydef] & [[rname _ :as range-keydef]]]
  (cond-> [(key-schema-element hname :hash)]
          range-keydef (conj (key-schema-element rname :range))))

(defn- proj-def
  "<#{:all :keys-only [<attr> ...]}> ->
   {:ProjectionType string
    :NonKeyAttributes [string ...]}"
  [projection]
  (let [ptype (if (utils/coll?* projection) :include projection)]
    (cond-> {:ProjectionType (utils/enum ptype)}
            (= ptype :include) (assoc :NonKeyAttributes (mapv name projection)))))

(defn- provisioned-throughput
  "Returns a {:ReadCapacityUnits long, :WriteCapacityUnits long}"
  [{read-units :read write-units :write :as throughput}]
  (assert (and read-units write-units)
          (str "Malformed throughput: " throughput))
  {:ReadCapacityUnits (long read-units) :WriteCapacityUnits (long write-units)})

(defn- keydefs "[<name> <type>] defs -> {:AttributeName string, :AttributeType string}"
  [hash-keydef range-keydef lsindexes gsindexes]
  (let [defs (->> (conj [] hash-keydef range-keydef)
                  (concat (mapv :range-keydef lsindexes))
                  (concat (mapv :hash-keydef gsindexes))
                  (concat (mapv :range-keydef gsindexes))
                  (filterv identity)
                  (distinct))]
    (mapv
      (fn [[key-name key-type :as def]]
        (assert (and key-name key-type) (str "Malformed keydef: " def))
        (assert (#{:s :n :ss :ns :b :bs} key-type)
                (str "Invalid keydef type: " key-type))
        {:AttributeName (name key-name)
         :AttributeType (utils/enum key-type)})
      defs)))

(defn- local-2nd-indexes
  "Implementation detail.
  [{:name _ :range-keydef _ :projection _} ...] indexes ->
  [{:IndexName  string
    :KeySchema  [{:AttributeName string :KeyType string}]
    :Projection {:ProjectionType string
                 :NonKeyAttributes [string]}} ...]"
  [hash-keydef indexes]
  (when indexes
    (mapv
      (fn [{index-name :name
            :keys      [range-keydef projection]
            :or        {projection :all}
            :as        index}]
        (assert (and index-name range-keydef projection)
                (str "Malformed local secondary index (LSI): " index))
        {:IndexName  (name index-name)
         :KeySchema  (key-schema hash-keydef range-keydef)
         :Projection (proj-def projection)})
      indexes)))

(defn- global-2nd-indexes
  "Implementation detail.
  [{:name _ :hash-keydef _ :range-keydef _ :projection _ :throughput _} ...] indexes ->
  [{:IndexName string
    :KeySchema [{:AttributeName string :KeyType string} ...]
    :Projection {:ProjectionType string :NonKeyAttributes [string]}
    :ProvisionedThroughput {:ReadCapacityUnits long, :WriteCapacityUnits long}} ...]"
  [indexes billing-mode]
  (when indexes
    (mapv
      (fn [{index-name :name
            :keys      [hash-keydef range-keydef throughput projection]
            :or        {projection :all}
            :as        index}]
        (assert (and index-name hash-keydef projection (or
                                                         (not (= :pay-per-request billing-mode))
                                                         (and (= :pay-per-request billing-mode) (not throughput))))
                (str "Malformed global secondary index (GSI): " index))
        (cond-> {:IndexName  (name index-name)
                 :KeySchema  (key-schema hash-keydef range-keydef)
                 :Projection (proj-def projection)}
                throughput (assoc :ProvisionedThroughput (provisioned-throughput throughput))))
      indexes)))

(defn- stream-specification
  "Implementation detail."
  [{:keys [enabled? view-type]}]
  (cond-> {}
          (not (nil? enabled?)) (assoc :StreamEnabled enabled?)
          view-type (assoc :StreamViewType (utils/enum view-type))))

(defn- create-table-request
  "Implementation detail."
  [table-name hash-keydef
   & [{:keys [range-keydef throughput lsindexes gsindexes stream-spec billing-mode]
       :or   {billing-mode :provisioned} :as opts}]]
  (assert (not (and throughput (= :pay-per-request billing-mode))) "Can't specify :throughput and :pay-per-request billing-mode")
  (let [lsindexes (or lsindexes (:indexes opts))]
    (cond-> {:TableName            (name table-name)
             :KeySchema            (key-schema hash-keydef range-keydef)
             :BillingMode          (str/upper-case (str/replace (name billing-mode) \- \_))
             :AttributeDefinitions (keydefs hash-keydef range-keydef lsindexes gsindexes)}
            throughput (assoc :ProvisionedThroughput (provisioned-throughput (or throughput {:read 1 :write 1})))
            lsindexes (assoc :LocalSecondaryIndexes (local-2nd-indexes hash-keydef lsindexes))
            gsindexes (assoc :GlobalSecondaryIndexes (global-2nd-indexes gsindexes billing-mode))
            stream-spec (assoc :StreamSpecification (stream-specification stream-spec)))))

(defn create-table
  "Creates a table with options:
    hash-keydef   - [<name> <#{:s :n :ss :ns :b :bs}>].
    :range-keydef - [<name> <#{:s :n :ss :ns :b :bs}>].
    :throughput   - {:read <units> :write <units>}.
    :billing-mode - :provisioned | :pay-per-request ; defaults to provisioned
    :block?       - Block for table to actually be active?
    :lsindexes    - [{:name _ :range-keydef _
                      :projection <#{:all :keys-only [<attr> ...]}>}].
    :gsindexes    - [{:name _ :hash-keydef _ :range-keydef _
                      :projection <#{:all :keys-only [<attr> ...]}>
                      :throughput _}].
    :stream-spec  - {:enabled? <default true if spec is present>
                     :view-type <#{:keys-only :new-image :old-image :new-and-old-images}>}"
  [client-opts table-name hash-keydef
   & [{:keys [block?] :as opts}]]
  (let [request (create-table-request table-name hash-keydef opts)
        response (invoke-with-ex (db-client client-opts)
                                 {:op      :CreateTable
                                  :request request})]
    (if-not block?
      (format-table (:TableDescription response))
      @(table-status-watch client-opts table-name :creating))))

(defn ensure-table
  "Creates a table if it doesn't already exist."
  [client-opts table-name hash-keydef & [opts]]
  (when-not (describe-table client-opts table-name)
    (create-table client-opts table-name hash-keydef opts)))

;;;; Table updates

(defn- global-2nd-index-updates
  "Validate new GSI configuration and create a gsi update map"
  [{:keys [billing-mode] :as table-desc}
   {index-name :name
    :keys      [hash-keydef range-keydef throughput projection operation]
    :or        {projection :all}
    :as        index}]
  (case operation
    :create (do
              (assert (and index-name hash-keydef projection (or (and throughput
                                                                      (not= :pay-per-request (:name billing-mode)))
                                                                 (and (not throughput)
                                                                      (= :pay-per-request (:name billing-mode)))))
                      (str "Malformed global secondary index (GSI): " index))
              {:Create (cond-> {:IndexName  (name index-name)
                                :KeySchema  (key-schema hash-keydef range-keydef)
                                :Projection (proj-def projection)}
                               throughput (assoc :ProvisionedThroughput (provisioned-throughput throughput)))})

    :update {:Update {:IndexName             (name index-name)
                      :ProvisionedThroughput (provisioned-throughput throughput)}}

    :delete {:Delete {:IndexName (name index-name)}}

    nil))

(defn- update-table-request
  "Implementation detail."
  [table table-desc {:keys [throughput gsindexes stream-spec billing-mode] :as params}]
  (assert (not (and throughput
                    (= :pay-per-request billing-mode))) "Can't specify :throughput and :pay-per-request billing-mode")
  (let [gsindexes (cond-> gsindexes (map? gsindexes) vector)
        attr-defs (keydefs nil nil nil gsindexes)]
    (cond-> {:TableName (name table)}
            throughput (assoc :ProvisionedThroughput (provisioned-throughput throughput))
            billing-mode (assoc :BillingMode (utils/enum billing-mode))
            gsindexes (assoc :GlobalSecondaryIndexUpdates (mapv (partial global-2nd-index-updates table-desc) gsindexes))
            stream-spec (assoc :StreamSpecification (stream-specification stream-spec))
            (seq attr-defs) (assoc :AttributeDefinitions attr-defs))))

(defn- validate-update-opts [table-desc {:keys [throughput billing-mode] :as params}]
  (let [{read* :read write* :write} throughput
        current-throughput (:throughput table-desc)
        {:keys [read write num-decreases-today]} current-throughput
        read* (or read* read)
        write* (or write* write)
        decreasing? (or (< read* read) (< write* write))]
    (cond

      (and throughput
           (= :pay-per-request billing-mode))
      (throw (Exception. "Can't specify :throughput and :pay-per-request billing-mode"))

      (and (not throughput)
           (= :pay-per-request billing-mode))
      params

      ;; Hard API limit
      (and decreasing? (>= num-decreases-today 4))
      (throw (Exception. (str "API Limit - Max 4 decreases per 24hr period")))

      ;; Only send a throughput update req if it'd actually change throughput
      (and (= (:read throughput) (:read current-throughput))
           (= (:write throughput) (:write current-throughput)))
      (dissoc params :throughput)

      :else params)))

(defn update-table
  "Returns a future which updates table and returns the post-update table
  description.

  Update opts:
    :throughput   - {:read <units> :write <units>}
    :billing-mode - :provisioned | :pay-per-request   ; defaults to provisioned
    :gsindexes    - {:operation                       ; e/o #{:create :update :delete}
                    :name                             ; Required
                    :throughput                       ; Only for :update / :create
                    :hash-keydef                      ; Only for :create
                    :range-keydef                     ;
                    :projection                       ; e/o #{:all :keys-only [<attr> ...]}}
    :stream-spec  - {:enabled?                        ;
                    :view-type                        ; e/o #{:keys-only :new-image :old-image :old-and-new-images}}

  Only one global secondary index operation can take place at a time.
  In order to change a stream view-type, you need to disable and re-enable the stream."
  [client-opts table update-opts & [{:keys [span-reqs]}]]
  (let [table-desc (describe-table client-opts table)
        status (:status table-desc)
        update-opts (validate-update-opts table-desc update-opts)]

    (cond
      (not= status :active)
      (throw (Exception. (str "Invalid table status: " status)))

      :else
      (future
        ;; If we are not receiving any actual update requests, or
        ;; they were all cleared out by validation, simply return
        ;; the same table description
        (if (empty? update-opts)
          table-desc
          (do
            (invoke-with-ex (db-client client-opts)
                            {:op      :UpdateTable
                             :request (update-table-request table table-desc update-opts)})
            ;; Returns _new_ descr when ready:
            @(table-status-watch client-opts table :updating)))))))

(defn delete-table
  "Deletes a table, go figure."
  [client-opts table]
  (-> (invoke-with-ex (db-client client-opts)
                      {:op      :DeleteTable
                       :request {:TableName (name table)}})
      (:TableDescription)
      (format-table)))

;;;; Items

(defn- get-item-request
  "Implementation detail."
  [table prim-kvs & [{:keys [attrs consistent? return-cc? proj-expr expr-attr-names]}]]
  (cond-> {:TableName (name table)
           :Key       (clj-item->db-item prim-kvs)}
          consistent? (assoc :ConsistentRead consistent?)
          attrs (assoc :AttributesToGet (mapv name attrs))
          proj-expr (assoc :ProjectionExpression proj-expr)
          expr-attr-names (assoc :ExpressionAttributeNames expr-attr-names)
          return-cc? (assoc :ReturnConsumedCapacity (utils/enum :total))))

(defn get-item
  "Retrieves an item from a table by its primary key with options:
    prim-kvs         - {<hash-key> <val>} or {<hash-key> <val> <range-key> <val>}.
    :attrs           - Attrs to return, [<attr> ...].
    :proj-expr       - Projection expression as a string
    :expr-attr-names - Map of strings for ExpressionAttributeNames
    :consistent?     - Use strongly (rather than eventually) consistent reads?"
  [client-opts table prim-kvs & [opts]]
  (let [response (invoke-with-ex (db-client client-opts)
                                 {:op      :GetItem
                                  :request (get-item-request table prim-kvs opts)})]
    (some-> (:Item response) (format-item-result (:ConsumedCapacity response)))))

(defn- expected-vals
  "{<attr> <cond> ...} -> {<attr> ExpectedAttributeValue ...}"
  [expected-map]
  (when (seq expected-map)
    (utils/name-map
      #(case %
         :exists {:ComparisonOperator "NOT_NULL"}
         :not-exists {:Exists false}
         (if (vector? %)
           {:ComparisonOperator (enum-op (first %))
            :AttributeValueList (mapv serialize (rest %))}
           {:Value (serialize %)}))
      expected-map)))

(defn- clj->db-expr-vals-map [m] (enc/map-vals serialize m))

(def ^:private deprecation-warning-expected_
  (delay
    (println "Faraday WARNING: `:expected` option is deprecated in favor of `:cond-expr`")))

(def ^:private deprecation-warning-update-map_
  (delay
    (println "Faraday WARNING: `update-map` is deprecated in favor of `:update-expr`")))

(defn- put-item-request
  [table item &
   [{:keys [return expected return-cc? cond-expr expr-attr-names expr-attr-vals]
     :or   {return :none}}]]
  (cond-> {:TableName (name table)
           :Item      (clj-item->db-item item)}
          expected (assoc :Expected (expected-vals expected))
          cond-expr (assoc :ConditionExpression cond-expr)
          expr-attr-names (assoc :ExpressionAttributeNames expr-attr-names)
          expr-attr-vals (assoc :ExpressionAttributeValues
                                (clj->db-expr-vals-map expr-attr-vals))
          return (assoc :ReturnValues (utils/enum return))
          return-cc? (assoc :ReturnConsumedCapacity (utils/enum :total))))

(defn put-item
  "Adds an item (Clojure map) to a table with options:
    :return          - e/o #{:none :all-old}
    :cond-expr       - \"attribute_exists(attr_name) AND|OR ...\"
    :expr-attr-names - {\"#attr_name\" \"name\"}
    :expr-attr-vals  - {\":attr_value\" \"value\"}
    :expected        - DEPRECATED in favor of `:cond-expr`,
      {<attr> <#{:exists :not-exists [<comparison-operator> <value>] <value>}> ...}
      With comparison-operator e/o #{:eq :le :lt :ge :gt :begins-with :between}."

  [client-opts table item &
   [{:keys [return expected return-cc? cond-expr]
     :as   opts}]]

  (assert (not (and expected cond-expr))
          "Only one of :expected or :cond-expr should be provided")

  (when expected @deprecation-warning-expected_)

  (let [response (invoke-with-ex (db-client client-opts)
                                 {:op      :PutItem
                                  :request (put-item-request table item opts)})]
    (format-item-result (:Attributes response) (:ConsumedCapacity response))))

(defn- attribute-updates
  "{<attr> [<action> <value>] ...} -> {<attr> AttributeValueUpdate ...}"
  [update-map]
  (when (seq update-map)
    (utils/name-map
      (fn [[action val]]
        (cond-> {:Action (utils/enum action)}
                (or (not= action :delete) (set? val))
                (assoc :Value (serialize val))))
      update-map)))

(defn- update-item-request
  [table prim-kvs &
   [{:keys [return expected return-cc? update-map
            cond-expr update-expr expr-attr-names expr-attr-vals]
     :or   {return :none}}]]
  (cond-> {:TableName (name table)
           :Key       (clj-item->db-item prim-kvs)}
          update-map (assoc :AttributeUpdates (attribute-updates update-map))
          update-expr (assoc :UpdateExpression update-expr)
          expr-attr-names (assoc :ExpressionAttributeNames expr-attr-names)
          expr-attr-vals (assoc :ExpressionAttributeValues
                                (clj->db-expr-vals-map expr-attr-vals))
          expected (assoc :Expected (expected-vals expected))
          cond-expr (assoc :ConditionExpression cond-expr)
          return (assoc :ReturnValues (utils/enum return))
          return-cc? (assoc :ReturnConsumedCapacity (utils/enum :total))))

(defn update-item
  "Updates an item in a table by its primary key with options:
    prim-kvs         - {<hash-key> <val>} or {<hash-key> <val> <range-key> <val>}
    :update-map      - DEPRECATED in favor of `:update-expr`,
                       {<attr> [<#{:put :add :delete}> <optional value>]}
    :cond-expr       - \"attribute_exists(attr_name) AND|OR ...\"
    :update-expr     - \"SET #attr_name = :attr_value\"
    :expr-attr-names - {\"#attr_name\" \"name\"}
    :expr-attr-vals  - {\":attr_value\" \"value\"}
    :return          - e/o #{:none :all-old :updated-old :all-new :updated-new}
    :expected        - DEPRECATED in favor of `:cond-expr`,
      {<attr> <#{:exists :not-exists [<comparison-operator> <value>] <value>}> ...}
      With comparison-operator e/o #{:eq :le :lt :ge :gt :begins-with :between}."
  [client-opts table prim-kvs &
   [{:keys [update-map return expected return-cc? cond-expr update-expr]
     :as   opts}]]

  (assert (not (and expected cond-expr))
          "Only one of :expected or :cond-expr should be provided")

  (assert (not (and update-expr (seq update-map)))
          "Only one of 'update-map' or :update-expr should be provided")

  (when expected @deprecation-warning-expected_)
  (when (seq update-map) @deprecation-warning-update-map_)

  (let [response (invoke-with-ex (db-client client-opts)
                                 {:op      :UpdateItem
                                  :request (update-item-request table prim-kvs opts)})]
    (format-item-result (:Attributes response) (:ConsumedCapacity response))))

(defn- delete-item-request
  "Implementation detail."
  [table prim-kvs & [{:keys [return expected return-cc? cond-expr expr-attr-vals expr-attr-names]
                      :or   {return :none}}]]
  (cond-> {:TableName (name table)
           :Key       (clj-item->db-item prim-kvs)}
          cond-expr (assoc :ConditionExpression cond-expr)
          expr-attr-names (assoc :ExpressionAttributeNames expr-attr-names)
          expr-attr-vals (assoc :ExpressionAttributeValues (clj->db-expr-vals-map expr-attr-vals))
          expected (assoc :Expected (expected-vals expected))
          return (assoc :ReturnValues (utils/enum return))
          return-cc? (assoc :ReturnConsumedCapacity (utils/enum :total))))

(defn delete-item
  "Deletes an item from a table by its primary key.
  See `put-item` for option docs."
  [client-opts table prim-kvs & [{:keys [return expected return-cc?]
                                  :as   opts}]]
  (let [response (invoke-with-ex (db-client client-opts)
                                 {:op      :DeleteItem
                                  :request (delete-item-request table prim-kvs opts)})]
    (format-item-result (:Attributes response) (:ConsumedCapacity response))))

;;;; Batch ops

;; TODO Do we want to change this to `false` in a future breaking release?
;; Would require updating `batch-get-item`, `batch-write-item` docstrings and
;; updating consumers of these fns in tests
(def ^:private attr-multi-vs?-default true)
(def ^:dynamic *attr-multi-vs?*
  "Treat attribute vals as expansions rather than literals?
  nil => use `attr-multi-vs?-default` (currently `true` though this may be
  changed in future to better support DDB's new collection types,
  Ref. https://github.com/ptaoussanis/faraday/issues/63)." nil)

(defmacro with-attr-multi-vs [& body] `(binding [*attr-multi-vs?* true] ~@body))
(defmacro without-attr-multi-vs [& body] `(binding [*attr-multi-vs?* false] ~@body))

(defn- attr-multi-vs
  "Implementation detail.
  [{<attr> <v-or-vs*> ...} ...]* -> [{<attr> <v> ...} ...] (* => optional vec)"
  [attr-multi-vs-map]
  (let [expand? *attr-multi-vs?*
        expand? (if (nil? expand?) attr-multi-vs?-default expand?)
        ensure-sequential (fn [x] (if (sequential? x) x [x]))]
    (if-not expand?
      (mapv clj-item->db-item (ensure-sequential attr-multi-vs-map))
      (reduce
        (fn [r attr-multi-vs]
          (let [attrs (keys attr-multi-vs)
                vs (mapv ensure-sequential (vals attr-multi-vs))]
            (when (> (count (filter next vs)) 1)
              (-> (Exception. "Can range over only a single attr's values")
                  (throw)))
            (into r (mapv (comp clj-item->db-item (partial zipmap attrs))
                          (apply utils/cartesian-product vs)))))
        [] (ensure-sequential attr-multi-vs-map)))))

(defn- batch-request-items
  "Implementation detail.
  {<table> <request> ...} -> {<table> KeysAndAttributes> ...}"
  [requests]
  (utils/name-map
    (fn [{:keys [prim-kvs attrs consistent?]}]
      (cond-> {:Keys (attr-multi-vs prim-kvs)}
              attrs (assoc :AttributesToGet (mapv name attrs))
              consistent? (assoc :ConsistentRead consistent?)))
    requests))

(defn- merge-more
  "Enables auto paging for batch batch-get/write and query/scan requests.
  Particularly useful for throughput limitations."
  [more-f {max-reqs :max :keys [throttle-ms]} last-result]
  (loop [{:keys [unprocessed last-prim-kvs] :as last-result} last-result idx 1]
    (let [more (or unprocessed last-prim-kvs)]
      (if (or (empty? more) (nil? max-reqs) (>= idx max-reqs))
        (if-let [items (:items last-result)]
          (with-meta items (dissoc last-result :items))
          last-result)
        (let [merge-results (fn [l r] (cond (number? l) (+ l r)
                                            (vector? l) (into l r)
                                            :else r))]
          (when throttle-ms (Thread/sleep ^long throttle-ms))
          (recur (let [next-result (more-f more)]
                   (merge (merge-with merge-results (dissoc last-result :items) (dissoc next-result :items))
                          {:items (enc/nested-merge-with into (:items last-result) (:items next-result))}))
                 (inc idx)))))))

(defn- batch-get-item-request
  "Implementation detail."
  [return-cc? raw-req]
  (cond-> {:RequestItems raw-req}
          return-cc? (assoc :ReturnConsumedCapacity (utils/enum :total))))

(defn batch-get-item
  "Retrieves a batch of items in a single request.
  Limits apply, Ref. http://goo.gl/Bj9TC.

  (batch-get-item client-opts
    {:users   {:prim-kvs {:name \"alice\"}}
     :posts   {:prim-kvs {:id [1 2 3]}
               :attrs    [:timestamp :subject]
               :consistent? true}
     :friends {:prim-kvs [{:catagory \"favorites\" :id [1 2 3]}
                          {:catagory \"recent\"    :id [7 8 9]}]}})

  :span-reqs - {:max _ :throttle-ms _} allows a number of requests to
  automatically be stitched together (to exceed throughput limits, for example)."
  [client-opts requests
   & [{:keys [return-cc? span-reqs attr-multi-vs?] :as opts}]]
  (binding [*attr-multi-vs?* attr-multi-vs?]
    (let [run1
          (fn [raw-req]
            (let [response (invoke-with-ex (db-client client-opts)
                                           {:op      :BatchGetItem
                                            :request (batch-get-item-request return-cc? raw-req)})]
              (format-batch-get-item-result response)))]
      (merge-more run1 span-reqs (run1 (batch-request-items requests))))))

(defn- write-request
  "Implementation detail."
  [action item]
  (case action
    :put {:PutRequest {:Item item}}
    :delete {:DeleteRequest {:Key item}}))

(defn- batch-write-item-request
  "Implementation detail."
  [return-cc? raw-req]
  (cond-> {:RequestItems raw-req}
          return-cc? (assoc :ReturnConsumedCapacity (utils/enum :total))))

(defn batch-write-item
  "Executes a batch of Puts and/or Deletes in a single request.
   Limits apply, Ref. http://goo.gl/Bj9TC. No transaction guarantees are
   provided, nor conditional puts. Request execution order is undefined.

   (batch-write-item client-opts
     {:users {:put    [{:user-id 1 :username \"sally\"}
                       {:user-id 2 :username \"jane\"}]
              :delete [{:user-id [3 4 5]}]}})

  :span-reqs - {:max _ :throttle-ms _} allows a number of requests to
  automatically be stitched together (to exceed throughput limits, for example)."
  [client-opts requests &
   [{:keys [return-cc? span-reqs attr-multi-vs?] :as opts}]]

  (binding [*attr-multi-vs?* attr-multi-vs?]
    (let [run1
          (fn [raw-req]
            (let [response (invoke-with-ex (db-client client-opts)
                                           {:op      :BatchWriteItem
                                            :request (batch-write-item-request return-cc? raw-req)})]
              (format-batch-write-item-result response)))]
      (merge-more run1 span-reqs
                  (run1
                    (utils/name-map
                      ;; {<table> <table-reqs> ...} -> {<table> [WriteRequest ...] ...}
                      (fn [table-request]
                        (reduce into []
                                (for [action (keys table-request)
                                      :let [items (attr-multi-vs (table-request action))]]
                                  (mapv (partial write-request action) items))))
                      requests))))))

(defmulti ^:private transact-write-item first)

(defmethod transact-write-item :cond-check
  [[_ {:keys [table-name prim-kvs cond-expr
              expr-attr-names expr-attr-vals return] :as request
       :or {return :none}}]]
  {:ConditionCheck (cond-> {:TableName           (name table-name)
                            :Key                 (clj-item->db-item prim-kvs)
                            :ConditionExpression cond-expr}
                           expr-attr-names (assoc :ExpressionAttributeNames expr-attr-names)
                           expr-attr-vals (assoc :ExpressionAttributeValues (clj->db-expr-vals-map expr-attr-vals))
                           return (assoc :ReturnValuesOnConditionCheckFailure (utils/enum return)))})

(defmethod transact-write-item :put
  [[_ {:keys [table-name item cond-expr expr-attr-names expr-attr-vals return]
       :or   {return :none}}]]
  {:Put (cond-> {:TableName (name table-name)
                 :Item      (clj-item->db-item item)}
                cond-expr (assoc :ConditionExpression cond-expr)
                expr-attr-names (assoc :ExpressionAttributeNames expr-attr-names)
                expr-attr-vals (assoc :ExpressionAttributeValues (clj->db-expr-vals-map expr-attr-vals))
                return (assoc :ReturnValuesOnConditionCheckFailure (utils/enum return)))})

(defmethod transact-write-item :delete
  [[_ {:keys [table-name prim-kvs cond-expr expr-attr-names expr-attr-vals return]
       :or   {return :none}}]]
  {:Delete (cond-> {:TableName (name table-name)
                    :Key       (clj-item->db-item prim-kvs)}
                   cond-expr (assoc :ConditionExpression cond-expr)
                   expr-attr-names (assoc :ExpressionAttributeNames expr-attr-names)
                   expr-attr-vals (assoc :ExpressionAttributeValues (clj->db-expr-vals-map expr-attr-vals))
                   return (assoc :ReturnValuesOnConditionCheckFailure (utils/enum return)))})

(defmethod transact-write-item :update
  [[_ {:keys [table-name prim-kvs update-expr
              cond-expr expr-attr-names expr-attr-vals return]
       :or   {return :none}}]]
  {:Update (cond-> {:TableName        (name table-name)
                    :Key              (clj-item->db-item prim-kvs)
                    :UpdateExpression update-expr}
                   cond-expr (assoc :ConditionExpression cond-expr)
                   expr-attr-names (assoc :ExpressionAttributeNames expr-attr-names)
                   expr-attr-vals (assoc :ExpressionAttributeValues (clj->db-expr-vals-map expr-attr-vals))
                   return (assoc :ReturnValuesOnConditionCheckFailure (utils/enum return)))})

(defn- transact-write-items-request
  [{:keys [client-req-token return-cc? items]}]
  (cond-> {:TransactItems (mapv transact-write-item items)}
          client-req-token (assoc :ClientRequestToken client-req-token)
          return-cc? (assoc :ReturnConsumedCapacity (utils/enum :total))))

(defn transact-write-items
  "Executes a transaction comprising puts, updates, deletes and cond-checks; either all succeed or all fail.
   Execution order is the order in the items vector.

   For example:
   (far/transact-write-items client-opts
                             {:items [[:cond-check {:table-name ttable
                                                    :prim-kvs {:id 300}
                                                    :cond-expr \"attribute_exists(#id)\"
                                                    :expr-attr-names {\"#id\" \"id\"}}]
                                      [:put {:table-name ttable
                                             :item i2
                                             :cond-expr \"attribute_not_exists(#id)\"
                                             :expr-attr-names {\"#id\" \"id\"}}]
                                      [:delete {:table-name ttable
                                                :prim-kvs {:id 303}
                                                :cond-expr \"attribute_exists(#id)\"
                                                :expr-attr-names {\"#id\" \"id\"}}]
                                      [:update {:table-name ttable
                                                :prim-kvs {:id 300}
                                                :update-expr \"SET #name = :name\"
                                                :cond-expr \"attribute_exists(#id) AND #name = :oldname\"
                                                :expr-attr-names {\"#id\" \"id\", \"#name\" \"name\"}
                                                :expr-attr-vals {\":oldname\" \"foo\", \":name\" \"foobar\"}}]]})
  returns {:cc-units {<table> <consumed-capacity>}}}"
  [client-opts raw-req]
  (let [response (invoke-with-ex (db-client client-opts)
                                 {:op      :TransactWriteItems
                                  :request (transact-write-items-request raw-req)})]
    {:cc-units (batch-cc-units (:ConsumedCapacity response))}))

(defn- transact-get-item
  [{:keys [table-name prim-kvs expr-attr-names proj-expr]}]
  {:Get (cond-> {:TableName (name table-name)
                 :Key       (clj-item->db-item prim-kvs)}
                expr-attr-names (assoc :ExpressionAttributeNames expr-attr-names)
                proj-expr (assoc :ProjectionExpression proj-expr))})

(defn- transact-get-items-request
  [{:keys [return-cc? items]}]
  (cond-> {:TransactItems (mapv transact-get-item items)}
          return-cc? (assoc :ReturnConsumedCapacity (utils/enum :total))))

(defn transact-get-items
  "Transactionally fetches requested items by primary key

  e.g.
  (far/transact-get-items client-opts
                          {:return-cc? true
                           :items [{:table-name ttable
                                    :prim-kvs {:id 305}}
                                   {:table-name ttable
                                   :prim-kvs {:id 306}}]})
  returns {:items [<item>] :cc-units {<table> <consumed-capacity>}}"
  [client-opts raw-req]
  (let [response (invoke-with-ex (db-client client-opts)
                                 {:op      :TransactGetItems
                                  :request (transact-get-items-request raw-req)})]
    {:items (mapv (comp db-item->clj-item :Item) (:Responses response))
     :cc-units (batch-cc-units (:ConsumedCapacity response))}))

;;;; API - queries & scans

(defn- query|scan-conditions
  "{<attr> [operator <val-or-vals>] ...} -> {<attr> Condition ...}"
  [conditions]
  (when (seq conditions)
    (utils/name-map
      (fn [[operator val-or-vals & more :as condition]]
        (assert (not (seq more)) (str "Malformed condition: " condition))
        (let [vals (if (utils/coll?* val-or-vals) val-or-vals [val-or-vals])]
          {:ComparisonOperator (enum-op operator)
           :AttributeValueList (mapv serialize vals)}))
      conditions)))

(defn- query-request
  "Implementation detail."
  [table prim-key-conds
   & [{:keys [last-prim-kvs query-filter span-reqs return index order limit consistent?
              proj-expr filter-expr expr-attr-vals expr-attr-names return-cc?] :as opts
       :or {order :asc}}]]
  (cond-> {:TableName        (name table)
           :KeyConditions    (query|scan-conditions prim-key-conds)
           :ScanIndexForward (case order :asc true :desc false)}
          last-prim-kvs (assoc :ExclusiveStartKey (clj-item->db-item last-prim-kvs))
          query-filter (assoc :QueryFilter (query|scan-conditions query-filter))
          proj-expr (assoc :ProjectionExpression proj-expr)
          filter-expr (assoc :FilterExpression filter-expr)
          expr-attr-names (assoc :ExpressionAttributeNames expr-attr-names)
          expr-attr-vals (assoc :ExpressionAttributeValues (clj->db-expr-vals-map expr-attr-vals))
          limit (assoc :Limit (int limit))
          index (assoc :IndexName (name index))
          consistent? (assoc :ConsistentRead consistent?)
          (utils/coll?* return) (assoc :AttributesToGet (mapv name return))
          return-cc? (assoc :ReturnConsumedCapacity (utils/enum :total))
          (and return (not (utils/coll?* return))) (assoc :Select (utils/enum return))))

(defn query
  "Retrieves items from a table (indexed) with options:
    prim-key-conds   - {<key-attr> [<comparison-operator> <val-or-vals>] ...}.
    :last-prim-kvs   - Primary key-val from which to eval, useful for paging.
    :query-filter    - {<key-attr> [<comparison-operator> <val-or-vals>] ...}.
    :proj-expr       - Projection expression string
    :filter-expr     - Filter expression string
    :expr-attr-names - Expression attribute names, as a map of {\"#attr_name\" \"name\"}
    :expr-attr-vals  - Expression attribute values, as a map {\":attr_value\" \"value\"}
    :span-reqs       - {:max _ :throttle-ms _} controls automatic multi-request
                       stitching.
    :return          - e/o #{:all-attributes :all-projected-attributes :count
                             [<attr> ...]}.
    :index           - Name of a local or global secondary index to query.
    :order           - Index scaning order e/o #{:asc :desc}.
    :limit           - Max num >=1 of items to eval (≠ num of matching items).
                       Useful to prevent harmful sudden bursts of read activity.
    :consistent?     - Use strongly (rather than eventually) consistent reads?

  (create-table client-opts :my-table [:name :s]
    {:range-keydef [:age :n] :block? true})

  (do (put-item client-opts :my-table {:name \"Steve\" :age 24})
      (put-item client-opts :my-table {:name \"Susan\" :age 27}))
  (query client-opts :my-table {:name [:eq \"Steve\"]
                                :age  [:between [10 30]]})
  => [{:age 24, :name \"Steve\"}]

  comparison-operators e/o #{:eq :le :lt :ge :gt :begins-with :between}.

  For unindexed item retrievel see `scan`.

  Ref. http://goo.gl/XfGKW for query+scan best practices."
  [client-opts table prim-key-conds
   & [{:keys [last-prim-kvs query-filter span-reqs return index order limit consistent?
              return-cc?] :as opts}]]
  (let [run1
        (fn [last-prim-kvs]
          (update-in
            (-> (invoke-with-ex (db-client client-opts)
                                {:op      :Query
                                 :request (query-request table prim-key-conds
                                                         (assoc opts :last-prim-kvs last-prim-kvs))})
                (format-query|scan-result))
            [:items]
            (fn [items] {table items})))
        result (merge-more run1 span-reqs (run1 last-prim-kvs))]
    (with-meta (get result table) (meta result))))

(defn- scan-request
  [table
   & [{:keys [attr-conds last-prim-kvs return limit total-segments
              proj-expr expr-attr-names filter-expr expr-attr-vals
              consistent? index segment return-cc?] :as opts}]]
  (cond-> {:TableName (name table)}
          attr-conds (assoc :ScanFilter (query|scan-conditions attr-conds))
          expr-attr-names (assoc :ExpressionAttributeNames expr-attr-names)
          expr-attr-vals (assoc :ExpressionAttributeValues (clj->db-expr-vals-map expr-attr-vals))
          filter-expr (assoc :FilterExpression filter-expr)
          index (assoc :IndexName (name index))
          last-prim-kvs (assoc :ExclusiveStartKey (clj-item->db-item last-prim-kvs))
          limit (assoc :Limit (int limit))
          proj-expr (assoc :ProjectionExpression proj-expr)
          total-segments (assoc :TotalSegments (int total-segments))
          segment (assoc :Segment (int segment))
          consistent? (assoc :ConsistentRead consistent?)
          (utils/coll?* return) (assoc :AttributesToGet (mapv name return))
          return-cc? (assoc :ReturnConsumedCapacity (utils/enum :total))
          (and return (not (utils/coll?* return))) (assoc :Select (utils/enum return))))

(defn scan
  "Retrieves items from a table (unindexed) with options:
    :attr-conds      - {<attr> [<comparison-operator> <val-or-vals>] ...}.
    :expr-attr-names - Expression attribute names, as a map of {\"#attr_name\" \"name\"}
    :expr-attr-vals  - Expression attribute names, as a map of {\":attr\" \"value\"}
    :filter-expr     - Filter expression string
    :index           - Index name to use.
    :proj-expr       - Projection expression as a string
    :limit           - Max num >=1 of items to eval (≠ num of matching items).
                       Useful to prevent harmful sudden bursts of read activity.
    :last-prim-kvs   - Primary key-val from which to eval, useful for paging.
    :span-reqs       - {:max _ :throttle-ms _} controls automatic multi-request
                       stitching.
    :return          - e/o #{:all-attributes :all-projected-attributes :count
                             [<attr> ...]}.
    :total-segments  - Total number of parallel scan segments.
    :segment         - Calling worker's segment number (>=0, <=total-segments).
    :consistent?     - Use strongly (rather than eventually) consistent reads?

  comparison-operators e/o #{:eq :le :lt :ge :gt :begins-with :between :ne
                             :not-null :null :contains :not-contains :in}.

  (create-table client-opts :my-table [:name :s]
    {:range-keydef [:age :n] :block? true})

  (do (put-item client-opts :my-table {:name \"Steve\" :age 24})
      (put-item client-opts :my-table {:name \"Susan\" :age 27}))
  (scan client-opts :my-table {:attr-conds {:age [:in [24 27]]}})
  => [{:age 24, :name \"Steve\"} {:age 27, :name \"Susan\"}]

  For automatic parallelization & segment control see `scan-parallel`.
  For indexed item retrievel see `query`.

  Ref. http://goo.gl/XfGKW for query+scan best practices."
  [client-opts table
   & [{:keys [attr-conds last-prim-kvs span-reqs return limit total-segments
              filter-expr
              segment return-cc?] :as opts}]]

  (assert (not (and filter-expr (seq attr-conds)))
          "Only one of ':filter-expr' or :attr-conds should be provided")

  (let [run1
        (fn [last-prim-kvs]
          (update-in
            (let [response (invoke-with-ex (db-client client-opts)
                                           {:op      :Scan
                                            :request (scan-request table (assoc opts :last-prim-kvs last-prim-kvs))})]
              (-> (format-query|scan-result response)
                  (assoc :scanned-count (:ScannedCount response))))
            [:items]
            (fn [items] {table items})))
        result (merge-more run1 span-reqs (run1 last-prim-kvs))]
    (with-meta (get result table) (meta result))))

(defn scan-lazy-seq
  "Returns a lazy sequence of items from table. Requests are stitched together to
  produce a continuous sequence, with additional requests occurring only when
  needed.

  See scan for supported options. :span-reqs is ignored."
  [client-opts table {:keys [limit] :as opts}]
  (lazy-seq
    (let [result (scan client-opts table (assoc opts :span-reqs {:max 0}))
          {:keys [last-prim-kvs]} (meta result)
          result-seq (if last-prim-kvs
                       (lazy-cat result
                                 (scan-lazy-seq client-opts table (-> opts
                                                                      (assoc :last-prim-kvs last-prim-kvs)
                                                                      (dissoc :limit))))
                       result)]
      (if limit
        (take limit result-seq)
        result-seq))))

(defn scan-parallel
  "Like `scan` but starts a number of worker threads and automatically handles
  parallel scan options (:total-segments and :segment). Returns a vector of
  `scan` results.

  Ref. http://goo.gl/KLwnn (official parallel scan documentation)."

  ;; TODO GitHub #17:
  ;; http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html
  ;; In a parallel scan, a Scan request that includes ExclusiveStartKey must
  ;; specify the same segment whose previous Scan returned the corresponding
  ;; value of LastEvaluatedKey.

  [client-opts table total-segments & [opts]]
  (let [opts (assoc opts :total-segments total-segments)]
    (->> (mapv (fn [seg] (future (scan client-opts table (assoc opts :segment seg))))
               (range total-segments))
         (mapv deref))))

;;;; DB Streams API
;; Ref. http://docs.aws.amazon.com/dynamodbstreams/latest/APIReference/Welcome.html

(defn- list-streams-request
  [{:keys [table-name limit start-arn]}]
  (cond-> {}
          table-name (assoc :TableName (name table-name))
          limit (assoc :Limit (int limit))
          start-arn (assoc :ExclusiveStartStreamArn start-arn)))

(defn list-streams
  "Returns a lazy sequence of stream descriptions. Each item is a map of:
   {:stream-arn - Stream identifier string
    :table-name - The table name for the stream}

    Options:
     :start-arn  - The stream identifier to start listing from (exclusive)
     :table-name - List only the streams for <table-name>
     :limit      - Retrieve streams in batches of <limit> at a time"
  [client-opts & [{:keys [start-arn table-name limit] :as opts}]]
  (let [client (db-streams-client client-opts)
        step (fn step [stream-arn]
               (lazy-seq
                 (let [response (invoke-with-ex client
                                                {:op      :ListStreams
                                                 :request (list-streams-request (assoc opts :start-arn stream-arn))})
                       last-stream-arn (:LastEvaluatedStreamArn response)
                       streams (mapv format-stream (:Streams response))]
                   (if last-stream-arn
                     (concat streams (step last-stream-arn))
                     (seq streams)))))]
    (step start-arn)))

(defn- describe-stream-request
  [stream-arn {:keys [limit start-shard-id]}]
  (cond-> {}
          stream-arn (assoc :StreamArn stream-arn)
          limit (assoc :Limit (int limit))
          start-shard-id (assoc :ExclusiveStartShardId start-shard-id)))

(defn describe-stream
  "Returns a map describing a stream, or nil if the stream doesn't exist."
  [client-opts stream-arn & [{:keys [limit start-shard-id] :as opts}]]
  (let [response (aws/invoke (db-streams-client client-opts)
                             {:op      :DescribeStream
                              :request (describe-stream-request stream-arn opts)})]
    (when-not (= (:cognitect.aws.error/code response) "ResourceNotFoundException")
      (exceptions! response)
      (format-stream-description (:StreamDescription response)))))

(defn- get-shard-iterator-request
  [stream-arn shard-id iterator-type {:keys [seq-num]}]
  (cond-> {:StreamArn         stream-arn
           :ShardId           shard-id
           :ShardIteratorType (utils/enum iterator-type)}
          seq-num (assoc :SequenceNumber seq-num)))

(defn shard-iterator
  "Returns the iterator string that can be used in the get-stream-records call
   or nil when the stream or shard doesn't exist."
  [client-opts stream-arn shard-id iterator-type
   & [{:keys [seq-num] :as opts}]]
  (let [response (aws/invoke (db-streams-client client-opts)
                             {:op      :GetShardIterator
                              :request (get-shard-iterator-request stream-arn shard-id iterator-type opts)})]
    (when-not (= (:cognitect.aws.error/code response) "ResourceNotFoundException")
      (exceptions! response)
      (:ShardIterator response))))

(defn- get-records-request
  [iter-str {:keys [limit]}]
  (cond-> {:ShardIterator iter-str}
          limit (assoc :Limit (int limit))))

(defn get-stream-records
  [client-opts shard-iterator & [{:keys [limit] :as opts}]]
  (let [response (invoke-with-ex (db-streams-client client-opts)
                                 {:op      :GetRecords
                                  :request (get-records-request shard-iterator opts)})]
    {:next-shard-iterator (:NextShardIterator response)
     :records             (mapv format-record (:Records response))}))

;;;; TTL API

(defn describe-ttl
  "Returns a map describing the TTL configuration for a table, or nil if
  the table does not exist."
  [client-opts table-name]
  (let [response (aws/invoke (db-client client-opts)
                             {:op      :DescribeTimeToLive
                              :request {:TableName (name table-name)}})]
    (when-not (= (:cognitect.aws.error/code response) "ResourceNotFoundException")
      (exceptions! response)
      {:attribute-name (get-in response [:TimeToLiveDescription :AttributeName])
       :status         (utils/un-enum (get-in response [:TimeToLiveDescription :TimeToLiveStatus]))})))

(defn- update-ttl-request
  [{:keys [table-name enabled? key-name]}]
  {:TableName               (name table-name)
   :TimeToLiveSpecification (cond-> {:Enabled enabled?}
                                    key-name (assoc :AttributeName (name key-name)))})

(defn update-ttl
  "Updates the TTL configuration for a table."
  [client-opts table-name enabled? key-name]
  (let [response (invoke-with-ex (db-client client-opts)
                                 {:op      :UpdateTimeToLive
                                  :request (update-ttl-request {:table-name table-name
                                                                :enabled?   enabled?
                                                                :key-name   key-name})})]
    {:enabled?       (get-in response [:TimeToLiveSpecification :Enabled])
     :attribute-name (get-in response [:TimeToLiveSpecification :AttributeName])}))

(defn ensure-ttl
  "Activates TTL with the given key-name iff the TTL configuration is
  not already set in this way."
  [client-opts table-name key-name]
  (let [ttl-spec (describe-ttl client-opts table-name)]
    (when-not (or (= {:attribute-name (name key-name) :status :enabled} ttl-spec)
                  (= {:attribute-name (name key-name) :status :enabling} ttl-spec))
      (update-ttl client-opts table-name true key-name))))

;;;; Misc utils, etc.

(defn items-by-attrs
  "Groups one or more items by one or more attributes, returning a map of form
  {<attr-val> <item> ...} or {{<attr> <val> ...} <item>}.

  Good for collecting batch or query/scan items results into useable maps,
  indexed by their 'primary key' (which, remember, may consist of 1 OR 2 attrs)."
  [attr-or-attrs item-or-items]
  (let [item-by-attrs
        (fn [a-or-as item]
          (if-not (utils/coll?* a-or-as)
            (let [a a-or-as] {(get item a) (dissoc item a)})
            (let [as a-or-as] {(select-keys item as) (apply dissoc item as)})))]
    (if-not (utils/coll?* item-or-items)
      (let [item item-or-items] (item-by-attrs attr-or-attrs item))
      (let [items item-or-items]
        (into {} (mapv (partial item-by-attrs attr-or-attrs) items))))))

(def remove-empty-attr-vals
  "Alpha, subject to change.
  Util to help remove (or coerce to nil) empty val types prohibited by DDB,
  Ref. http://goo.gl/Xg85pO. Note that empty string and binary attributes are
  now permitted: https://amzn.to/3szZ0E5 See also `freeze` as an alternative."
  (let [->?seq (fn [c] (when (seq c) c))]
    (fn f1 [x]
      (cond
        (map? x)
        (->?seq
          (reduce-kv
            (fn [acc k v] (let [v* (f1 v)] (if (nil? v*) acc (assoc acc k v*))))
            {} x))

        (coll? x)
        (->?seq
          (reduce
            (fn rf [acc in] (let [v* (f1 in)] (if (nil? v*) acc (conj acc v*))))
            (if (sequential? x) [] (empty x)) x))

        :else x))))
