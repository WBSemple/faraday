(ns taoensso.faraday2
  "Clojure DynamoDB client using the AWS SDK v2 via https://github.com/cognitect-labs/aws-api"
  (:require [cognitect.aws.client.api :as aws])
  (:import [cognitect.aws.client.impl Client]))

(def ^:private db-client*
  "Returns a Client instance for the supplied client opts:
    (db-client* {:credentials-provider (cognitect.aws.credentials/basic-credentials-provider
                                         {:access-key-id \"ABC\"
                                          :secret-access-key \"XYZ\"})})
    (db-client* {:credentials-provider (cognitect.aws.credentials/profile-credentials-provider
                                         \"myprofile\")})

  See https://github.com/cognitect-labs/aws-api#credentials"
  (memoize (fn [client-opts]
             (aws/client (assoc client-opts :api :dynamodb)))))

(def ^:private db-streams-client*
  "Returns a Client instance for the supplied client opts:
    (db-streams-client* {:credentials-provider (cognitect.aws.credentials/basic-credentials-provider
                                                 {:access-key-id \"ABC\"
                                                  :secret-access-key \"XYZ\"})})
    (db-streams-client* {:credentials-provider (cognitect.aws.credentials/profile-credentials-provider
                                                 \"myprofile\")})

  See https://github.com/cognitect-labs/aws-api#credentials"
  (memoize (fn [client-opts]
             (aws/client (assoc client-opts :api :streams-dynamodb)))))

(defn- db-client ^Client
  [client-opts]
  (db-client* client-opts))

(defn- db-streams-client ^Client
  [client-opts]
  (db-streams-client* client-opts))

(defn list-tables
  "Returns a lazy sequence of table names."
  [client-opts]
  (let [step
        (fn step [^String offset]
          (lazy-seq
            (let [client (db-client client-opts)
                  result (if (nil? offset)
                           (aws/invoke client {:op :ListTables})
                           (aws/invoke client {:op :ListTables :request {:ExclusiveStartTableName offset}}))
                  last-key (:LastEvaluatedTableName result)
                  chunk (map keyword (:TableNames result))]
              (if last-key
                (concat chunk (step (name last-key)))
                chunk))))]
    (step nil)))
