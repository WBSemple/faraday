(ns taoensso.faraday2.tests.main
  (:require
    [clojure.test :refer [deftest is use-fixtures]]
    [cognitect.aws.client.api :as aws]
    [cognitect.aws.credentials :as credentials]
    [taoensso.faraday2 :as far2])
  (:import
    [java.net URI]
    [org.testcontainers.containers GenericContainer]))

(defmethod clojure.test/report :begin-test-var [m]
  (println "\u001B[32mTesting" (-> m :var meta :name) "\u001B[0m"))

;;;; Config & setup

(def ^:dynamic *client-opts*
  (let [endpoint (or (get (System/getenv) "AWS_DYNAMODB_ENDPOINT") "http://localhost:6798")
        url (.toURL (new URI endpoint))]
    {:credentials-provider (credentials/basic-credentials-provider
                             {:access-key-id (or (get (System/getenv) "AWS_DYNAMODB_ACCESS_KEY") "test")
                              :secret-access-key (or (get (System/getenv) "AWS_DYNAMODB_SECRET_KEY") "test")})
     :endpoint-override {:protocol (keyword (.getProtocol url))
                         :hostname (.getHost url)
                         :port (.getPort url)}
     :region "eu-west-2"}))

(defn- dynamodb-local
  [t]
  (let [dynamodb-port 8000
        container (doto (GenericContainer. "amazon/dynamodb-local:2.0.0")
                        (.addExposedPort (int dynamodb-port))
                        (.start))
        local-port (.getMappedPort container dynamodb-port)]
    (with-open [_ container]
      (with-bindings {#'*client-opts* (assoc *client-opts* :endpoint-override {:protocol :http
                                                                               :hostname "localhost"
                                                                               :port local-port})}
        (t)))))

(use-fixtures :once dynamodb-local)

(deftest client-creation
  (let [opts->tables #(aws/invoke (#'far2/db-client %) {:op :ListTables})]
    (is (vector? (:TableNames (opts->tables *client-opts*))))
    (is (= {:cognitect.anomalies/category :cognitect.anomalies/incorrect
            :cognitect.anomalies/message "invalid URI scheme random"}
           (-> (opts->tables (assoc-in *client-opts* [:endpoint-override :protocol] :random))
               (dissoc :cognitect.aws/throwable))))
    (is (= {:cognitect.anomalies/category :cognitect.anomalies/fault
            :cognitect.anomalies/message "No known endpoint."}
           (-> (opts->tables (assoc *client-opts* :region "random"))
               (dissoc :cognitect.aws/throwable))))))
