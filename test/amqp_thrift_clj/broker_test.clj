(ns amqp-thrift-clj.broker-test
  (:require [clojure.test :refer :all]
            [langohr.basic :as basic]
            [langohr.channel :as channel]
            [langohr.queue :as queue]
            [langohr.core :as core]
            [amqp-thrift-clj.core :refer :all]
            [amqp-thrift-clj.broker :refer :all :as br]))

(def *service-name* "test.rpc")

(deftest server-broker-test-open
  (with-open [connection (core/connect)
              channel (channel/open connection)]
    (let [^Broker broker (br/server-broker *service-name* channel)]
      (is (br/open? broker))
      (is (not (br/closed? broker))))))

(deftest server-broker-close
  (with-open [connection (core/connect)]
    (let [channel (channel/open connection)
          ^Broker broker (br/server-broker *service-name* channel)]
      (is (br/open? broker))
      (br/close broker)
      (is (br/closed? broker)))))

(deftest server-broker-read-message
  (with-open [connection (core/connect)
              channel (channel/open connection)]
    (queue/declare channel *service-name*)
    (basic/publish channel "" *service-name* "Hello!")
    (let [^Broker broker (br/server-broker *service-name* channel)]
      (testing "gets the correct message"
        (is (= "Hello!" (String. (br/read-message broker)))))
      (testing "Does not reset until answered"
        (is (= "Hello!" (String. (br/read-message broker))))
        (is (= "Hello!" (String. (br/read-message broker))))))))

(deftest server-broker-publish-message
  (with-open [connection (core/connect)
              channel (channel/open connection)]
    (let [^Broker broker (br/server-broker *service-name* channel)]
      (testing "Does not reset until answered"
        (is (= "Hello!" (String. (br/read-message broker))))
        (is (= "Hello!" (String. (br/read-message broker))))))))

(comment
  (run-tests))
