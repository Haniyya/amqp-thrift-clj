(ns amqp-thrift-clj.message-broker-transport
  (:require [amqp-thrift-clj.broker :as cb]
            [clojure.java.io :as io])
  (:import [org.apache.thrift.transport TTransport]
           [org.apache.thrift TByteArrayOutputStream]
           [java.io InputStream ByteArrayOutputStream ByteArrayInputStream]
           [amqp_thrift_clj.broker Broker])
  (:gen-class
    :init init
    :state state
    :constructor [Broker]
    :name com.amqp_thrift_clj.TMessageBrokerTransport
    :extends org.apache.thrift.transport.TTransport))

(defrecord MessageBrokerTransportState [broker outbuf inbuf])

(defn open? [{:keys [broker outbuf]}]
  (and (.isOpen outbuf) (cb/open? broker)))

(defn close! [{:keys [broker outbuf] :as mbt}]
  (.close outbuf)
  (cb/close broker)
  mbt)

(defn get-inbuf [{:keys [broker] :as mbt}]
  (let [inbuf (io/reader (char-array (cb/read-message broker)))]
    (assoc mbt :inbuf inbuf)))

(defn read-into [{:keys [inbuf] :as mbt} buf off len]
  (if (or (nil? inbuf) (not (.ready inbuf)))
    (read-into (get-inbuf mbt) buf off len)
    (assoc mbt :bytes-read (.read inbuf buf off len))))

(defn write-to [{:keys [outbuf] :as mbt} buf off len]
  (assoc mbt :bytes-written (.write outbuf buf off len)))

(defn flush-buf [{:keys [outbuf broker] :as mbt}]
  (cb/publish-message broker (.toString outbuf))
  (.close outbuf)
  (assoc mbt :outbuf (TByteArrayOutputStream.)))

(defn make-message-broker-transport [broker]
  (->MessageBrokerTransportState broker (TByteArrayOutputStream.) nil))

;; TMessageBrokerTransport implementation

(defn -init [broker]
  [[] [(atom (make-message-broker-transport broker))]])

(defn -isOpen [this]
  (open? @(.state this)))

(defn -open [this] true)

(defn -close [this]
  (swap! (.state this) close!))

(defn -read [this buf off len]
  (swap! (.state this) read-into buf off len)
  (:bytes-read @(.state this) 0))

(defn -write [this buf off len]
  (swap! (.state this) write-to buf off len)
  (:bytes-written @(.state this) 0))

(defn -flush [this]
  (swap! (.state this) flush-buf))

(comment
  (compile 'rohini-clj.message-broker-transport))
