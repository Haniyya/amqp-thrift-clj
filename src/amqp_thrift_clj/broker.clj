(ns amqp-thrift-clj.broker
  (:import [java.util UUID]
           [java.io Closeable])
  (:require [langohr.core :as lc]
            [langohr.channel :as lch]
            [langohr.consumers :as consumers]
            [langohr.basic :as lb]
            [clojure.core.async :as a]))

(defprotocol Broker
  (open? [this])
  (close [this])
  (closed? [this])
  (error? [this])
  (read-message [this])
  (publish-message [this message]))

(defn- same-uuid [uuid]
  (filter #(= uuid (UUID/fromString (:correlation-id %)))))

(defn- queue-name [service-name] service-name)

(defn- receive-response [lchan response-chan uuid]
  (fn [_ch {:keys [delivery-tag correlation-id]} ^bytes payload]
    (println (String. payload "UTF-8"))
    (when (= uuid (UUID/fromString correlation-id))
      (lb/ack lchan delivery-tag)
      (a/put! response-chan (String. payload "UTF-8")))))

(def *exchange* "")
(def *default-reply-to* "amq.rabbitmq.reply-to")

(def payload last)
(def meta-info second)

(defn- respond-to [request channel message]
  (let [{:keys [correlation-id reply-to]} (meta-info request)]
    (lb/publish channel *exchange* reply-to message
      {:correlation-id correlation-id})
    nil))

(defrecord ServerBroker [service-name lchan processing]
  Broker
  (open? [_] (lch/open? lchan))
  (close [this]
    (when (lch/open? lchan)
      (lch/close lchan))
    this)
  (closed? [_] (lch/closed? lchan))
  (error? [_] false)
  (read-message [_]
    (when (nil? @processing)
      (let [queue (queue-name service-name)
            message (lb/get lchan queue true)]
        (reset! processing message)))
    (last @processing))
  (publish-message [_ message]
    (when (nil? @processing)
      (throw (Exception. "No message to answer!")))
    (swap! processing respond-to lchan message)))

(defn server-broker [service-name lchan]
  (->ServerBroker service-name lchan (atom nil)))

(defrecord ClientBroker [service-name lchan responses]
  Closeable
  Broker
  (open? [_] (lc/open? lchan))
  (close [_] (lc/close lchan) (a/close! responses))
  (closed? [_] (lc/closed? lchan))
  (error? [_] false)
  (read-message [this] (a/<!! responses))
  (publish-message [this message]
    (let [new-cor-id (UUID/randomUUID)
          queue (queue-name service-name)]
      (lb/publish lchan *exchange* queue message
                  {:correlation-id (str new-cor-id) :reply-to *default-reply-to*})
      (consumers/subscribe lchan *default-reply-to*
                           (receive-response lchan responses new-cor-id)
                           {:auto-ack false}))))
  

(defn client-broker [service-name lchan]
  (->ClientBroker service-name lchan (a/chan 16)))
