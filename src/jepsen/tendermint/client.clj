(ns jepsen.tendermint.client
  "Client for merkleeyes."
  (:refer-clojure :exclude [read])
  (:require [jepsen.tendermint.gowire :as w]
            [clojure.data.fressian :as f]
            [clj-http.client :as http]
            [clojure.tools.logging :refer [info warn]]
            [jepsen.util :refer [map-vals]]
            [slingshot.slingshot :refer [throw+]])
  (:import (java.nio ByteBuffer)
           (java.util Random)
           (java.lang StringBuilder)))

; General-purpose ABCI operations

(defn byte-buf->hex
  "Convert a byte buffer to a hex string."
  [^ByteBuffer buf]
  (let [sb (StringBuilder.)]
    (loop []
      (if (.hasRemaining buf)
        (do (.append sb (format "%02x" (bit-and (.get buf) 0xff)))
            (recur))
        (str sb)))))

(defn hex->byte-buf
  "Convert a string of hex digits to a byte buffer"
  [s]
  (let [n (/ (count s) 2)
        a (byte-array n)]
    (loop [i 0]
      (when (< i n)
        (aset a i (unchecked-byte (Integer/parseInt
                                    (subs s (* i 2) (+ (* i 2) 2)) 16)))
        (recur (inc i))))
    (ByteBuffer/wrap a)))

(defn encode-query-param
  "Encodes a string or bytebuffer for use as a URL parameter. Converts strings
  to quoted strings, e.g. \"foo\" -> \"\\\"foo\\\"\", and byte buffers to 0x...
  strings, e.g. \"0xabcd\"."
  [x]
  (condp instance? x
    String     (str "\"" x "\"")
    ByteBuffer (str "0x" (byte-buf->hex x))))

(defn validate-tx-code
  "Checks a check_tx or deliver_tx structure for errors, and throws as
  necessary.  Returns the tx otherwise."
  [tx]
  (case (:code tx)
    0   tx
    4   (throw+ {:type :unauthorized :log (:log tx)})
    111 (throw+ {:type :base-unknown-address, :log (:log tx)})
        (throw+ (assoc tx :type :unknown-tx-error))))

(def port "HTTP interface port" 46657)
(def default-http-opts
  "clj-http options"
  {:socket-timeout  10000
   :conn-timeout    10000
   :accept          :json
   :as              :json
   :throw-entire-message? true})

(defn broadcast-tx!
  "Broadcast a given transaction to the given node. tx can be a string, in
  which case it is encoded as \"...\", or a ByteBuffer, in which case it is
  encoded as 0x.... Throws for errors in either check_tx or deliver_tx, and if
  no errors are present, returns a result map."
  [node tx]
  (let [tx (encode-query-param tx)
        http-res (http/get (str "http://" node ":" port "/broadcast_tx_commit")
                           (assoc default-http-opts
                                  :query-params {:tx tx}))
        result (-> http-res :body :result)]
    ; (info :result result)
    (validate-tx-code (:check_tx result))
    (validate-tx-code (:deliver_tx result))
    result))

(defn abci-query
  "Performs an ABCI query on the given node."
  [node path data]
  (http/get (str "http://" node ":" port "/abci_query")
            (assoc default-http-opts
                   :query-params  {:data  (encode-query-param data)
                                   :path  (encode-query-param path)
                                   :prove false})))


; Merkleeyes-specific paths

(defn nonce
  "A 12 byte random nonce byte buffer"
  []
  (let [buf (byte-array 12)]
    (.nextBytes (Random.) buf)
    (ByteBuffer/wrap buf)))

(def tx-types
  "A map of transaction type keywords to their magic bytes."
  (map-vals unchecked-byte
            {:set    0x01
             :remove 0x02
             :get    0x03
             :cas    0x04}))

(defn tx-type
  "Returns the byte for a transaction type keyword"
  [type-kw]
  (or (get tx-types type-kw)
      (throw (IllegalArgumentException. (str "Unknown tx type " type-kw)))))

(defn tx
  "Construct a merkleeyes transaction byte buffer"
  [type & args]
  (let [nonce (nonce)
        b (w/buffer-for (cons nonce args) 1)]
    (.put b nonce)
    (.put b (tx-type type))
    (reduce w/write-byte-buffer! b args)
    (.flip b)))

(defn write!
  "Ask node to set k to v"
  [node k v]
  (broadcast-tx! node (tx :set (f/write k) (f/write v))))

(defn read
  "Perform a transactional read"
  [node k]
  (-> (broadcast-tx! node (tx :get (f/write k)))
      :deliver_tx
      :data
      hex->byte-buf
      f/read))

(defn cas!
  "Perform a compare-and-set from v to v' of k"
  [node k v v']
  (broadcast-tx! node (tx :cas (f/write k) (f/write v) (f/write v'))))

(defn local-read
  "Read by querying a particular node"
  [node k]
  (let [k (f/write k)
        res (-> (abci-query node "/store" k)
                :body
                :result
                :response
                :value)]
    (if (= res "")
      nil
      (f/read (hex->byte-buf res)))))
