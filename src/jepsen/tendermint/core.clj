(ns jepsen.tendermint.core
  (:refer-clojure :exclude [test])
  (:require [clojure.tools.logging :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [knossos.model :as model]
            [slingshot.slingshot :refer [try+]]
            [jepsen [checker :as checker]
             [cli :as cli]
             [client :as client]
             [control :as c]
             [db :as db]
             [generator :as gen]
             [independent :as independent]
             [nemesis :as nemesis]
             [tests :as tests]
             [util :as util :refer [timeout map-vals]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [cheshire.core :as json]
            [jepsen.tendermint.client :as tc]
            ))

(def base-dir "/opt/tendermint")

(defn install-component!
  "Download and install a tendermint component"
  [app opts]
  (let [base    "https://s3-us-west-2.amazonaws.com/tendermint/binaries/"
        ext     "linux_amd64.zip"
        version (get-in opts [:versions (keyword app)])
        path    (str base app "/v" version "/" app "_" version "_" ext)]
    (cu/install-archive! path (str base-dir "/" app))))

(defn gen-validator
  "Generate a new validator structure, and return the validator's data as a
  map."
  []
  (c/cd base-dir
        (-> (c/exec "./tendermint" :--home base-dir :gen_validator)
            (json/parse-string true))))

(defn gen-validator!
  "Generates a validator for the current node, writes it to validator.json, and
  registers the public key in the test map."
  [test node]
  (let [validator (gen-validator)]
    (deliver (get-in test [:pub-keys node]) (:pub_key validator))
    (c/su
      (c/cd base-dir
            (c/exec :echo (json/generate-string validator)
                    :> "priv_validator.json")
            (info "Wrote priv_validator.json")))))

(defn gen-genesis
  "Generate a new genesis structure for a test. Blocks until all pubkeys are
  available."
  [test]
  {:app_hash      ""
   :chain_id      "jepsen"
   :genesis_time  "0001-01-01T00:00:00.000Z"
   :validators    (map (fn [[node pub-key]]
                         {:amount  10
                          :name    node
                          :pub_key @pub-key})
                       (:pub-keys test))})

(defn gen-genesis!
  "Generates a new genesis file and writes it to disk."
  [test]
  (c/su
    (c/cd base-dir
          (c/exec :echo (json/generate-string (gen-genesis test))
                  :> "genesis.json")
          (info "Wrote genesis.json"))))

(defn write-config!
  "Writes out a config.toml file to the current node."
  []
  (c/su
    (c/cd base-dir
          (c/exec :echo (slurp (io/resource "config.toml"))
                  :> "config.toml"))))

(defn seeds
  "Constructs a --seeds command line for a test, so a tendermint node knows
  what other nodes to talk to."
  [test node]
  (->> (:nodes test)
       (remove #{node})
       (map (fn [node] (str node ":46656")))
       (str/join ",")))

(def socket
  "The socket we use to communicate with merkleeyes"
  "unix://merkleeyes.sock")

(def merkleeyes-logfile (str base-dir "/merkleeyes.log"))
(def tendermint-logfile (str base-dir "/tendermint.log"))
(def merkleeyes-pidfile (str base-dir "/merkleeyes.pid"))
(def tendermint-pidfile (str base-dir "/tendermint.pid"))

(defn start-tendermint!
  "Starts tendermint as a daemon."
  [test node]
  (c/su
    (c/cd base-dir
          (cu/start-daemon!
            {:logfile tendermint-logfile
             :pidfile tendermint-pidfile
             :chdir   base-dir}
            "./tendermint"
            :--home base-dir
            :node
            :--proxy_app socket
            :--p2p.seeds (seeds test node)))))

(defn start-merkleeyes!
  "Starts merkleeyes as a daemon."
  []
  (c/su
    (c/cd base-dir
          (cu/start-daemon!
            {:logfile merkleeyes-logfile
             :pidfile merkleeyes-pidfile
             :chdir   base-dir}
            "./merkleeyes"
            :start
            :--dbName   "jepsen"
            :--address  socket))))

(defn stop-tendermint! []
  (c/su (cu/stop-daemon! tendermint-pidfile)))

(defn stop-merkleeyes! []
  (c/su (cu/stop-daemon! merkleeyes-pidfile)))

(defn db
  "A complete Tendermint system. Options:

  :versions         A version map, with keys...
    :tendermint     The version of tendermint to install (e.g. \"0.10.0\")
    :abci           The version ot ABCI
    :merkleeyes     The version of Merkle Eyes"
  [opts]
  (reify db/DB
    (setup! [_ test node]
      (c/su
        ; (install-component! "tendermint"  opts)
        (install-component! "abci"        opts)
        ; (install-component! "merkleeyes"  opts)
        (c/cd base-dir
              (c/exec :wget "https://s3-us-west-2.amazonaws.com/tendermint/jepsen/tendermint")
              (c/exec :chmod "+x" "tendermint")
              (c/exec :wget "https://s3-us-west-2.amazonaws.com/tendermint/jepsen/merkleeyes")
              (c/exec :chmod "+x" "merkleeyes"))

        (gen-validator! test node)
        (gen-genesis!   test)
        (write-config!)

        (start-merkleeyes!)
        (start-tendermint! test node)

        (Thread/sleep 10000)))


    (teardown! [_ test node]
      (stop-merkleeyes!)
      (stop-tendermint!)
      (c/su
        (c/exec :rm :-rf base-dir)))

    db/LogFiles
    (log-files [_ test node]
      [tendermint-logfile
       merkleeyes-logfile])))

(defn client
  ([]
   (client nil))
  ([node]
   (reify client/Client
     (setup! [_ test node]
       (client node))

     (invoke! [_ test op]
       (let [[k v] (:value op)
             crash (if (= (:f op) :read)
                     :fail
                     :info)]
         (try+
           (case (:f op)
             :read  (assoc op
                           :type :ok
                           :value (independent/tuple k (tc/read node k)))
             :write (do (tc/write! node k v)
                        (assoc op :type :ok))
             :cas   (let [[v v'] v]
                      (tc/cas! node k v v')
                      (assoc op :type :ok)))

           (catch [:type :unauthorized] e
             (assoc op :type :fail, :error :precondition-failed))

           (catch [:type :base-unknown-address] e
             (assoc op :type :fail, :error :not-found))

           (catch java.net.SocketTimeoutException e
             (assoc op :type crash, :error :timeout)))))

     (teardown! [_ test]))))

(defn r   [_ _] {:type :invoke, :f :read,  :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 10)})
(defn cas [_ _] {:type :invoke, :f :cas,   :value [(rand-int 10) (rand-int 10)]})

(defn test
  [opts]
  (let [n (count (:nodes opts))]
    (merge tests/noop-test
           opts
           {:name "tendermint"
            :os   debian/os
            :nonserializable-keys [:pub-keys]
            :pub-keys (->> (:nodes opts)
                           (map (fn [node] [node (promise)]))
                           (into {}))
            :db   (db {:versions {:tendermint "0.10.0"
                                  :abci       "0.5.0"
                                  :merkleeyes "0.2.2"}})
            :client (client)
            :generator (->> (independent/concurrent-generator
                              (* 2 n)
                              (range)
                              (fn [k]
                                (->> (gen/mix [w cas])
                                     (gen/reserve n r)
                                     (gen/stagger 1/2)
                                     (gen/limit 100))))
                            (gen/nemesis
                              (gen/start-stop 5 30))
                            (gen/time-limit (:time-limit opts)))
            :nemesis (nemesis/partition-random-node)
            :model   (model/cas-register)
            :checker (checker/compose
                       {:linear   (independent/checker (checker/linearizable))
                        :timeline (independent/checker   (timeline/html))
                        :perf     (checker/perf)})
            })))
