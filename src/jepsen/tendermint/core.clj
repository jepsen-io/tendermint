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
             [util :as util :refer [timeout with-retry map-vals]]]
            [jepsen.checker.timeline :as timeline]
            [jepsen.nemesis.time :as nt]
            [jepsen.control.util :as cu]
            [jepsen.os.debian :as debian]
            [cheshire.core :as json]
            [jepsen.tendermint.client :as tc]
            ))

(def base-dir "/opt/tendermint")

(defn install-component!
  "Download and install a tendermint component"
  [app opts]
  (let [opt-name (keyword (str app "-url"))
        path (get opts opt-name)]
    (cu/install-archive! path (str base-dir "/" app))))

(defn gen-validator
  "Generate a new validator structure, and return the validator's data as a
  map."
  []
  (c/cd base-dir
        (-> (c/exec "./tendermint" :--home base-dir :gen_validator)
            (json/parse-string true))))

(defn gen-validator!
  "Generates a validator for the current node (or, if this validator is a dup,
  waits for the appropriate validator to be ready and clones its keys), then
  writes out a validator.json, and registers the keys in the test map."
  [test node]
  (let [validator (if-let [orig (get (:dup-validators test) node)]
                    ; Copy from orig's keys
                    (do (info node "copying" orig "validator key")
                        @(get-in test [:validators orig]))
                    ; Generate a fresh one
                    (gen-validator))]
    (deliver (get-in test [:validators node]) validator)
    (c/su
      (c/cd base-dir
            (c/exec :echo (json/generate-string validator)
                    :> "priv_validator.json")
            (info "Wrote priv_validator.json")))))

(defn gen-genesis
  "Generate a new genesis structure for a test. Blocks until all pubkeys are
  available."
  [test]
  (let [weights (:validator-weights test)]
    {:app_hash      ""
     :chain_id      "jepsen"
     :genesis_time  "0001-01-01T00:00:00.000Z"
     :validators    (->> (:validators test)
                         (reduce (fn [validators [node validator]]
                                   (let [pub-key (:pub_key @validator)]
                                     (if (some #(= pub-key (:pub_key %))
                                               validators)
                                       validators
                                       (conj validators
                                             {:amount  (get weights node)
                                              :name    node
                                              :pub_key pub-key}))))
                                 []))}))

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
        (install-component! "tendermint"  opts)
        (install-component! "abci"        opts)
        (install-component! "merkleeyes"  opts)

        (gen-validator! test node)
        (gen-genesis!   test)
        (write-config!)

        (start-merkleeyes!)
        (start-tendermint! test node)

        (nt/install!)

        (Thread/sleep 1000)))

    (teardown! [_ test node]
      (stop-merkleeyes!)
      (stop-tendermint!)
      (c/su
        (c/exec :rm :-rf base-dir)))

    db/LogFiles
    (log-files [_ test node]
      [tendermint-logfile
       merkleeyes-logfile
       (str base-dir "/priv_validator.json")
       (str base-dir "/genesis.json")
       ])))

(defn r   [_ _] {:type :invoke, :f :read,  :value nil})
(defn w   [_ _] {:type :invoke, :f :write, :value (rand-int 10)})
(defn cas [_ _] {:type :invoke, :f :cas,   :value [(rand-int 10) (rand-int 10)]})

(defn cas-register-client
  ([]
   (cas-register-client nil))
  ([node]
   (reify client/Client
     (setup! [_ test node]
       (cas-register-client node))

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

           (catch java.net.ConnectException e
             (condp re-find (.getMessage e)
               #"Connection refused"
               (assoc op :type :fail, :error :connection-refused)

               (assoc op :type crash, :error [:connect-exception
                                              (.getMessage e)])))

           (catch java.net.SocketTimeoutException e
             (assoc op :type crash, :error :timeout)))))

     (teardown! [_ test]))))


(defn set-client
  ([]
   (set-client nil))
  ([node]
   (reify client/Client
     (setup! [_ test node]
       (set-client node))

     (invoke! [_ test op]
       (let [[k v] (:value op)
             crash (if (= (:f op) :read)
                     :fail
                     :info)]
         (try+
           (case (:f op)
             :init (with-retry [tries 0]
                     (tc/write! node k [])
                     (assoc op :type :ok)
                     (catch Exception e
                       (if (<= 10 tries)
                         (throw e)
                         (do (info "Couldn't initialize key" k ":" (.getMessage e) "- retrying")
                             (Thread/sleep (* 50 (Math/pow 2 tries)))
                             (retry (inc tries))))))
             :add (let [s (or (vec (tc/read node k)) [])
                        s' (conj s v)]
                    (tc/cas! node k s s')
                    (assoc op :type :ok))
             :read (assoc op
                          :type :ok
                          :value (independent/tuple
                                   k
                                   (into (sorted-set) (tc/read node k)))))

           (catch [:type :unauthorized] e
             (assoc op :type :fail, :error :precondition-failed))

           (catch [:type :base-unknown-address] e
             (assoc op :type :fail, :error :not-found))

           (catch java.net.ConnectException e
             (condp re-find (.getMessage e)
               #"Connection refused"
               (assoc op :type :fail, :error :connection-refused)

               (assoc op :type crash, :error [:connect-exception
                                              (.getMessage e)])))

           (catch java.net.SocketTimeoutException e
             (assoc op :type crash, :error :timeout)))))

     (teardown! [_ test]))))

(defn dup-groups
  "Takes a test with a :dup-validators map of nodes to the nodes they imitate,
  and turns that into a collection of collections of nodes, each of which is
  several nodes pretending to be the same node. Returns a map of :groups, which
  are the aforementioned groups, :singles, those groups with only 1 node, and
  :dups, with more than one."
  [test]
  (let [dv (:dup-validators test)
        groups (->> (:nodes test)
                    (reduce (fn [index node]
                              (let [orig (get dv node node)
                                    coll (get index orig #{})]
                                (assoc index orig (conj coll node))))
                            {})
                    vals)]
    {:groups  groups
     :singles (filter #(= 1 (count %)) groups)
     :dups    (filter #(< 1 (count %)) groups)}))

(defn peekaboo-dup-validators-grudge
  "Takes a test. Returns a function which takes a collection of nodes from that
  test, and constructs a network partition (a grudge) which isolates some dups
  completely, and leaves one connected to the majority component."
  [test]
  (let [{:keys [groups singles dups]} (dup-groups test)]
    (fn [nodes]
      ; Pick one random node from every group of dups to participate in the
      ; main component, and compute the remaining complement for each dup
      ; group.
      (let [chosen-ones (map (comp hash-set rand-nth vec) dups)
            exiles      (map remove chosen-ones dups)]
        (nemesis/complete-grudge
          (cons ; Main group
                (set (concat (apply concat singles)
                             (apply concat chosen-ones)))
                ; Exiles
                exiles))))))

(defn split-dup-validators-grudge
  "Takes a test. Returns a function which takes a collection of nodes from that
  test, and constructs a network partition (a grudge) which splits the network
  into n disjoint components, each having a single duplicate validator and an
  equal share of the remaining nodes."
  [test]
  (let [{:keys [groups singles dups]} (dup-groups test)]
    (fn [nodes]
      (let [n (reduce max (map count dups))]
        (->> groups
             shuffle
             (map shuffle)
             (apply concat)
             (reduce (fn [[components i] node]
                       [(update components (mod i n) conj node)
                        (inc i)])
                     [[] 0])
             first
             nemesis/complete-grudge)))))

(defn nemesis
  "The generator and nemesis for each nemesis profile"
  [test]
  (case (:nemesis test)
    :peekaboo-dup-validators {:nemesis (nemesis/partitioner
                                         (peekaboo-dup-validators-grudge test))
                              :generator (gen/start-stop 0 5)}
    :split-dup-validators {:nemesis (nemesis/partitioner
                                      (split-dup-validators-grudge test))
                           :generator (gen/once {:type :info, :f :start})}
    :half-partitions {:nemesis   (nemesis/partition-random-halves)
                      :generator (gen/start-stop 5 30)}
    :ring-partitions {:nemesis (nemesis/partition-majorities-ring)
                      :generator (gen/start-stop 5 30)}
    :single-partitions {:nemesis (nemesis/partition-random-node)
                        :generator (gen/start-stop 5 30)}
    :clocks     {:nemesis   (nt/clock-nemesis)
                 :generator (gen/stagger 5 (nt/clock-gen))}
    :none       {:nemesis   client/noop
                 :generator gen/void}))

(defn dup-validators
  "Takes a test. Constructs a map of nodes to the nodes whose validator keys
  they use instead of their own. If a node has no entry in the map, it
  generates its own validator key."
  [test]
  (if (:dup-validators test)
    (let [[orig & clones] (take 2 (:nodes test))]
      (zipmap clones (repeat orig)))))
    ; We need fewer than 1/3.
    ; (let [[orig & clones] (take (Math/floor (/ (count (:nodes test)) 3.01))
    ;                             (:nodes test))]
    ;   (zipmap clones (repeat orig)))))

(defn validator-weights
  "Takes a test. Computes a map of node names to voting amounts. When
  dup-validators are involved, allocates just shy of 2/3 votes to the
  duplicated key, assuming there's exactly one dup key."
  [test]
  (let [dup-vals (:dup-validators test)]
    (if-not (seq dup-vals)
      ; Equal weights
      (zipmap (:nodes test) (repeat 1))

      (let [{:keys [groups singles dups]} (dup-groups test)
            n                             (count groups)]
        (assert (= 1 (count dups))
                "Don't know how to handle more than one dup validator key")
        ; For super dup validators, we want the dup validator key to have just
        ; shy of 2/3 voting power. That means the sum of the normal nodes
        ; weights should be just over 1/3, so that the remaining node can make
        ; up just under 2/3rds of the votes by itself. Let a normal node's
        ; weight be 2. Then 2(n-1) is the combined voting power of the normal
        ; bloc. We can then choose 4(n-1) - 1 as the weight for the dup
        ; validator. The total votes are
        ;
        ;    2(n-1) + 4(n-1) - 1
        ;  = 6(n-1) - 1
        ;
        ; which implies a single dup node has fraction...
        ;
        ;    (4(n-1) - 1) / (6(n-1) - 1)
        ;
        ; which approaches 2/3 from 0 for n = 1 -> infinity, and if a single
        ; regular node is added to a duplicate node, a 2/3+ majority is
        ; available for all n >= 1.
        ;
        ; For regular dup validators, let an individual node have weight 2. The
        ; total number of individual votes is 2(n-1), which should be just
        ; larger than twice the number of dup votes, e.g:
        ;
        ;     2(n-1) = 2d + e
        ;
        ; where e is some small positive integer, and d is the number of dup
        ; votes. Solving for d:
        ;
        ;     (2(n-1) - e) / 2 = d
        ;          n - 1 - e/2 = d    ; Choose e = 2
        ;                n - 2 = d
        ;
        ; The total number of votes is therefore:
        ;
        ;     2(n-1) + n - 2
        ;   = 3n - 4
        ;
        ; So a dup validator alone has vote fraction:
        ;
        ;     (n - 2) / (3n - 4)
        ;
        ; which is always under 1/3. And with a single validator, it has vote
        ; fraction:
        ;
        ;     (n - 2) + 2 / (3n - 4)
        ;   =           n / (3n - 4)
        ;
        ; which is always over 1/3.
        (merge (zipmap (apply concat singles) (repeat 2))
               (zipmap (first dups) (repeat
                                      (if (:super-dup-validators test)
                                        (dec (* 4 (dec n)))
                                        (- n 2)))))))))
(defn deref-gen
  "Sometimes you need to build a generator not *now*, but *later*; e.g. because
  it depends on state that won't be available until the generator is actually
  invoked. Wrap a derefable returning a generator in this, and it'll be deref'ed
  only when asked for ops."
  [dgen]
  (reify gen/Generator
    (op [this test process]
      (gen/op @dgen test process))))

(defn workload
  "Given a test map, computes

      {:generator a generator of client ops
       :client    a client to execute those ops
       :model     a model to validate the history
       :checker   a map of checker names to checkers to run}."
  [test]
  (let [n (count (:nodes test))]
    (case (:workload test)
      :cas-register {:client    (cas-register-client)
                     :model     (model/cas-register)
                     :generator (independent/concurrent-generator
                                  (* 2 n)
                                  (range)
                                  (fn [k]
                                    (->> (gen/mix (w cas))
                                         (gen/reserve n r)
                                         (gen/stagger 1)
                                         (gen/limit 120))))
                     :final-generator nil
                     :checker {:linear (independent/checker
                                         (checker/linearizable))}}
      :set
      (let [keys (atom [])]
        {:client (set-client)
         :model  nil
         :generator (independent/concurrent-generator
                      n
                      (range)
                      (fn [k]
                        (swap! keys conj k)
                        (gen/phases
                          (gen/once {:type :invoke, :f :init})
                          (->> (range)
                               (map (fn [x]
                                      {:type :invoke
                                       :f    :add
                                       :value x}))
                               gen/seq
                               (gen/stagger 1/2)))))
         :final-generator (deref-gen
                            (delay
                              (locking keys
                                (independent/concurrent-generator
                                  n
                                  @keys
                                  (fn [k]
                                    (gen/each (gen/once {:type :invoke
                                                         :f :read})))))))
         :checker {:set (independent/checker (checker/set))}}))))


(defn test
  [opts]
  (let [test (merge
               tests/noop-test
               opts
               {:name (str "tendermint " (:workload test) " "
                           (name (:nemesis opts)))
                :os   debian/os
                :nonserializable-keys [:validators]
                ; A map of validator nodes to the nodes whose keys they use
                ; instead of their own.
                :dup-validators (dup-validators opts)
                ; Map of node names to validator data structures, including keys
                :validators (->> (:nodes opts)
                                 (map (fn [node] [node (promise)]))
                                 (into {}))
                :db       (db opts)})
        nemesis (nemesis test)
        workload (workload test)
        checker (checker/compose
                  (merge {:timeline (independent/checker (timeline/html))
                          :perf     (checker/perf)}
                         (:checker workload)))
        test    (merge test
                       {:validator-weights (validator-weights test)
                        :client     (:client workload)
                        :generator  (gen/phases
                                      (->> (:generator workload)
                                           (gen/nemesis (:generator nemesis))
                                           (gen/time-limit (:time-limit opts)))
                                      (gen/nemesis
                                        (gen/once {:type :info, :f :stop}))
                                      (gen/sleep 30)
                                      (gen/clients
                                        (:final-generator workload)))
                        :nemesis    (:nemesis nemesis)
                        :model      (:model workload)
                        :checker    checker})]
    test))
