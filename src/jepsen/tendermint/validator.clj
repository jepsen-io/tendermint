(ns jepsen.tendermint.validator
  {:lang :core.typed
   :doc "Supports validator set configuration and changes."}
  (:require [clojure.set :as set]
            [clojure.tools.logging :refer [info warn]]
            [clojure.pprint :refer [pprint]]
            [clojure.core.typed :as t]
            [cheshire.core :as json]
            [dom-top.core :as dt]
            [jepsen.tendermint [client :as tc]
                               [util :refer [base-dir]]]
            [jepsen [util :as util :refer [map-vals]]
                    [control :as c]
                    [client :as client]
                    [nemesis :as nemesis]
                    [generator :as gen]])
  (:import (clojure.tools.logging.impl LoggerFactory Logger)
           (clojure.lang Namespace
                         Symbol)))

; Type support
(defmacro tk
  "Typechecked keyword function. Returns the given keyword, but tells
  core.typed it's a function of [m -> v]."
  [kw m v]
  `(t/ann-form ~kw [~m ~'-> ~v]))

(defmacro tmfn
  "Typed map fn. Core.typed doesn't know (Map a b) is also the fn [a -> (Option
  b)], so we have to tell it."
  [m K V]
  `(t/fn [k# :- ~K] :- (t/Option ~V)
     (get ~m k#)))

; External types

(t/ann jepsen.control/*dir* String)
(t/ann jepsen.tendermint.util/base-dir String)

(t/ann ^:no-check clojure.core/update
       (t/All [m k v v' arg ...]
              (t/IFn
                [m k [v arg ... arg -> v'] arg ... arg -> (t/Assoc m k v')])))

(t/ann ^:no-check clojure.tools.logging/*logger-factory* LoggerFactory)

(t/ann ^:no-check clojure.tools.logging.impl/get-logger
       [LoggerFactory (t/U clojure.lang.Symbol Namespace)
              -> clojure.tools.logging.impl.Logger])

(t/ann ^:no-check clojure.tools.logging.impl/enabled?
       [Logger t/Keyword -> Boolean])

(t/ann ^:no-check clojure.tools.logging/log*
       [Logger t/Keyword (t/U Throwable nil) String -> nil])

(t/ann ^:no-check jepsen.util/map-vals
       (t/All [k v1 v2]
              [[v1 -> v2] (t/Map k v1) -> (t/Map k v2)]))

(t/ann ^:no-check jepsen.control/on-nodes
       (t/All [res]
              (t/IFn [Test [Test Node -> res]
                      -> (t/Map Node res)]
                     [Test (t/NonEmptyColl Node) [Test Node -> res]
                      -> (t/I (t/Map Node res)
                              (t/NonEmptySeqable
                                (clojure.lang.AMapEntry Node res)))])))

; Domain types

(t/defalias Node
  "Jepsen nodes are strings."
  String)

(t/defalias Test
  "Jepsen tests have nodes and a current validator atom."
  (HMap :mandatory {:nodes            (t/NonEmptyVec  Node)
                    :validator-config (t/Atom1        Config)}
        :optional {:max-byzantine-vote-fraction Number}))

(t/defalias Version
  "Tendermint cluster version numbers."
  Long)

(t/defalias ShortKey
  "In some places, Tendermint represents keys only by their raw data."
  String)

(t/defalias Key
  "A key is a map with :type and :data. Tendermint uses this to represent
  public and private keys in validators"
  (HMap :mandatory {:type String
                    :data ShortKey}
        :complete? true))

(t/defalias Validator
  "A Validator's complete structure, including both votes and information
  necessary to construct priv_validator.json & genesis.json."
  (HMap :mandatory {:address  String
                    :pub_key  Key
                    :priv_key key
                    :votes    Long}))

(t/defalias Config
  "A configuration represents a definite state of the cluster: the validators
  which are a part of the cluster, what nodes are running what validators, the
  version number of the config in tendermint, the nodes that are in the test,
  etc."
  (HMap :mandatory {:version    Version
                    :node-set   (t/Set Node)
                    :nodes      (t/Map Node Key)
                    :validators (t/Map Key Validator)
                    :max-byzantine-vote-fraction Number}))

(t/defalias CreateTransition
  "Create an instance of a validator on a node"
  (t/HMap :mandatory {:type       (t/Val :create)
                      :node       Node
                      :validator  Validator}
          :complete? true))

(t/defalias DestroyTransition
  "Destroy an instance of a validator"
  (t/HMap :mandatory {:type (t/Val :destroy)
                      :node Node}
          :complete? true))

(t/defalias AddTransition
  "Add a new validator to the config"
  (t/HMap :mandatory {:type       (t/Val :add)
                      :version    Version
                      :validator  Validator}
          :complete? true))

(t/defalias RemoveTransition
  "Remove a validator from the config"
  (t/HMap :mandatory {:type     (t/Val :remove)
                      :version  Version
                      :pub_key  Key}
          :complete? true))

(t/defalias AlterVotesTransition
  "Change the votes allocated to a validator"
  (t/HMap :mandatory {:type     (t/Val :alter-votes)
                      :version  Version
                      :pub_key  Key
                      :votes    Long}
          :complete? true))

(t/defalias Transition (t/U CreateTransition
                            DestroyTransition
                            AddTransition
                            RemoveTransition
                            AlterVotesTransition))

; OK, let's begin

(t/ann ^:no-check gen-validator [-> Validator])
(defn gen-validator
  "Generate a new validator structure, and return the validator's data as a
  map."
  []
  (c/cd base-dir
        (-> (c/exec "./tendermint" :--home base-dir :gen_validator)
            (json/parse-string true))))

(t/ann compact-key [Key -> ShortKey])
(defn compact-key
  "A compact, lossy, human-friendly representation of a validator key."
  [k]
  (subs (:data k) 0 5))

(t/ann compact-config [Config -> (HMap)])
(defn compact-config
  "Just the essentials, please. Compacts a config into a human-readable,
  limited representation for debugging."
  [c]
  (info (with-out-str (pprint c)))
  {:version (:version c)
   :validators (->> (:validators c)
                    (map (t/fn [pair :- (clojure.lang.AMapEntry Key Validator)]
                           (let [k (key pair)
                                 v (val pair)]
                             [(compact-key k)
                              {:votes (:votes v)}])))
                    (into (sorted-map)))
   :nodes (map-vals compact-key (:nodes c))
   :max-byzantine-vote-fraction (:max-byzantine-vote-fraction c)})


(t/ann config [(HMap :optional {:version    Version
                                :node-set   (t/Set Node)
                                :nodes      (t/Map Node Key)
                                :validators (t/Map Key Validator)
                                :max-byzantine-vote-fraction Number})
                 -> Config])
(defn config
  "There are two pieces of state we need to handle. The first is the validator
  set, as known to the cluster, which maps public keys to maps like:

      {:address
       :pub_key {:type ...
                 :data ...}
       :priv_key {:type ...
                  :data ...}
       :votes    an-int}

  And the second is a map of nodes to the validator key they're running:

      {\"n1\" \"ABCD...\"
       ...}

  Additionally, we need a bound :max-byzantine-vote-fraction on the fraction of
  the vote any byzantine validator is allowed to control, a :version, denoting
  the version of the validator set that the cluster knows, and a :node-set, the
  set of nodes that exist."
  [opts]
  (merge {:validators {}
          :nodes      {}
          :node-set   #{}
          :version    -1
          :max-byzantine-vote-fraction 1/3}
         opts))

(t/ann initial-config [Test -> Config])
(defn initial-config
  "Constructs an initial configuration for a test with a list of :nodes
  provided."
  [test]
  (let [validators (c/with-test-nodes test
                     (gen-validator))]
    (t/ann-form validators (t/Map Node Validator))
    (config
      {:validators (reduce (t/fn [m         :- (t/Map Key Validator)
                                  [node v]  :- '[Node Validator]]
                             (assoc m (:pub_key v) v))
                           {}
                           validators)
       :nodes      (map-vals (tk :pub_key Validator Key) validators)
       :node-set   (set (:nodes test))
       :max-byzantine-vote-fraction (:max-byzantine-vote-fraction test 1/3)})))

(t/ann genesis [Config -> t/Any])
(defn genesis
  "Computes a genesis.json structure for the given config."
  [config]
  {:app_hash      ""
   :chain_id      "jepsen"
   :genesis_time  "0001-01-01T00:00:00.000Z"
   :validators    (->> (:validators config)
                       vals
                       (map (t/fn [validator :- Validator]
                              (let [pub-key (:pub_key validator)
                                    name (->> (:nodes config)
                                              (filter
                                                (t/fn [[_ v] :- '[t/Any Key]]
                                                   (= v pub-key)))
                                              first)
                                    _ (assert name)
                                    name (key name)]
                                {:amount  2
                                 :name    name
                                 :pub_key pub-key}))))})

(t/ann pub-key-on-node [Config Node -> (t/Option Key)])
(defn pub-key-on-node
  "What pubkey is running on a given node?"
  [config node]
  (-> config :nodes (get node)))

(t/ann total-votes [Config -> Number])
(defn total-votes
  "How many votes are in the validator set total?"
  [config]
  (->> (:validators config)
       vals
       (map (tk :votes Validator Number))
       (reduce + 0)))

(t/ann vote-fractions [Config -> (t/Map Key Number)])
(defn vote-fractions
  "A map of validator public keys to the fraction of the vote they control."
  [config]
  (let [total (total-votes config)]
    (->> (:validators config)
         (map-vals (t/fn [v :- Validator]
                     (/ (:votes v) total))))))

(t/ann running-validators [Config -> (t/Option (t/Coll Validator))])
(defn running-validators
  "A collection of validators running on at least one node."
  [config]
  (->> (set (vals (:nodes config)))
       (keep (tmfn (:validators config) Key Validator))))

(t/ann ghost-validators [Config -> (t/Coll Validator)])
(defn ghost-validators
  "A collection of validators not running on any node."
  [config]
  (set/difference (set (vals (:validators config)))
                  (set (running-validators config))))

(t/ann nodes-running-validators [Config -> (t/Map Key (t/Coll Node))])
(defn nodes-running-validators
  "Takes a config, yielding a map of validator keys to groups of nodes that run
  that validator."
  [config]
  (->> (:nodes config)
       (reduce (t/fn [m               :- (t/Map Key (t/Vec Node))
                      [node pub-key]  :- '[Node Key]]
                   (assoc m pub-key (conj (get m pub-key []) node)))
               {})))

(t/ann ^:no-check byzantine-validators [Config -> (t/Coll Validator)])
(defn byzantine-validators
  "A collection of all validators in the validator set which are running on
  more than one node."
  [config]
  (->> (nodes-running-validators config)
       (filter (t/fn [[key nodes] :- '[Key (t/Coll Node)]]
                 (< 1 (count nodes))))
       (map key)
       (keep (tmfn (:validators config) Key Validator))))


(t/ann byzantine-validator-keys [Config -> (t/Coll Key)])
(defn byzantine-validator-keys
  "A collection of all validator keys in the validator set which are running on
  more than one node."
  [config]
  (map (tk :pub_key Validator Key) (byzantine-validators config)))

(t/ann dup-groups [Config -> (HMap :mandatory {:groups (t/Coll (t/Coll Node))
                                               :singles (t/Coll (t/Coll Node))
                                               :dups    (t/Coll (t/Coll Node))}
                                   :complete? true)])
(defn dup-groups
  "Takes a config. Computes a map of:

      {:groups  A collection of groups of nodes, each running the same validator
       :singles Groups with only one nodes
       :dups    Groups with multiple nodes}"
  [config]
  (let [groups (-> config nodes-running-validators vals)]
    {:groups  groups
     :singles (filter (t/fn [g :- (t/Coll t/Any)] (= 1 (count g))) groups)
     :dups    (filter (t/fn [g :- (t/Coll t/Any)] (< 1 (count g))) groups)}))



(t/ann at-least-one-running-validator? [Config -> Boolean])
(defn at-least-one-running-validator?
  "Does the given config have at least one validator which is running on some
  node?"
  [config]
  (boolean (seq (running-validators config))))

(t/ann omnipotent-byzantines? [Config -> Boolean])
(defn omnipotent-byzantines?
  "Does this config contain any byzantine validator which controls more than
  max-byzantine-vote-fraction of the vote?"
  [config]
  (let [vfs       (vote-fractions config)
        threshold (:max-byzantine-vote-fraction config)]
    (boolean (some (t/fn [k :- Key]
                     (let [vf (get vfs k)]
                       (assert vf (str "No vote fraction for " k))
                       (<= threshold vf)))
                   (byzantine-validator-keys config)))))

(t/ann ghost-limit Long)
(def ghost-limit
  "Ghosts are souls without bodies. How many validators can exist without
  actually running on any node?"
  2)

(t/ann too-many-ghosts? [Config -> Boolean])
(defn too-many-ghosts?
  "Does this config have too many validators which aren't running on any
  nodes?"
  [config]
  (< ghost-limit
     (count
       (set/difference (set (keys (:validators config)))
                       (set (vals (:nodes config)))))))

(t/ann zombie-limit Long)
(def zombie-limit
  "Zombies are bodies without souls. How many nodes can run a validator that's
  not actually a part of the cluster?"
  2)

(t/ann too-many-zombies? [Config -> Boolean])
(defn too-many-zombies?
  "Does this config have too many nodes which are running validators that
  aren't a part of the cluster?"
  [config]
  (< zombie-limit
     (count
       (remove (set (keys (:validators config)))
               (vals (:nodes config))))))

(t/ann quorum Number)
(def quorum
  "What fraction of the configuration's voting power should be
  online and non-byzantine in order to perform operations?"
  2/3)

(t/ann quorum? [Config -> Boolean])
(defn quorum?
  "Does the given configuration provide a quorum of running votes?"
  [config]
  (< quorum (/ (reduce + 0 (map (tk :votes Validator Number)
                                (running-validators config)))
               (total-votes config))))

(t/ann fault-limit Number)
(def fault-limit
  "What fraction of votes can be either byzantine or ghosts?"
  1/3)

(t/ann faulty? [Config -> Boolean])
(defn faulty?
  "Are too many nodes byzantine or down?"
  [config]
  (<= fault-limit
      (/ (reduce + 0 (map (tk :votes Validator Number)
                          (set/union (set (byzantine-validators config))
                                     (set (ghost-validators config)))))
         (total-votes config))))


(t/ann assert-valid [Config -> Config])
(defn assert-valid
  "Ensures that the given config is valid, and returns it. Throws
  AssertError if not."
  [config]
  (assert (at-least-one-running-validator? config))
  (assert (not (omnipotent-byzantines? config)))
  (assert (not (too-many-ghosts? config)))
  (assert (not (too-many-zombies? config)))
  (assert (quorum? config))
  (assert (not (faulty? config)))
  (assert (every? (:node-set config) (keys (:nodes config))))
  (assert (every? pos? (map (tk :votes Validator Number)
                            (vals (:validators config)))))
  config)

; Possible state transitions:
; - Create an instance of a validator on a node
; - Destroy a validator instance on some node

; - Add a validator to the validator set
; - Remove a validator from the config set

; - Adjust the weight of a validator


(t/ann step [Config Transition -> Config])
(defn step
  "Apply a low-level state transition to a config, returning a new config.
  Throws if the requested transition is illegal."
  [config transition]
  (assert-valid
    (case (:type transition)
      ; Create a new validator on a node
      :create (let [n (:node transition)
                    v (:validator transition)]
                (assert (not (get-in config [:nodes n])))
                (assoc config :nodes (assoc (:nodes config) n (:pub_key v))))

      ; Destroy a validator on a node
      :destroy (assoc config :nodes (dissoc (:nodes config)
                                            (:node transition)))


      ; Add a validator to the validator set
      :add (let [v (:validator transition)]
             (assert (not (get-in config [:validators (:pub_key v)])))
             (assoc config :validators
                    (assoc (:validators config) (:pub_key v) v)))

      ; Remove a validator from the validator set
      :remove (assoc config :validators
                     (dissoc (:validators config (:pub_key transition))))

      ; Change the votes allocated to a validator
      :alter-votes (let [k (:pub_key transition)
                         v (:votes transition)
                         validators (:validators config)
                         validator  (get validators k)
                         _ (assert validator)
                         validator' (assoc validator :votes v)
                         validators' (assoc validators k validator')]
                     (assoc config :validators validators')))))

(t/ann rand-validator [Config -> Validator])
(defn rand-validator
  "Selects a random validator from the config."
  [config]
  (rand-nth (vals (:validators config))))

(t/ann rand-free-node [Config -> (t/Option Node)])
(defn rand-free-node
  "Selects a random node which isn't running anything."
  [config]
  (when-let [candidates (seq (set/difference (:node-set config)
                                             (set (keys (:nodes config)))))]
    (rand-nth candidates)))

(t/ann rand-taken-node [Config -> (t/Option Node)])
(defn rand-taken-node
  "Selects a random node that's running a validator."
  [config]
  (rand-nth (keys (:nodes config))))

(t/ann rand-transition [Test Config -> Transition])
(defn rand-transition
  "Generates a random transition on the given config."
  [test config]
  (or (condp <= (rand)
        ; Create a new instance of a validator on a node.
        4/5 (let [v (rand-validator config)
                  n (rand-free-node config)]
              (when (and v n)
                {:type      :create
                 :node      n
                 :validator v}))

        ;; Nuke a node
        3/5 (when-let [node (rand-taken-node config)]
              {:type :destroy
               :node node})

        ;; Create a new validator
        2/5 (let [v (-> (c/on-nodes test
                                    [(rand-nth (:nodes test))]
                                    (t/fn [test :- Test, node :- Node]
                                      (gen-validator)))
                        first
                        val
                        (assoc :votes 2))]
              {:type      :add
               :version   (:version config)
               :validator v})

        ;; Remove a validator
        1/5 (let [v (rand-validator config)]
              {:type :remove
               :version (:version config)
               :pub_key (:pub_key v)})

        ; Adjust a node's weight
        0/5 (let [v (rand-validator config)]
              {:type    :alter-votes
               :version (:version config)
               :pub_key (:pub_key v)
               ; Long forces core.typed to admit it's not AnyInteger;
               ; strictly speaking we might go out of bounds
               :votes   (long (max 1 (+ (:votes v) (- (rand-int 11) 5))))}))

      ; We rolled an impossible transition; try again
      (recur test config)))

(t/ann ^:no-check rand-legal-transition [Test Config -> Transition])
(defn rand-legal-transition
  "Generates a random transition on the given config which results in a legal
  state."
  [test config]
  (dt/with-retry [i 0]
    (if (<= 100 i)
      (throw (RuntimeException. (str "Unable to generate state transition from "
                                     (pr-str config)
                                     " in less than 100 tries; aborting.")))
      (let [t (rand-transition test config)]
        (step config t)
        t))
    (catch AssertionError e
      (info e :invalid-transition)
      (retry (inc i)))))

(t/tc-ignore
(defn prospective-validator-by-pub-key-data
  "Looks up a prospective validator by key data alone; e.g. instead of by
  {:type ...  :data ...}."
  [config pub-key-data]
  (loop [validators (seq (vals (:prospective-validators config)))]
    (when validators
      (if (= pub-key-data (:data (:pub_key (first validators))))
        (first validators)
        (recur (next validators)))))))


(t/ann validator-by-pub-key-data [Config ShortKey -> (t/Option Validator)])
(defn validator-by-pub-key-data
  "Looks up a validator by key data alone; e.g. instead of by {:type ... :data
  ...}."
  [config pub-key-data]
  (t/loop [validators :- (t/Option (t/NonEmptyASeq Validator)) (seq (vals (:validators config)))]
    (when validators
      (if (= pub-key-data (:data (:pub_key (first validators))))
        (first validators)
        (recur (next validators))))))

(t/tc-ignore
(defn current-config
  "Combines our internal test view of which nodes are running what validators
  with a transactional read of the current state of validator votes, producing
  a config that can be used to generate cluster transitions."
  [test node]
  ; TODO: improve node selection
  (let [local-config   @(:validator-config test)
        cluster-config (tc/validator-set node)
        version     (:version cluster-config)
        ; Update validator weights
        validators' (reduce (fn [validators' cluster-validator]
                              (let [validator (validator-by-pub-key-data
                                                local-config
                                                (:pub_key cluster-validator))]
                                (if validator
                                  ; We're updating an existing validator weight
                                  (update validators' (:pub_key validator)
                                          assoc
                                          :votes (:power cluster-validator))

                                  ; Ah, we added a validator. Promote it from
                                  ; :prospective-validators to the current map
                                  (let [v (prospective-validator-by-pub-key-data
                                            local-config
                                            (:pub_key cluster-validator))]
                                    ; TODO: remove from prospective-validators
                                    (assoc validators' (:pub_key v) v)))))
                            (:validators local-config)
                            (:validators cluster-config))
        ; TODO: deal with removed validators
        ]
    (assoc local-config
           :version version
           :validators validators')))

(defn refresh-config!
  "Attempts to update the test's config with new information from the cluster.
  Returns our estimate of the current config. Not threadsafe."
  [test]
  ; TODO: make this threadsafe
  (or (reduce (fn [_ node]
                (try
                  (when-let [c (current-config test node)]
                    (reset! (:validator-config test) c)
                    (reduced c))
                  (catch java.io.IOException e
                    nil)))
              nil
              (shuffle (:nodes test)))
      @(:validator-config test)))

(defn generator
  "A generator of legal state transitions on the current validator state."
  []
  (reify gen/Generator
    (op [this test process]
      (try
        (let [config (refresh-config! test)]
          (info (with-out-str (pprint (compact-config config))))
          {:type  :info
           :f     :transition
           :value (rand-legal-transition test config)})
        (catch Exception e
          (warn e "error generating transition")
          (throw e))))))

)
