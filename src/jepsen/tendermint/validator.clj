(ns jepsen.tendermint.validator
  "Supports validator set configuration and changes."
  (:require [clojure.set :as set]
            [clojure.tools.logging :refer [info warn]]
            [clojure.pprint :refer [pprint]]
            [cheshire.core :as json]
            [dom-top.core :as dt]
            [jepsen.tendermint [client :as tc]
                               [util :refer [base-dir]]]
            [jepsen [util :as util :refer [map-vals]]
                    [control :as c]
                    [client :as client]
                    [nemesis :as nemesis]
                    [generator :as gen]]))

(defn gen-validator
  "Generate a new validator structure, and return the validator's data as a
  map."
  []
  (c/cd base-dir
        (-> (c/exec "./tendermint" :--home base-dir :gen_validator)
            (json/parse-string true))))

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

(defn initial-config
  "Constructs an initial configuration for a test with a list of :nodes
  provided."
  [test]
  (let [validators (c/with-test-nodes test
                     (gen-validator))]
    (config
      {:validators (reduce (fn [m [node v]] (assoc m (:pub_key v) v))
                           {}
                           validators)
       :nodes      (map-vals :pub_key validators)
       :node-set   (set (:nodes test))
       :max-byzantine-vote-fraction (:max-byzantine-vote-fraction test 1/3)})))

(defn genesis
  "Computes a genesis.json structure for the given config."
  [config]
  {:app_hash      ""
   :chain_id      "jepsen"
   :genesis_time  "0001-01-01T00:00:00.000Z"
   :validators    (->> (:validators config)
                       vals
                       (map (fn [validator]
                              (let [pub-key (:pub_key validator)
                                    name (->> (:nodes config)
                                              (filter #(= (val %) pub-key))
                                              first
                                              key)]
                                {:amount  2
                                 :name    name
                                 :pub_key pub-key}))))})

(defn pub-key-on-node
  "What pubkey is running on a given node?"
  [config node]
  (get-in config [:nodes node]))

(defn total-votes
  "How many votes are in the validator set total?"
  [config]
  (->> (:validators config)
       vals
       (map :votes)
       (reduce + 0)))

(defn vote-fractions
  "A map of validator public keys to the fraction of the vote they control."
  [config]
  (let [total (total-votes config)]
    (->> (:validators config)
         (map-vals (fn [v]
                     (/ (:votes v) total))))))

(defn nodes-running-validators
  "Takes a config, yielding a map of validator keys to groups of nodes that run
  that validator."
  [config]
  (->> (:nodes config)
       (reduce (fn [m [node pub-key]]
                 (update m pub-key conj node))
               {})))

(defn byzantine-validator-keys
  "A collection of all validator keys in the validator set which are running on
  more than one node."
  [config]
  (->> (nodes-running-validators config)
       (filter (fn [[key nodes]] (< 1 (count nodes))))
       (map key)
       (keep (:validators config))
       (map :pub_key)))

(defn dup-groups
  "Takes a config. Computes a map of:

      {:groups  A collection of groups of nodes, each running the same validator
       :singles Groups with only one nodes
       :dups    Groups with multiple nodes}"
  [config]
  (let [groups (-> config nodes-running-validators vals)]
    {:groups  groups
     :singles (filter #(= 1 (count %)) groups)
     :dups    (filter #(< 1 (count %)) groups)}))

(defn at-least-one-running-validator?
  "Does the given config have at least one validator which is running on some
  node?"
  [config]
  (seq (set/intersection (set (keys (:validators config)))
                         (set (vals (:nodes config))))))


(defn omnipotent-byzantines?
  "Does this config contain any byzantine validator which controls more than
  max-byzantine-vote-fraction of the vote?"
  [config]
  (let [vfs       (vote-fractions config)
        threshold (:max-byzantine-vote-fraction config)]
    (some (fn [k] (<= threshold (get vfs k)))
          (byzantine-validator-keys config))))

(defn assert-valid
  "Ensures that the given config is valid, and returns it. Throws
  AssertError if not.

  - At least one validator which is running on a node
  - No byzantine aggregator controls too much of the vote
  - Validators run on real nodes
  - Validators have nonzero votes"
  [config]
  (assert (at-least-one-running-validator? config))
  (assert (not (omnipotent-byzantines? config)))
  (assert (every? (:node-set config) (keys (:nodes config))))
  (assert (not-any? zero? (map :votes (vals (:validators config)))))
  config)

; Possible state transitions:
; - Create an instance of a validator on a node
; - Destroy a validator instance on some node

; - Add a validator to the validator set
; - Remove a validator from the config set

; - Adjust the weight of a validator

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
                (assoc-in config [:nodes n] v))

      ; Destroy a validator on a node
      :destroy (update config :nodes dissoc (:node transition))

      ; Add a validator to the validator set
      :add (let [v (:validator transition)]
             (assert (not (get-in config [:validators (:pub_key v)])))
             (assoc-in config [:validators (:pub_key v)] v))

      ; Remove a validator from the validator set
      :remove (update config :validators dissoc
                      (:pub_key transition))

      ; Change the votes allocated to a validator
      :alter-votes (let [k (:pub_key transition)
                         v (:votes transition)]
                     (assert (get-in config [:validators k]))
                     (assoc-in config [:validators k :votes] v)))))

(defn rand-validator
  "Selects a random validator from the config."
  [config]
  (rand-nth (vals (:validators config))))

(defn rand-free-node
  "Selects a random node which isn't running anything."
  [config]
  (rand-nth (set/difference (:node-set config)
                            (set (keys (:nodes config))))))

(defn rand-taken-node
  "Selects a random node that's running a validator."
  [config]
  (rand-nth (keys (:nodes config))))

(defn rand-transition
  "Generates a random transition on the given config."
  [test config]
  (condp <= (rand)
    ; Create a new validator
    2/3 (let [v (-> (c/on-nodes test (list (rand-nth (:nodes test)))
                                (fn [test node]
                                  (gen-validator)))
                    first
                    val
                    (assoc :votes 2))]
          {:type      :add
           :version   (:version config)
           :validator v})

    ; Remove a validator
    1/3 (let [v (rand-validator config)]
          {:type :remove
           :version (:version config)
           :pub_key (:pub_key v)})

    ; Adjust a node's weight
    1/4 (let [v (rand-validator config)]
          {:type    :alter-votes
           :version (:version config)
           :pub_key (:pub_key v)
           :votes   (max 1 (+ (:votes v) (- (rand-int 11) 5)))})

    ; Nuke a node
    0 {:type :destroy
       :node (rand-taken-node config)}))

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

(defn prospective-validator-by-pub-key-data
  "Looks up a prospective validator by key data alone; e.g. instead of by
  {:type ...  :data ...}."
  [config pub-key-data]
  (loop [validators (seq (vals (:prospective-validators config)))]
    (when validators
      (if (= pub-key-data (:data (:pub_key (first validators))))
        (first validators)
        (recur (next validators))))))

(defn validator-by-pub-key-data
  "Looks up a validator by key data alone; e.g. instead of by {:type ... :data
  ...}."
  [config pub-key-data]
  (loop [validators (seq (vals (:validators config)))]
    (when validators
      (if (= pub-key-data (:data (:pub_key (first validators))))
        (first validators)
        (recur (next validators))))))

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
          (info (with-out-str (pprint config)))
          {:type  :info
           :f     :transition
           :value (rand-legal-transition test config)})
        (catch Exception e
          (warn e "error generating transition")
          (throw e))))))
