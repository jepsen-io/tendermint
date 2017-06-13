(ns jepsen.tendermint.cli
  "Command line interface to Tendermint tests."
  (:require [clojure.pprint :refer [pprint]]
            [clojure.tools.cli :as tc]
            [jepsen [cli :as jc]]
            [jepsen.tendermint [core :as core]]))

(def opts
  "Extra command line opts."
  [[nil "--nemesis NEMESIS" "Nemesis to use; e.g. clocks"
   :default :none
   :parse-fn keyword]])

(defn -main
  [& args]
  (jc/run! (merge (jc/serve-cmd)
                  (jc/single-test-cmd {:test-fn core/test
                                       :opt-spec opts}))
          args))
