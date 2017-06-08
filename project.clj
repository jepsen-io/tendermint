(defproject jepsen.tendermint "0.1.0-SNAPSHOT"
  :description "Jepsen tests for the Tendermint Byzantine consensus system"
  :url "http://github.com/jepsen-io/tendermint"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [cheshire "5.7.1"]
                 [slingshot "0.12.2"]
                 [clj-http "3.6.1"]
                 [jepsen "0.1.6-SNAPSHOT"]]
  :jvm-opts ["-Xmx12g"
             "-XX:+UseConcMarkSweepGC"
             "-XX:+UseParNewGC"
             "-XX:+CMSParallelRemarkEnabled"
             "-XX:+AggressiveOpts"
             "-XX:+UseFastAccessorMethods"
             "-XX:MaxInlineLevel=32"
             "-XX:MaxRecursiveInlineLevel=2"
             "-server"]
  :main jepsen.tendermint.cli)
