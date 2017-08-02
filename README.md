# jepsen.tendermint

Jepsen tests for the Tendermint distributed consensus system.

## Building

- Clone this repo
- Install JDK8 or higher
- Install [Leiningen](https://leiningen.org/)
- Optional: install gnuplot
- In the tendermint repository, run `lein run test`

To build a fat JAR file that you can run independently of leiningen, run `lein
uberjar`.

## Usage

`lein run serve` runs a web server for browsing test results.

`lein run test` runs a test. Use `lein run test --help` to see options: you'll
likely want to set `--node some.hostname ...` or `--nodes-file some_file`, and
adjust `--username` as desired.

For instance, to test a shifting cluster membership, with two clients per node,
for 3000 seconds, over a group of compare-and-set registers, you'd run

`lein run test --nemesis changing-validators --concurrency 2n --time-limit 3000 --workload cas-register`

## License

Copyright © 2017 Jepsen, LLC

Distributed under the Apache Public License 2.0.
