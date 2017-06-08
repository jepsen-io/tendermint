(ns jepsen.tendermint.gowire
  "A verrrrry minimal implementation of Tendermint's custom serialization
  format: https://github.com/tendermint/go-wire"
  (:import (java.nio ByteBuffer)))

(defn int-size
  "Number of bytes required to represent a number. We're only doing positive
  ones for now."
  [n]
  (cond (<  n 0x0)        (throw (IllegalArgumentException.
                                   (str "Number " n " can't be negative")))
        (<= n 0x0)        0
        (<= n 0xff)       1
        (<= n 0xffff)     2
        (<= n 0xffffff)   3
        (<= n 0xffffffff) 4
        true              (throw (IllegalArgumentException.
                                   (str "Number " n " is too large")))))

(defn buffer-for
  "Constructs a ByteBuffer big enough to write the given collection of objects
  to. Extra is an integer number of extra bytes to allocate."
  ([xs]
   (buffer-for xs 0))
  ([xs extra]
   (ByteBuffer/allocate
     (->> xs
          (map (fn [x]
                 (condp instance? x

                   ByteBuffer (+ 5 (.remaining x)))))
          (reduce + extra)))))

(defn write-varint!
  "Writes a varint to a ByteBuffer, and returns the buffer."
  [^ByteBuffer buf n]
  (let [int-size (int-size n)]
    (.put buf (unchecked-byte int-size)) ; Write size byte
    (condp = int-size
      0 nil
      1 (.put       buf (unchecked-byte n))
      2 (.putShort  buf (unchecked-short n))
      3 (throw (IllegalArgumentException. "Todo: bit munging"))
      4 (.putInt    buf (unchecked-int n))))
  buf)

(defn write-byte-buffer!
  "Writes a buffer x to a wire buffer buf. Returns buf."
  [^ByteBuffer buf ^ByteBuffer x]
  (write-varint! buf (.remaining x))
  (.put buf x)
  buf)
