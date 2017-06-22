(ns datasplash.pubsub
  (:require [datasplash.core :refer :all])
  (:import (org.apache.beam.sdk.io.gcp.pubsub PubsubIO$Read PubsubIO$Write)
           (org.apache.beam.sdk.values PBegin)
           (org.apache.beam.sdk Pipeline)))

(defn read-from-pubsub
  "Create an unbounded PCollection from a pubsub stream"
  [subscription-or-topic options p]
  (cond-> p
          (instance? Pipeline p) (PBegin/in)
          (re-find #"subscriptions" subscription-or-topic) (apply-transform (.fromSubscription PubsubIO$Read subscription-or-topic) {} options)
          (re-find #"topics" subscription-or-topic) (apply-transform (.fromTopic PubsubIO$Read subscription-or-topic) {} options)))

(defn write-to-pubsub
  "Write the contents of an unbounded PCollection to to a pubsub stream"
  [topic options pcoll]
  (-> pcoll
      (apply-transform (.to PubsubIO$Write topic) {} options)))
