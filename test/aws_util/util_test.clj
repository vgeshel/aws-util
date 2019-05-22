(ns aws-util.util-test
  (:require [cognitect.aws.client.api :as aws]
            [cognitect.aws.client.api.async :as aws-async]
            [aws-util.util :as util-aws]
            [manifold.stream :as ms]))


(def -health
  (delay
   (aws/client {:api                  :health
                :retriable?           (constantly false)
                :credentials-provider (util-aws/default-credentials-provider)})))

(defn health []
  @-health)

(defn health-events []
  (util-aws/invoke-async-and-paginate (health) {:op :DescribeEvents}))

(defn health-events->stream []
  (util-aws/invoke-and-stream (health) {:op :DescribeEvents} :events))

(comment
  ;; verify that the two versions return the same thing
  (= (ms/stream->seq (health-events->stream)) (:events @(health-events))))
