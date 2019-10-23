(ns aws-util.util
  "Utilities for the Cognitect AWS API"
  (:require [cognitect.aws.client.api :as aws]
            [cognitect.aws.client.api.async :as aws.async]
            [cognitect.aws.credentials :as credentials]
            [cognitect.aws.region :as region]
            [manifold.deferred :as d :refer [let-flow]]
            [manifold.stream :as ms])
  (:import [software.amazon.awssdk.auth.credentials DefaultCredentialsProvider AwsCredentials AwsSessionCredentials AwsCredentialsProvider]
           [software.amazon.awssdk.regions.providers DefaultAwsRegionProviderChain]))

(defn credentials-provider
  "A credentials provider that uses the AWS SDK implementation instead of Cognitect's.
  The provider built into the cognitect library doesn't work reliably on ECS."
  []
  (let [^AwsCredentialsProvider provider (-> (DefaultCredentialsProvider/builder)
                                             (.asyncCredentialUpdateEnabled true)
                                             (.build))]
    (reify credentials/CredentialsProvider
      (fetch [_]
        (let [^AwsCredentials creds (.resolveCredentials provider)]
          (cond-> {:aws/access-key-id     (.accessKeyId creds)
                   :aws/secret-access-key (.secretAccessKey creds)}
            (instance? AwsSessionCredentials creds) (assoc :aws/session-token (.sessionToken ^AwsSessionCredentials creds))))))))

(defonce ^:private -default-credentials-provider (delay (credentials-provider)))

(defn default-credentials-provider
  "Return a global, shared credential provider. This uses the default provider chain and automatically refreshes.

  This implementation is more reliable that the one included in the Cognitect library, which I've seen fail mysteriously when running on ECS."
  []
  @-default-credentials-provider)

(defn region-provider []
  (let [chain (DefaultAwsRegionProviderChain.)
        region (str (.getRegion chain))]
    (reify region/RegionProvider
      (fetch [_]
        region))))

(def ^:private -default-region-provider
  (delay (region-provider)))

(defn default-region-provider []
  @-default-region-provider)

(defn invoke-async-and-paginate
  "Repeat the op invocation passing `nextToken` repeatedly.

  The name of the next token field can be overridden by passing `:result-next-token-key` and `:request-next-token-key` in the `op-map`.

  Return a manifold deferred which will yield the complete result set. The shape of the yieled object is the same as that of a single invocation response, typically, `{:elements [...], :nextToken \"...\"}` "
  [client {:keys [result-next-token-key request-next-token-key]
           :as   op-map
           :or   {result-next-token-key  :nextToken
                  request-next-token-key :nextToken}}]
  (d/loop [op op-map
           all-res {}]
    (d/chain
     (-> (aws.async/invoke client (dissoc op :result-next-token-key :request-next-token-key))
         ms/->source
         ms/take!)
     (fn [res]
       (let [next-token (get res result-next-token-key)
             all-res    (merge-with (fn [left right]
                                      (if (and (coll? left) (coll? right))
                                        (concat left right)
                                        right))
                                    all-res
                                    res)]
         (if (not-empty next-token)
           (d/recur (assoc-in op [:request request-next-token-key] next-token) all-res)
           all-res))))))

(defn invoke-and-stream
  "Repeatedly invoke an op paging through the results and appending them to a manifold stream.

  See `invoke-async-and-paginate` for iteration details.

  The result elements are extracted from the AWS API responce using the `response-key` (can be a keyword or a function). If a stream is not passed it will be created. Returns the stream. Does not block but will pause calling AWS if backpressure is applied on the stream (pass a limited-capacity stream to create backpressure).

  Note that the shape of the result is different from that of the AWS operation."
  ([client op-map response-key]
   (invoke-and-stream client op-map response-key (ms/stream) true))
  ([client
    {:keys [result-next-token-key request-next-token-key]
     :as   op-map
     :or   {result-next-token-key  :nextToken
            request-next-token-key :nextToken}}
    response-key stream close?]
   (d/chain
    (d/loop [op op-map]
      (d/chain
       (-> (aws.async/invoke client (dissoc op :result-next-token-key :request-next-token-key))
           ms/->source
           ms/take!)
       (fn [res]
         (let [next-token (get res result-next-token-key)
               results    (seq (response-key res))
               put-done   (if results
                            (ms/put-all! stream results)
                            (d/success-deferred true))]
           ;; first deliver all results to the stream, then query for the next batch
           (d/chain
            put-done
            (fn [_]
              (if (not-empty next-token)
                (d/recur (assoc-in op [:request request-next-token-key] next-token))
                :done)))))))
    (fn [_]
      (when close?
        (ms/close! stream))))
   stream))
