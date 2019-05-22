# aws-util

Utilities for use with the [Cognitect AWS library for Clojure](https://github.com/cognitect-labs/aws-api).

## Installation

Use in `deps.edn` as a git dependency.

## Usage

In `aws-util.util`:

 * `credentials-provider` and `default-credentials-provider` give an alternative implementation of credentials, more reliable in my experience then Cognitect's. This uses AWS Java SDK 2.
 * `invoke-async-and-paginate` and `invoke-and-stream` are two helpers for paginating through paginated results of AWS operations, returning [Manifold](https://github.com/ztellman/manifold) deferred and stream, respectively.

## License

Copyright © 2019 Vadim Geshel

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
