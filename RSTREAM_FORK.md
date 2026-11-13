<!-- SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly> -->
<!-- SPDX-License-Identifier: MIT -->

# rstream interceptor maintenance

This fork reconciles the runtime changes in upstream v0.1.51, commit
`3084336e7b893862766504226840a9f3e4f999c9`, with the existing rstream FlexFEC,
transport-feedback, throughput and sparse RTX extensions. Upstream comparison
starts at `9a8ca49f93560d6f6590352e6c5abe2c1413fd81`. Consumers pin immutable fork
commits; a pseudo-version's ancestor tag does not identify its reviewed source.
The landing README and fork release/merge automation remain unchanged.

## Retained behavior

- The FlexFEC-03 decoder remains exported and hardened for video consumers.
- Feedback history remains bounded by 32,768 records and 60 seconds. Received
  records retire only after successful parsing; missing records remain available
  for late receipts. Duplicate loss reports and late recovery retain their
  existing accounting.
- The observed-throughput ring retains chronological ordering, a configurable
  time window, integer saturation and a 32,768-record ceiling. It excludes the
  first packet's bytes from a measured interval and emits no invented rate for
  a single timestamp. This deliberately retains the fork implementation rather
  than upstream's different 500-entry estimator. Startup allocation is bounded;
  existing steady-state allocation and chronological-reference tests remain.
- Delay grouping retains latest-departure and bounded compressed-burst behavior.
  Upstream's removal of stored ACK lists is adopted using first-arrival metadata,
  so groups no longer retain packets merely to bound their time span.
- RTX sequence numbers are allocated when repair packets are prepared for sending,
  independently of how many primary packets were cached.

## Reconciled upstream behavior

Upstream TWCC status-count limits, unknown/evicted packet handling and 24-bit
reference-time unwrapping are retained with the fork's history lifecycle. Delta
validation also rejects nil entries before dereferencing them. Tests cover
truncation/padding, long unknown runs, reordered feedback and reference rollover.
Rollover tests reuse one adapter across feedback batches, as a real connection
does. History-eviction tests select their intended history capacity explicitly.

Upstream NACK pending/RTT suppression and the shared retransmission bitrate budget
are retained. A queued RTX packet can be a clone of its cached source: the cached
source owns pending/recent-send state, and both references remain alive until the
writer/pacer releases the resend callback. Marking the clone alone leaves the
cached source incorrectly pending. `TestResponderInterceptor_PendingRTX` covers
this lifecycle alongside the non-RTX case, concurrent repeated NACKs, pacing,
retry and close. The sparse RTX sequence regression remains unchanged except for
the upstream private method's additional bitrate argument.

## Qualification

Run the full race suite and vet, plus repeated pending-RTX tests:

```sh
go test -race ./...
go vet ./...
go test -race ./pkg/nack -run 'Pending(RTT|RTX)' -count=10
```

The pending-RTX regression must fail if resend completion updates the clone
instead of the cached source. Preserve the full FlexFEC, unknown-feedback,
loss/recovery, observed-throughput and allocation tests. Producer/distributor
decoded-media and ICE-restart qualification with the exact WebRTC/interceptor
pins is required before accepting consumer updates. Passing this library suite
alone does not certify those applications or production capacity.
