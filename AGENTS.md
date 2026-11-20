<!-- SPDX-FileCopyrightText: 2026 rstream -->
<!-- SPDX-License-Identifier: MIT -->

# Repository guidance

## Shared conventions

Follow the existing architecture, naming and coding style. Preserve public contracts, authentication, certificate verification and project policy. Keep changes in the component that owns the behavior.

Update user documentation and examples when behavior changes. Run the relevant checks; place regression tests with the owning component. Keep secrets, execution logs, investigation notes and standalone upstream reproductions outside the repository. Do not link to private repositories from public source, documentation or PR descriptions.

## Fork-specific guidance

Keep the upstream README, attribution and contribution policy intact. Keep the README preamble limited to the reason for retaining the fork.

Preserve the upstream module path and check the exported API before repinning consumers. Run `go test -race ./...` and `go vet ./...`; keep FlexFEC, feedback rollover, bounded throughput and pending RTX covered. Resend completion must update the cached source packet's state. Qualify the video producer and distributor with the matching WebRTC revision.
