<!-- SPDX-FileCopyrightText: 2026 rstream -->
<!-- SPDX-License-Identifier: MIT -->

# Confirm the interceptor API baseline

Pending before merging or releasing the updated fork.

Upstream 0.1.51 changes the legacy feedback API and TWCC sender value comparison. The current rstream consumers pass their tests, but the API checker still detects differences from this fork's older main branch.

Review the affected callers, decide which compatibility is required, and document the supported API before release. Keep the API checker enabled; do not hide its findings or patch Pion merely to make it green.
