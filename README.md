<!-- SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly> -->
<!-- SPDX-License-Identifier: MIT -->

# rstream fork of Pion Interceptor

This repository maintains RTP/RTCP interceptors used by the rstream video examples. It derives from [Pion Interceptor](https://github.com/pion/interceptor) and retains the upstream Go module identity and MIT license.

The retained extensions support video repair, transport feedback and throughput estimation. The [API baseline](RSTREAM_FORK.md#api-baseline) describes the compatibility transition that still needs review.

Use the [fork guide](RSTREAM_FORK.md) to understand the retained changes and compatibility constraints before selecting a revision. Applications pin reviewed immutable replacements in their own module graph; a pseudo-version’s ancestor tag alone does not describe the reviewed source.

| Task                                | Start here                                                                                                                               |
| ----------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| Integrate rstream in an application | [Go reference SDK](https://github.com/rstreamlabs/rstream-go) or [application examples](https://github.com/rstreamlabs/rstream-examples) |
| Understand this fork                | [Retained behavior and maintenance](RSTREAM_FORK.md)                                                                                     |
| Learn the upstream API              | [Pion interceptor](https://github.com/pion/interceptor)                                                                                  |
| Review source examples              | [Examples](examples)                                                                                                                     |
| Contribute to upstream Pion         | [Contribution guide](https://github.com/pion/webrtc/wiki/Contributing)                                                                   |

For rstream-specific changes, use this repository’s issues and preserve the fork regressions. Report vulnerabilities privately through the [rstream security policy](https://github.com/rstreamlabs/.github/blob/main/SECURITY.md). Upstream attribution and license terms are in [LICENSE](LICENSE).
