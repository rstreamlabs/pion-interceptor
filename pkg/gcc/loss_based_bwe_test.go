// SPDX-FileCopyrightText: 2026 The Pion community <https://pion.ly>
// SPDX-License-Identifier: MIT

package gcc

import (
	"testing"

	"github.com/pion/logging"
	"github.com/stretchr/testify/assert"
)

func TestLossBasedBWEHonorsConfiguredBitrateBounds(t *testing.T) {
	const (
		initialBitrate = 8_000_000
		minimumBitrate = 2_000_000
		maximumBitrate = 10_000_000
	)
	estimator := newLossBasedBWE(
		initialBitrate,
		minimumBitrate,
		maximumBitrate,
		logging.NewDefaultLoggerFactory(),
	)
	estimator.bitrate = 100_000
	assert.Equal(t, minimumBitrate, estimator.getEstimate(initialBitrate).TargetBitrate)
	estimator.bitrate = 100_000_000
	assert.Equal(t, maximumBitrate, estimator.getEstimate(100_000_000).TargetBitrate)
}
