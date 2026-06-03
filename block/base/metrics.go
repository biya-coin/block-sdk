package base

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const namespace = "biyachain"

var (
	// PrepareLaneSeconds 记录每次 PrepareLane 内部三个阶段的耗时（秒）。
	// label "step": handler / get_info / update
	PrepareLaneSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "proposal",
		Name:      "prepare_lane_seconds",
		Help:      "Time spent in PrepareLane per lane (handler / get_info / update).",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 1, 1.5, 2},
	}, []string{"lane", "step"}) // step: handler / get_info / update

	// LanePrepareTotalSeconds records the full DefaultPrepareLaneHandler duration.
	LanePrepareTotalSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "proposal",
		Name:      "lane_prepare_total_seconds",
		Help:      "Total time spent inside DefaultPrepareLaneHandler per lane.",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1},
	}, []string{"lane"})

	// LaneSimSeconds 记录 DefaultPrepareLaneHandler 内部每个子步骤的累计耗时（秒）。
	// label "step": select / tx / sender_info / skipped_sender / tx_info /
	// limits / match / proposal_contains / verify / include / next / logging / flush / overhead / other
	LaneSimSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Subsystem: "proposal",
		Name:      "lane_sim_seconds",
		Help:      "Per-block cumulative time for each sub-step inside PrepareLaneHandler.",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1},
	}, []string{"lane", "step"})
)
