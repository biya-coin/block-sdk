package proposals

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const namespace = "biyachain"

var UpdateProposalStepSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: namespace,
	Subsystem: "proposal",
	Name:      "update_proposal_step_seconds",
	Help:      "Sub-step durations inside Proposal.UpdateProposal.",
	Buckets:   []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25},
}, []string{"lane", "step"})
