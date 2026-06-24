package base

import (
	sdk "github.com/cosmos/cosmos-sdk/types"

	"github.com/skip-mev/block-sdk/v2/block"
)

// LaneOption defines a function that can be used to set options on a lane.
type LaneOption func(*BaseLane)

// WithAnteHandler sets the ante handler for the lane.
func WithAnteHandler(anteHandler sdk.AnteHandler) LaneOption {
	return func(l *BaseLane) { l.cfg.AnteHandler = anteHandler }
}

// WithFastNonceVerifier sets an optional fast-path nonce verifier on the lane
// and immediately rebuilds the prepareLaneHandler to pick it up.
// Safe to use either at construction time (via NewBaseLane options) or
// after construction (via lane.WithOptions).
func WithFastNonceVerifier(fn FastNonceVerifier) LaneOption {
	return func(l *BaseLane) {
		l.fastNonceVerifier = fn
		// Rebuild the prepare handler so the new verifier takes effect immediately.
		handler := NewDefaultProposalHandler(l)
		handler.WithFastNonceVerifier(fn)
		l.prepareLaneHandler = handler.PrepareLaneHandler()
	}
}

// WithPrepareLaneHandler sets the prepare lane handler for the lane. This handler
// is called when a new proposal is being requested and the lane needs to submit
// transactions it wants included in the block.
func WithPrepareLaneHandler(prepareLaneHandler PrepareLaneHandler) LaneOption {
	return func(l *BaseLane) {
		if prepareLaneHandler == nil {
			panic("prepare lane handler cannot be nil")
		}

		l.prepareLaneHandler = prepareLaneHandler
	}
}

// WithProcessLaneHandler sets the process lane handler for the lane. This handler
// is called when a new proposal is being verified and the lane needs to verify
// that the transactions included in the proposal are valid respecting the verification
// logic of the lane.
func WithProcessLaneHandler(processLaneHandler ProcessLaneHandler) LaneOption {
	return func(l *BaseLane) {
		if processLaneHandler == nil {
			panic("process lane handler cannot be nil")
		}

		l.processLaneHandler = processLaneHandler
	}
}

// WithMatchHandler sets the match handler for the lane. This handler is called
// when a new transaction is being submitted to the lane and the lane needs to
// determine if the transaction should be processed by the lane.
func WithMatchHandler(matchHandler MatchHandler) LaneOption {
	return func(l *BaseLane) {
		if matchHandler == nil {
			panic("match handler cannot be nil")
		}

		l.matchHandler = matchHandler
	}
}

// WithSignerMatchHandler sets the signer-aware match handler for the lane. This
// handler is used when the caller has already extracted the first signer address.
func WithSignerMatchHandler(matchHandler SignerMatchHandler) LaneOption {
	return func(l *BaseLane) {
		if matchHandler == nil {
			panic("signer match handler cannot be nil")
		}

		l.signerMatchHandler = matchHandler
	}
}

// WithMempool sets the mempool for the lane. This mempool is used to store
// transactions that are waiting to be processed.
func WithMempool(mempool block.LaneMempool) LaneOption {
	return func(l *BaseLane) {
		if mempool == nil {
			panic("mempool cannot be nil")
		}

		l.LaneMempool = mempool
	}
}

// WithMempoolConfigs sets the mempool for the lane with the given lane config
// and TxPriority struct. This mempool is used to store transactions that are waiting
// to be processed.
func WithMempoolConfigs[C comparable](cfg LaneConfig, txPriority TxPriority[C]) LaneOption {
	return func(l *BaseLane) {
		l.LaneMempool = NewMempool(
			txPriority,
			cfg.SignerExtractor,
			cfg.MaxTxs,
		)
	}
}
