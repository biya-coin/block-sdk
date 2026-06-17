package base

import (
	"fmt"

	"cosmossdk.io/log"
	"cosmossdk.io/math"
	sdk "github.com/cosmos/cosmos-sdk/types"

	signer_extraction "github.com/skip-mev/block-sdk/v2/adapters/signer_extraction_adapter"
	"github.com/skip-mev/block-sdk/v2/block"
)

var _ block.Lane = (*BaseLane)(nil)

// BaseLane is a generic implementation of a lane. It is meant to be used
// as a base for other lanes to be built on top of. It provides a default
// implementation of the MatchHandler, PrepareLaneHandler, ProcessLaneHandler,
// and CheckOrderHandler. To extend this lane, you must either utilize the default
// handlers or construct your own that you pass into the base/setters.
type BaseLane struct { //nolint
	// cfg stores functionality required to encode/decode transactions, maintains how
	// many transactions are allowed in this lane's mempool, and the amount of block
	// space this lane is allowed to consume.
	cfg LaneConfig

	// laneName is the name of the lane.
	laneName string

	// LaneMempool is the mempool that is responsible for storing transactions
	// that are waiting to be processed.
	block.LaneMempool

	// matchHandler is the function that determines whether a transaction
	// should be processed by this lane.
	matchHandler MatchHandler

	// prepareLaneHandler is the function that is called when a new proposal is being
	// requested and the lane needs to submit transactions it wants to be included in the block.
	prepareLaneHandler PrepareLaneHandler

	// processLaneHandler is the function that is called when a new proposal is being
	// verified and the lane needs to verify that the transactions included in the proposal
	// are valid respecting the verification logic of the lane.
	processLaneHandler ProcessLaneHandler

	// fastNonceVerifier is an optional fast-path nonce checker for single-signer txs.
	// When set, PrepareLaneHandler uses it instead of a full VerifyTx call.
	fastNonceVerifier FastNonceVerifier
}

// NewBaseLane returns a new lane base. When creating this lane, the type
// of the lane must be specified. The type of the lane is directly associated with the
// type of the mempool that is used to store transactions that are waiting to be processed.
func NewBaseLane(
	cfg LaneConfig,
	laneName string,
	options ...LaneOption,
) (*BaseLane, error) {
	lane := &BaseLane{
		cfg:      cfg,
		laneName: laneName,
	}

	lane.LaneMempool = NewMempool(
		DefaultTxPriority(),
		lane.cfg.SignerExtractor,
		lane.cfg.MaxTxs,
	)

	lane.matchHandler = DefaultMatchHandler()

	handler := NewDefaultProposalHandler(lane)
	if lane.fastNonceVerifier != nil {
		handler.WithFastNonceVerifier(lane.fastNonceVerifier)
	}
	lane.prepareLaneHandler = handler.PrepareLaneHandler()
	lane.processLaneHandler = handler.ProcessLaneHandler()

	for _, option := range options {
		option(lane)
	}

	if err := lane.ValidateBasic(); err != nil {
		return nil, err
	}

	return lane, nil
}

// ValidateBasic ensures that the lane was constructed properly. In the case that
// the lane was not constructed with proper handlers, default handlers are set.
func (l *BaseLane) ValidateBasic() error {
	if err := l.cfg.ValidateBasic(); err != nil {
		return err
	}

	if l.laneName == "" {
		return fmt.Errorf("lane name cannot be empty")
	}

	if l.LaneMempool == nil {
		return fmt.Errorf("lane mempool cannot be nil")
	}

	if l.matchHandler == nil {
		return fmt.Errorf("match handler cannot be nil")
	}

	if l.prepareLaneHandler == nil {
		return fmt.Errorf("prepare lane handler cannot be nil")
	}

	if l.processLaneHandler == nil {
		return fmt.Errorf("process lane handler cannot be nil")
	}

	return nil
}

// Match returns true if the transaction should be processed by this lane. This
// function first determines if the transaction matches the lane and then checks
// if the transaction is on the ignore list. If the transaction is on the ignore
// list, it returns false.
func (l *BaseLane) Match(ctx sdk.Context, tx sdk.Tx) bool {
	return l.matchHandler(ctx, tx)
}

// Name returns the name of the lane.
func (l *BaseLane) Name() string {
	return l.laneName
}

// Logger returns the logger for the lane.
func (l *BaseLane) Logger() log.Logger {
	return l.cfg.Logger
}

// TxDecoder returns the tx decoder for the lane.
func (l *BaseLane) TxDecoder() sdk.TxDecoder {
	return l.cfg.TxDecoder
}

// TxEncoder returns the tx encoder for the lane.
func (l *BaseLane) TxEncoder() sdk.TxEncoder {
	return l.cfg.TxEncoder
}

// SignerExtractor returns the signer extractor for the lane.
func (l *BaseLane) SignerExtractor() signer_extraction.Adapter {
	return l.cfg.SignerExtractor
}

// ContainsWithSigners returns true if the transaction is contained in the lane
// using signer data that was already extracted by the caller.
func (l *BaseLane) ContainsWithSigners(tx sdk.Tx, signers []signer_extraction.SignerData) bool {
	mempool, ok := l.LaneMempool.(interface {
		ContainsWithSigners(sdk.Tx, []signer_extraction.SignerData) bool
	})
	if !ok {
		return l.Contains(tx)
	}

	return mempool.ContainsWithSigners(tx, signers)
}

// RemoveWithSigners removes a transaction from the lane using signer data that
// was already extracted by the caller.
func (l *BaseLane) RemoveWithSigners(tx sdk.Tx, signers []signer_extraction.SignerData) error {
	mempool, ok := l.LaneMempool.(interface {
		RemoveWithSigners(sdk.Tx, []signer_extraction.SignerData) error
	})
	if !ok {
		return l.Remove(tx)
	}

	return mempool.RemoveWithSigners(tx, signers)
}

// GetMaxBlockSpace returns the maximum amount of block space that the lane is
// allowed to consume as a percentage of the total block space.
func (l *BaseLane) GetMaxBlockSpace() math.LegacyDec {
	return l.cfg.MaxBlockSpace
}

// SetMaxBlockSpace sets the maximum amount of block space that the lane is
// allowed to consume as a percentage of the total block space.
func (l *BaseLane) SetMaxBlockSpace(maxBlockSpace math.LegacyDec) {
	l.cfg.MaxBlockSpace = maxBlockSpace
}

// WithOptions returns a new lane with the given options.
func (l *BaseLane) WithOptions(options ...LaneOption) *BaseLane {
	for _, option := range options {
		option(l)
	}

	return l
}
