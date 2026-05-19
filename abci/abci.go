package abci

import (
	"fmt"
	"time"

	"cosmossdk.io/log"
	abci "github.com/cometbft/cometbft/abci/types"
	sdk "github.com/cosmos/cosmos-sdk/types"

	"github.com/cosmos/cosmos-sdk/baseapp"

	"github.com/skip-mev/block-sdk/v2/block"
	"github.com/skip-mev/block-sdk/v2/block/proposals"
	"github.com/skip-mev/block-sdk/v2/block/utils"
)

type (
	// ProposalHandler is a wrapper around the ABCI++ PrepareProposal and ProcessProposal
	// handlers.
	ProposalHandler struct {
		logger                   log.Logger
		txDecoder                sdk.TxDecoder
		txEncoder                sdk.TxEncoder
		mempool                  block.Mempool
		useCustomProcessProposal bool
	}
)

// NewDefaultProposalHandler returns a new ABCI++ proposal handler. This proposal handler will
// iteratively call each of the lanes in the chain to prepare and process the proposal. This
// will not use custom process proposal logic.
func NewDefaultProposalHandler(
	logger log.Logger,
	txDecoder sdk.TxDecoder,
	txEncoder sdk.TxEncoder,
	mempool block.Mempool,
) *ProposalHandler {
	return &ProposalHandler{
		logger:                   logger,
		txDecoder:                txDecoder,
		txEncoder:                txEncoder,
		mempool:                  mempool,
		useCustomProcessProposal: false,
	}
}

// New returns a new ABCI++ proposal handler with the ability to use custom process proposal logic.
//
// NOTE: It is highly recommended to use the default proposal handler unless you have a specific
// use case that requires custom process proposal logic.
func New(
	logger log.Logger,
	txDecoder sdk.TxDecoder,
	txEncoder sdk.TxEncoder,
	mempool block.Mempool,
	useCustomProcessProposal bool,
) *ProposalHandler {
	return &ProposalHandler{
		logger:                   logger,
		txDecoder:                txDecoder,
		txEncoder:                txEncoder,
		mempool:                  mempool,
		useCustomProcessProposal: useCustomProcessProposal,
	}
}

// PrepareProposalHandler prepares the proposal by selecting transactions from each lane
// according to each lane's selection logic. We select transactions in the order in which the
// lanes are configured on the chain. Note that each lane has an boundary on the number of
// bytes/gas that can be included in the proposal. By default, the default lane will not have
// a boundary on the number of bytes that can be included in the proposal and will include all
// valid transactions in the proposal (up to MaxBlockSize, MaxGasLimit).
func (h *ProposalHandler) PrepareProposalHandler() sdk.PrepareProposalHandler {
	return func(ctx sdk.Context, req *abci.PrepareProposalRequest) (resp *abci.PrepareProposalResponse, err error) {
		if req.Height <= 1 {
			return &abci.PrepareProposalResponse{Txs: req.Txs}, nil
		}

		tTotal := time.Now()

		// In the case where there is a panic, we recover here and return an empty proposal.
		defer func() {
			if rec := recover(); rec != nil {
				h.logger.Error("failed to prepare proposal", "err", err)

				// TODO: Should we attempt to return a empty proposal here with empty proposal info?
				resp = &abci.PrepareProposalResponse{Txs: make([][]byte, 0)}
				err = fmt.Errorf("failed to prepare proposal: %v", rec)
			}
		}()

		h.logger.Debug(
			"mempool distribution before proposal creation",
			"distribution", h.mempool.GetTxDistribution(),
			"height", req.Height,
		)

		// Get the max gas limit and max block size for the proposal.
		_, maxGasLimit := proposals.GetBlockLimits(ctx)
		proposal := proposals.NewProposal(h.logger, req.MaxTxBytes, maxGasLimit)

		// Fill the proposal with transactions from each lane.
		prepareLanesHandler := ChainPrepareLanes(h.mempool.Registry())
		tHandler := time.Now()
		finalProposal, err := prepareLanesHandler(ctx, proposal)
		prepareHandlerMs := float64(time.Since(tHandler).Nanoseconds()) / 1e6
		totalMs := float64(time.Since(tTotal).Nanoseconds()) / 1e6
		// logfmt: Loki 可直接解析
		fmt.Printf("msg=sdk_prepare_timing prepare_handler_ms=%.3f total_ms=%.3f\n", prepareHandlerMs, totalMs)
		if err != nil {
			h.logger.Error("failed to prepare proposal", "err", err)
			return &abci.PrepareProposalResponse{Txs: make([][]byte, 0)}, err
		}

		h.logger.Debug(
			"prepared proposal",
			"num_txs", len(finalProposal.Txs),
			"total_tx_bytes", finalProposal.Info.BlockSize,
			"max_tx_bytes", finalProposal.Info.MaxBlockSize,
			"total_gas_limit", finalProposal.Info.GasLimit,
			"max_gas_limit", finalProposal.Info.MaxGasLimit,
			"height", req.Height,
		)

		h.logger.Info(
			"mempool distribution after proposal creation",
			"distribution", h.mempool.GetTxDistribution(),
			"height", req.Height,
		)

		return &abci.PrepareProposalResponse{
			Txs: finalProposal.Txs,
		}, nil
	}
}

// ProcessProposalHandler processes the proposal by verifying all transactions in the proposal
// according to each lane's verification logic. Proposals are verified similar to how they are
// constructed. After a proposal is processed, it should amount to the same proposal that was prepared.
// The proposal is verified in a greedy fashion, respecting the ordering of lanes. A lane will
// verify all transactions in the proposal that belong to the lane and pass any remaining transactions
// to the next lane in the chain.
func (h *ProposalHandler) ProcessProposalHandler() sdk.ProcessProposalHandler {
	if !h.useCustomProcessProposal {
		return baseapp.NoOpProcessProposal()
	}

	return func(ctx sdk.Context, req *abci.ProcessProposalRequest) (resp *abci.ProcessProposalResponse, err error) {
		if req.Height <= 1 {
			return &abci.ProcessProposalResponse{Status: abci.PROCESS_PROPOSAL_STATUS_ACCEPT}, nil
		}

		// In the case where any of the lanes panic, we recover here and return a reject status.
		defer func() {
			if rec := recover(); rec != nil {
				h.logger.Error("failed to process proposal", "recover_err", rec)

				resp = &abci.ProcessProposalResponse{Status: abci.PROCESS_PROPOSAL_STATUS_REJECT}
				err = fmt.Errorf("failed to process proposal: %v", rec)
			}
		}()

		// Decode the transactions in the proposal. These will be verified by each lane in a greedy fashion.
		decodedTxs, err := utils.GetDecodedTxs(h.txDecoder, req.Txs)
		if err != nil {
			h.logger.Error("failed to decode txs", "err", err)
			return &abci.ProcessProposalResponse{Status: abci.PROCESS_PROPOSAL_STATUS_REJECT}, err
		}

		// Build handler that will verify the partial proposals according to each lane's verification logic.
		processLanesHandler := ChainProcessLanes(h.mempool.Registry())

		// Verify the proposal.
		finalProposal, err := processLanesHandler(
			ctx,
			proposals.NewProposalWithContext(ctx, h.logger),
			decodedTxs,
		)
		if err != nil {
			h.logger.Error("failed to validate the proposal", "err", err)
			return &abci.ProcessProposalResponse{Status: abci.PROCESS_PROPOSAL_STATUS_REJECT}, err
		}

		h.logger.Debug(
			"processed proposal",
			"num_txs", len(finalProposal.Txs),
			"total_tx_bytes", finalProposal.Info.BlockSize,
			"max_tx_bytes", finalProposal.Info.MaxBlockSize,
			"total_gas_limit", finalProposal.Info.GasLimit,
			"max_gas_limit", finalProposal.Info.MaxGasLimit,
			"height", req.Height,
		)

		return &abci.ProcessProposalResponse{Status: abci.PROCESS_PROPOSAL_STATUS_ACCEPT}, nil
	}
}
