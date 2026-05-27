package base

import (
	"fmt"
	"time"

	sdk "github.com/cosmos/cosmos-sdk/types"

	"github.com/skip-mev/block-sdk/v2/block"
	"github.com/skip-mev/block-sdk/v2/block/proposals"
	"github.com/skip-mev/block-sdk/v2/block/utils"
)

// PrepareLane will prepare a partial proposal for the lane. It will select transactions from the
// lane respecting the selection logic of the prepareLaneHandler. It will then update the partial
// proposal with the selected transactions. If the proposal is unable to be updated, we return an
// error. The proposal will only be modified if it passes all of the invariant checks.
func (l *BaseLane) PrepareLane(
	ctx sdk.Context,
	proposal proposals.Proposal,
	next block.PrepareLanesHandler,
) (proposals.Proposal, error) {
	l.Logger().Debug("preparing lane", "lane", l.Name())

	// Select transactions from the lane respecting the selection logic of the lane and the
	// max block space for the lane.
	limit := proposal.GetLaneLimits(l.cfg.MaxBlockSpace)

	t1 := time.Now()
	txsToInclude, cachedTxsWithInfo, txsToRemove, err := l.prepareLaneHandler(ctx, proposal, limit)
	prepareUs := time.Since(t1).Microseconds()
	if err != nil {
		l.Logger().Error(
			"failed to prepare lane",
			"lane", l.Name(),
			"err", err,
		)

		return proposal, err
	}

	// Remove all transactions that were invalid during the creation of the partial proposal.
	if err := utils.RemoveTxsFromLane(txsToRemove, l); err != nil {
		l.Logger().Error(
			"failed to remove transactions from lane",
			"lane", l.Name(),
			"err", err,
		)
	}

	// Get the transaction info for each selected transaction.
	// The handler appends txsToInclude and cachedTxsWithInfo in lockstep, so indices always
	// correspond. If the handler did not pre-compute TxWithInfo (e.g. MEV lane returns nil),
	// cachedTxsWithInfo is empty and we fall back to GetTxInfo for every tx.
	t3 := time.Now()
	txsWithInfo := make([]utils.TxWithInfo, len(txsToInclude))
	for i, tx := range txsToInclude {
		if i < len(cachedTxsWithInfo) {
			txsWithInfo[i] = cachedTxsWithInfo[i]
			continue
		}
		txInfo, err := l.GetTxInfo(ctx, tx)
		if err != nil {
			l.Logger().Error(
				"failed to get tx info",
				"lane", l.Name(),
				"err", err,
			)

			return proposal, err
		}
		txsWithInfo[i] = txInfo
	}
	infoUs := time.Since(t3).Microseconds()

	// Update the proposal with the selected transactions. This fails if the lane attempted to add
	// more transactions than the allocated max block space for the lane.
	t4 := time.Now()
	errUpdate := proposal.UpdateProposal(l, txsWithInfo)
	updateUs := time.Since(t4).Microseconds()

	totalLaneMs := float64(prepareUs+infoUs+updateUs) / 1e3
	fmt.Printf("msg=prepare_lane_timing lane=%s total_ms=%.3f prepare_handler_ms=%.3f get_info_ms=%.3f update_ms=%.3f include_count=%d\n",
		l.Name(), totalLaneMs,
		float64(prepareUs)/1e3,
		float64(infoUs)/1e3,
		float64(updateUs)/1e3,
		len(txsToInclude),
	)
	PrepareLaneSeconds.WithLabelValues(l.Name(), "handler").Observe(float64(prepareUs) / 1e6)
	PrepareLaneSeconds.WithLabelValues(l.Name(), "get_info").Observe(float64(infoUs) / 1e6)
	PrepareLaneSeconds.WithLabelValues(l.Name(), "update").Observe(float64(updateUs) / 1e6)

	if errUpdate != nil {
		l.Logger().Error(
			"failed to update proposal",
			"lane", l.Name(),
			"err", errUpdate,
			"num_txs_to_add", len(txsToInclude),
			"num_txs_to_remove", len(txsToRemove),
			"lane_max_block_size", limit.MaxTxBytes,
			"lane_max_gas_limit", limit.MaxGasLimit,
		)

		return proposal, errUpdate
	}

	l.Logger().Debug(
		"lane prepared",
		"lane", l.Name(),
		"num_txs_added", len(txsToInclude),
		"num_txs_removed", len(txsToRemove),
		"lane_max_block_size", limit.MaxTxBytes,
		"lane_max_gas_limit", limit.MaxGasLimit,
	)

	return next(ctx, proposal)
}

// ProcessLane verifies that the transactions included in the block proposal are valid respecting
// the verification logic of the lane (processLaneHandler). If any of the transactions are invalid,
// we return an error. If all of the transactions are valid, we return the updated proposal.
func (l *BaseLane) ProcessLane(
	ctx sdk.Context,
	proposal proposals.Proposal,
	txs []sdk.Tx,
	next block.ProcessLanesHandler,
) (proposals.Proposal, error) {
	l.Logger().Debug(
		"processing lane",
		"lane", l.Name(),
		"num_txs_to_verify", len(txs),
	)

	if len(txs) == 0 {
		return next(ctx, proposal, txs)
	}

	// Verify the transactions that belong to the lane and return any transactions that must be
	// validated by the next lane in the chain.
	txsFromLane, remainingTxs, err := l.processLaneHandler(ctx, txs)
	if err != nil {
		l.Logger().Error(
			"failed to process lane",
			"lane", l.Name(),
			"err", err,
		)

		return proposal, err
	}

	// Retrieve the transaction info for each transaction that belongs to the lane.
	txsWithInfo := make([]utils.TxWithInfo, len(txsFromLane))
	for i, tx := range txsFromLane {
		txInfo, err := l.GetTxInfo(ctx, tx)
		if err != nil {
			l.Logger().Error(
				"failed to get tx info",
				"lane", l.Name(),
				"err", err,
			)

			return proposal, err
		}

		txsWithInfo[i] = txInfo
	}

	// Optimistically update the proposal with the partial proposal.
	if err := proposal.UpdateProposal(l, txsWithInfo); err != nil {
		l.Logger().Error(
			"failed to update proposal",
			"lane", l.Name(),
			"num_txs_verified", len(txsFromLane),
			"err", err,
		)

		return proposal, err
	}

	l.Logger().Info(
		"lane processed",
		"lane", l.Name(),
		"num_txs_verified", len(txsFromLane),
		"num_txs_remaining", len(remainingTxs),
	)

	// Validate the remaining transactions with the next lane in the chain.
	return next(ctx, proposal, remainingTxs)
}

// VerifyTx verifies that the transaction is valid respecting the ante verification logic of
// of the antehandler chain.
func (l *BaseLane) VerifyTx(ctx sdk.Context, tx sdk.Tx, simulate bool) error {
	if l.cfg.AnteHandler != nil {
		// Only write to the context if the tx does not fail.
		catchCtx, write := ctx.CacheContext()
		if _, err := l.cfg.AnteHandler(catchCtx, tx, simulate); err != nil {
			return err
		}

		write()

		return nil
	}

	return nil
}
