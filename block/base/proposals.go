package base

import (
	"fmt"
	"time"

	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/x/auth/signing"

	"github.com/skip-mev/block-sdk/v2/block/proposals"
	"github.com/skip-mev/block-sdk/v2/block/utils"
)

// senderInfoGetter is implemented by PriorityNonceIterator to expose sender/nonce
// from the mempool index key without re-parsing the transaction.
type senderInfoGetter interface {
	SenderInfo() (sender string, nonce uint64)
}

// FastNonceFlushSender is a sentinel sender value passed to FastNonceVerifier
// to trigger a batch flush: the verifier writes all cached account state to the
// store and clears its internal cache. The nonce argument is ignored.
const FastNonceFlushSender = "\x00flush\x00"

// FastNonceVerifier is an optional callback for single-signer transactions.
// It verifies that `sender`'s current sequence matches `nonce` and increments
// the sequence in the cached context state on success.
//
// Return values:
//   - cmp < 0 : nonce < expected (stale tx). Caller should skip this tx but
//     continue scanning — the next nonce from this sender may match.
//   - cmp == 0: nonce == expected (match). Tx is valid; caller should include it.
//   - cmp > 0 : nonce > expected (future tx / gap). Caller should skip the
//     entire sender — all subsequent nonces are even larger.
//   - err != nil: non-nonce error (unknown account, bad address, etc.).
//     Caller should remove the tx from the mempool.
//
// When nil, PrepareLaneHandler falls back to the full VerifyTx path.
type FastNonceVerifier func(ctx sdk.Context, sender string, nonce uint64) (cmp int, err error)

// DefaultProposalHandler returns a default implementation of the PrepareLaneHandler and
// ProcessLaneHandler.
type DefaultProposalHandler struct {
	lane              *BaseLane
	fastNonceVerifier FastNonceVerifier
}

// NewDefaultProposalHandler returns a new default proposal handler.
func NewDefaultProposalHandler(lane *BaseLane) *DefaultProposalHandler {
	return &DefaultProposalHandler{
		lane: lane,
	}
}

// WithFastNonceVerifier sets an optional fast-path nonce verifier used for
// single-signer transactions, avoiding a full VerifyTx / GetSigners call.
func (h *DefaultProposalHandler) WithFastNonceVerifier(fn FastNonceVerifier) *DefaultProposalHandler {
	h.fastNonceVerifier = fn
	return h
}

// DefaultPrepareLaneHandler returns a default implementation of the PrepareLaneHandler. It
// selects all transactions in the mempool that are valid and not already in the partial
// proposal. It will continue to reap transactions until the maximum blockspace/gas for this
// lane has been reached. Additionally, any transactions that are invalid will be returned.
func (h *DefaultProposalHandler) PrepareLaneHandler() PrepareLaneHandler {
	return func(ctx sdk.Context, proposal proposals.Proposal, limit proposals.LaneLimits) ([]sdk.Tx, []utils.TxWithInfo, []sdk.Tx, error) {
		t0 := time.Now()
		var (
			totalSize      int64
			totalGas       uint64
			txsToInclude   []sdk.Tx
			txsWithInfo    []utils.TxWithInfo
			txsToRemove    []sdk.Tx
			skippedSenders = make(map[string]struct{})
		)

		// TODO(max): rewrite debugging to use LazyHash once this commit is available as part of cometbftv1 migration:
		// https://github.com/cometbft/cometbft/commit/fad150950f1b6095b89e140e1286de027fe9778f

		// Accumulated timing (microseconds) per block for lane_sim Prometheus metrics.
		// Each variable corresponds to one labelled step emitted in the defer below.
		var (
			accSelectUs            int64 // h.lane.Select
			accSenderInfoUs        int64 // extract sender/nonce from iterator key
			accTxInfoUs            int64 // GetTxInfoLight
			accSkippedSenderUs     int64 // lookup + hit-branch for skipped senders
			accGasLimitUs          int64 // single-tx gas-limit guard
			accTxSizeUs            int64 // single-tx size guard
			accLaneMatchUs         int64 // h.lane.Match
			accAlreadyInProposalUs int64 // proposal.Contains
			accUpdatedSizeUs       int64 // accumulated-size budget check (+ markSkippedSender)
			accUpdatedGasUs        int64 // accumulated-gas budget check (+ markSkippedSender)
			accVerifySigParseUs    int64 // GetSignaturesV2 / GetSigners (inside verify path)
			accVerifySingleUs      int64 // fastNonceVerifier single-signer call
			accVerifyMultiUs       int64 // fastNonceVerifier multi-signer loop
			accVerifyFullUs        int64 // h.lane.VerifyTx fallback
			accIterTxUs            int64 // iterator.Tx()
			accIncludeUs           int64 // append to txsToInclude / txsWithInfo
			accNextUs              int64 // iterator.Next
			accFlushUs             int64 // fastNonceVerifier flush sentinel
			accRemoveTxUs          int64 // append to txsToRemove
		)

		minRemainingSizeToContinue := limit.MaxTxBytes / 1000
		minRemainingGasToContinue := limit.MaxGasLimit / 1000

		removeTx := func(tx sdk.Tx) {
			tR := time.Now()
			txsToRemove = append(txsToRemove, tx)
			accRemoveTxUs += time.Since(tR).Microseconds()
		}
		markSkippedSender := func(sender string) {
			if sender == "" {
				return
			}
			skippedSenders[sender] = struct{}{}
		}

		defer func() {
			if h.fastNonceVerifier != nil {
				tFlush := time.Now()
				_, _ = h.fastNonceVerifier(ctx, FastNonceFlushSender, 0)
				accFlushUs += time.Since(tFlush).Microseconds()
			}

			if h.lane.Name() != "exchange" {
				return
			}

			totalUs := time.Since(t0).Microseconds()
			LanePrepareTotalSeconds.WithLabelValues(h.lane.Name()).Observe(float64(totalUs) / 1e6)

			obs := func(step string, us int64) {
				LaneSimSeconds.WithLabelValues(h.lane.Name(), step).Observe(float64(us) / 1e6)
			}
			obs("select", accSelectUs)
			obs("iter_tx", accIterTxUs)
			obs("sender_info", accSenderInfoUs)
			obs("tx_info", accTxInfoUs)
			obs("skipped_sender", accSkippedSenderUs)
			obs("gas_limit", accGasLimitUs)
			obs("tx_size", accTxSizeUs)
			obs("lane_match", accLaneMatchUs)
			obs("already_in_proposal", accAlreadyInProposalUs)
			obs("updated_size", accUpdatedSizeUs)
			obs("updated_gas", accUpdatedGasUs)
			obs("verify_sig_parse", accVerifySigParseUs)
			obs("verify_single", accVerifySingleUs)
			obs("verify_multi", accVerifyMultiUs)
			obs("verify_full", accVerifyFullUs)
			obs("include", accIncludeUs)
			obs("next", accNextUs)
			obs("flush", accFlushUs)
			obs("remove_tx", accRemoveTxUs)

			accountedUs := accSelectUs + accIterTxUs + accSenderInfoUs + accTxInfoUs +
				accSkippedSenderUs + accGasLimitUs + accTxSizeUs +
				accLaneMatchUs + accAlreadyInProposalUs +
				accUpdatedSizeUs + accUpdatedGasUs +
				accVerifySigParseUs + accVerifySingleUs + accVerifyMultiUs + accVerifyFullUs +
				accIncludeUs + accNextUs + accFlushUs + accRemoveTxUs
			otherUs := totalUs - accountedUs
			if otherUs < 0 {
				otherUs = 0
			}
			obs("other", otherUs)
		}()

		// Select all transactions in the mempool that are valid and not already in the
		// partial proposal.
		tSel := time.Now()
		iterator := h.lane.Select(ctx, nil)
		accSelectUs += time.Since(tSel).Microseconds()

		for ; iterator != nil; func() {
			tNext := time.Now()
			iterator = iterator.Next()
			accNextUs += time.Since(tNext).Microseconds()
		}() {
			tIterTx := time.Now()
			tx := iterator.Tx()
			accIterTxUs += time.Since(tIterTx).Microseconds()

			// ── sender_info ──────────────────────────────────────────────────────────
			// Get sender and nonce from the mempool index key (set at Insert time from signers[0]).
			tSender := time.Now()
			var senderStr string
			var senderNonce uint64
			if sig, ok := iterator.(senderInfoGetter); ok {
				senderStr, senderNonce = sig.SenderInfo()
			}
			accSenderInfoUs += time.Since(tSender).Microseconds()

			// ── tx_info ──────────────────────────────────────────────────────────────
			tInfo := time.Now()
			txInfo, err := h.lane.GetTxInfoLight(ctx, tx)
			accTxInfoUs += time.Since(tInfo).Microseconds()
			if err != nil {
				h.lane.Logger().Info("failed to get hash of tx", "err", err)
				removeTx(tx)
				continue
			}

			// ── skipped_sender ────────────────────────────────────────────────────
			// If the transaction is from a skipped sender, we skip it altogether. Allows to avoid sequence error when
			// first skipped tx is not included in the proposal.
			tSkip := time.Now()
			isSkippedSender := senderStr != "" && func() bool { _, ok := skippedSenders[senderStr]; return ok }()
			if isSkippedSender {
				h.lane.Logger().Debug(
					"failed to select tx for lane; tx from skipped sender",
					"tx_hash", txInfo.Hash,
					"lane", h.lane.Name(),
				)
				accSkippedSenderUs += time.Since(tSkip).Microseconds()
				continue
			}
			accSkippedSenderUs += time.Since(tSkip).Microseconds()

			// ── gas_limit ─────────────────────────────────────────────────────────
			// Reject single-tx gas that can never fit regardless of accumulated totals.
			tGasLimit := time.Now()
			if txInfo.GasLimit > limit.MaxGasLimit {
				h.lane.Logger().Debug(
					"failed to select tx for lane; gas limit above the maximum allowed",
					"lane", h.lane.Name(),
					"tx_gas", txInfo.GasLimit,
					"max_gas", limit.MaxGasLimit,
					"tx_hash", txInfo.Hash,
				)
				accGasLimitUs += time.Since(tGasLimit).Microseconds()
				removeTx(tx)
				continue
			}
			accGasLimitUs += time.Since(tGasLimit).Microseconds()

			// ── tx_size ───────────────────────────────────────────────────────────
			// Reject single-tx size that can never fit regardless of accumulated totals.
			tTxSize := time.Now()
			if txInfo.Size > limit.MaxTxBytes {
				h.lane.Logger().Debug(
					"failed to select tx for lane; tx bytes above the maximum allowed",
					"lane", h.lane.Name(),
					"tx_size", txInfo.Size,
					"max_tx_bytes", limit.MaxTxBytes,
					"tx_hash", txInfo.Hash,
				)
				accTxSizeUs += time.Since(tTxSize).Microseconds()
				removeTx(tx)
				continue
			}
			accTxSizeUs += time.Since(tTxSize).Microseconds()

			// ── lane_match ────────────────────────────────────────────────────────
			// Double check that the transaction belongs to this lane.
			tMatch := time.Now()
			if !h.lane.Match(ctx, tx) {
				h.lane.Logger().Debug(
					"failed to select tx for lane; tx does not belong to lane",
					"tx_hash", txInfo.Hash,
					"lane", h.lane.Name(),
				)
				accLaneMatchUs += time.Since(tMatch).Microseconds()
				removeTx(tx)
				continue
			}
			accLaneMatchUs += time.Since(tMatch).Microseconds()

			// ── already_in_proposal ───────────────────────────────────────────────
			// If the transaction is already in the (partial) block proposal, we skip it.
			tDup := time.Now()
			if proposal.Contains(txInfo.Hash) {
				h.lane.Logger().Debug(
					"failed to select tx for lane; tx is already in proposal",
					"tx_hash", txInfo.Hash,
					"lane", h.lane.Name(),
				)
				accAlreadyInProposalUs += time.Since(tDup).Microseconds()
				continue
			}
			accAlreadyInProposalUs += time.Since(tDup).Microseconds()

			// ── updated_size ──────────────────────────────────────────────────────
			// If the transaction is too large, we skip it,
			// but also have to prevent anything from the same sender from being included during this iteration.
			tUpdSize := time.Now()
			updatedSize := totalSize + txInfo.Size
			if updatedSize > limit.MaxTxBytes {
				h.lane.Logger().Debug(
					"failed to select tx for lane; tx bytes above the maximum allowed",
					"lane", h.lane.Name(),
					"tx_size", txInfo.Size,
					"total_size", totalSize,
					"max_tx_bytes", limit.MaxTxBytes,
					"tx_hash", txInfo.Hash,
				)
				// Early-break: if remaining byte budget is less than 1/1000 of the limit,
				// further scanning is unlikely to find a fitting tx — stop early.
				remainingSize := limit.MaxTxBytes - totalSize
				if remainingSize < minRemainingSizeToContinue {
					h.lane.Logger().Debug(
						"stopping lane selection; remaining byte budget below continuation threshold",
						"lane", h.lane.Name(),
						"remaining_bytes", remainingSize,
						"continue_threshold_bytes", minRemainingSizeToContinue,
						"max_tx_bytes", limit.MaxTxBytes,
					)
					accUpdatedSizeUs += time.Since(tUpdSize).Microseconds()
					break
				}
				markSkippedSender(senderStr)
				accUpdatedSizeUs += time.Since(tUpdSize).Microseconds()
				continue
			}
			accUpdatedSizeUs += time.Since(tUpdSize).Microseconds()

			// ── updated_gas ───────────────────────────────────────────────────────
			// If the gas limit of the transaction is too large, we skip it,
			// but also have to prevent anything from the same sender from being included during this iteration.
			tUpdGas := time.Now()
			updatedGas := totalGas + txInfo.GasLimit
			if updatedGas > limit.MaxGasLimit {
				h.lane.Logger().Debug(
					"failed to select tx for lane; gas limit above the maximum allowed",
					"lane", h.lane.Name(),
					"tx_gas", txInfo.GasLimit,
					"total_gas", totalGas,
					"max_gas", limit.MaxGasLimit,
					"tx_hash", txInfo.Hash,
				)
				// Early-break: if remaining gas budget is less than 1/1000 of the limit,
				// further scanning is unlikely to find a fitting tx — stop early.
				remainingGas := limit.MaxGasLimit - totalGas
				if remainingGas < minRemainingGasToContinue {
					h.lane.Logger().Debug(
						"stopping lane selection; remaining gas budget below continuation threshold",
						"lane", h.lane.Name(),
						"remaining_gas", remainingGas,
						"continue_threshold_gas", minRemainingGasToContinue,
						"max_gas", limit.MaxGasLimit,
					)
					accUpdatedGasUs += time.Since(tUpdGas).Microseconds()
					break
				}
				markSkippedSender(senderStr)
				accUpdatedGasUs += time.Since(tUpdGas).Microseconds()
				continue
			}
			accUpdatedGasUs += time.Since(tUpdGas).Microseconds()

			// ── verify ────────────────────────────────────────────────────────────
			// Verify nonce(s). When FastNonceVerifier is set, parse signature count
			// to choose the optimal path and avoid a full VerifyTx / GetSigners call.
			if h.fastNonceVerifier != nil {
				// verify_sig_parse: cast + GetSignaturesV2
				tSigParse := time.Now()
				sigTx, ok := tx.(signing.SigVerifiableTx)
				if !ok {
					accVerifySigParseUs += time.Since(tSigParse).Microseconds()
					h.lane.Logger().Info("tx does not implement SigVerifiableTx", "tx_hash", txInfo.Hash)
					removeTx(tx)
					continue
				}
				sigs, sigErr := sigTx.GetSignaturesV2()
				accVerifySigParseUs += time.Since(tSigParse).Microseconds()
				if sigErr != nil {
					h.lane.Logger().Info("failed to get signatures", "tx_hash", txInfo.Hash, "err", sigErr)
					removeTx(tx)
					continue
				}
				if len(sigs) == 0 {
					h.lane.Logger().Info("tx has no signatures", "tx_hash", txInfo.Hash)
					removeTx(tx)
					continue
				}

				if len(sigs) == 1 {
					// verify_single: fastNonceVerifier for single-signer tx.
					// senderIndex is iterated in ascending nonce order, so:
					//   cmp < 0 (stale):  nonce < expected → remove (will never be valid again).
					//   cmp > 0 (future): nonce > expected → skip entire sender.
					//   err != nil:        non-nonce error → remove.
					tSingle := time.Now()
					cmp, verifyErr := h.fastNonceVerifier(ctx, senderStr, senderNonce)
					accVerifySingleUs += time.Since(tSingle).Microseconds()
					if verifyErr != nil {
						h.lane.Logger().Info(
							"failed fast nonce verify (single signer), removing tx",
							"tx_hash", txInfo.Hash,
							"sender", senderStr,
							"err", verifyErr,
						)
						removeTx(tx)
						continue
					}
					if cmp > 0 {
						// future tx: gap — all subsequent nonces from this sender are larger.
						markSkippedSender(senderStr)
						continue
					}
					if cmp < 0 {
						// stale tx: already committed on-chain.
						h.lane.Logger().Debug(
							"removing stale tx from mempool",
							"tx_hash", txInfo.Hash,
							"sender", senderStr,
							"nonce", senderNonce,
						)
						removeTx(tx)
						continue
					}
				} else {
					// verify_multi: multiple independent signers.
					// GetSigners() is needed here; the overhead is acceptable for multi-sig txs.
					tSigParse2 := time.Now()
					signers, signersErr := sigTx.GetSigners()
					accVerifySigParseUs += time.Since(tSigParse2).Microseconds()
					if signersErr != nil {
						h.lane.Logger().Info("failed to get signers", "tx_hash", txInfo.Hash, "err", signersErr)
						removeTx(tx)
						continue
					}
					if len(signers) != len(sigs) {
						h.lane.Logger().Info("signer/signature count mismatch", "tx_hash", txInfo.Hash)
						removeTx(tx)
						continue
					}
					tMulti := time.Now()
					var multiErr error
					for i, addrBytes := range signers {
						addrStr := sdk.AccAddress(addrBytes).String()
						cmp, verifyErr := h.fastNonceVerifier(ctx, addrStr, sigs[i].Sequence)
						if verifyErr != nil {
							multiErr = verifyErr
						} else if cmp != 0 {
							multiErr = fmt.Errorf("nonce mismatch for signer %s", addrStr)
						}
						if multiErr != nil {
							h.lane.Logger().Info(
								"failed fast nonce verify (multi signer)",
								"tx_hash", txInfo.Hash,
								"signer", addrStr,
								"err", multiErr,
							)
							break
						}
					}
					accVerifyMultiUs += time.Since(tMulti).Microseconds()
					if multiErr != nil {
						removeTx(tx)
						continue
					}
				}
			} else {
				// verify_full: no FastNonceVerifier — fall back to full ante handler.
				tFull := time.Now()
				if err = h.lane.VerifyTx(ctx, tx, false); err != nil {
					accVerifyFullUs += time.Since(tFull).Microseconds()
					h.lane.Logger().Info(
						"failed to verify tx",
						"tx_hash", txInfo.Hash,
						"err", err,
					)
					removeTx(tx)
					continue
				}
				accVerifyFullUs += time.Since(tFull).Microseconds()
			}

			// ── include ───────────────────────────────────────────────────────────
			tInclude := time.Now()
			totalSize += txInfo.Size
			totalGas += txInfo.GasLimit
			txsToInclude = append(txsToInclude, tx)
			// Carry the already-computed TxWithInfo so the caller avoids a second GetTxInfo call.
			txsWithInfo = append(txsWithInfo, txInfo)
			accIncludeUs += time.Since(tInclude).Microseconds()
		}

		return txsToInclude, txsWithInfo, txsToRemove, nil
	}
}

// DefaultProcessLaneHandler returns a default implementation of the ProcessLaneHandler. It verifies
// the following invariants:
//  1. Transactions belonging to the lane must be contiguous from the beginning of the partial proposal.
//  2. Transactions that do not belong to the lane must be contiguous from the end of the partial proposal.
//  3. Transactions must be ordered respecting the priority defined by the lane (e.g. gas price).
//  4. Transactions must be valid according to the verification logic of the lane.
func (h *DefaultProposalHandler) ProcessLaneHandler() ProcessLaneHandler {
	return func(ctx sdk.Context, partialProposal []sdk.Tx) ([]sdk.Tx, []sdk.Tx, error) {
		if len(partialProposal) == 0 {
			return nil, nil, nil
		}

		for index, tx := range partialProposal {
			if !h.lane.Match(ctx, tx) {
				// If the transaction does not belong to this lane, we return the remaining transactions
				// iff there are no matches in the remaining transactions after this index.
				if index+1 < len(partialProposal) {
					if err := h.lane.VerifyNoMatches(ctx, partialProposal[index+1:]); err != nil {
						return nil, nil, fmt.Errorf("failed to verify no matches: %w", err)
					}
				}

				return partialProposal[:index], partialProposal[index:], nil
			}

			// If the transactions do not respect the priority defined by the mempool, we consider the proposal
			// to be invalid
			if index > 0 {
				if v, err := h.lane.Compare(ctx, partialProposal[index-1], tx); v == -1 || err != nil {
					return nil, nil, fmt.Errorf("transaction at index %d has a higher priority than %d", index, index-1)
				}
			}

			if err := h.lane.VerifyTx(ctx, tx, false); err != nil {
				return nil, nil, fmt.Errorf("failed to verify tx: %w", err)
			}
		}

		// This means we have processed all transactions in the partial proposal i.e.
		// all of the transactions belong to this lane. There are no remaining transactions.
		return partialProposal, nil, nil
	}
}
