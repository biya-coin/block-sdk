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
		var (
			accSelectUs           int64
			accTxUs               int64
			accSenderInfoUs       int64
			accSkippedSenderUs    int64
			accTxInfoUs           int64
			accLimitsUs           int64
			accMatchUs            int64
			accProposalContainsUs int64
			accVerifyUs           int64
			accIncludeUs          int64
			accNextUs             int64
			accLoggingUs          int64
			accFlushUs            int64
			iterCount             int64
		)

		minRemainingSizeToContinue := limit.MaxTxBytes / 1000
		minRemainingGasToContinue := limit.MaxGasLimit / 1000

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
			accountedUs := accSelectUs + accTxUs + accSenderInfoUs + accSkippedSenderUs + accTxInfoUs + accLimitsUs + accMatchUs + accProposalContainsUs + accVerifyUs + accIncludeUs + accNextUs + accLoggingUs + accFlushUs
			overheadUs := totalUs - accountedUs
			if overheadUs < 0 {
				overheadUs = 0
			}
			otherUs := totalUs - accountedUs - overheadUs
			if otherUs < 0 {
				otherUs = 0
			}

			LaneSimSeconds.WithLabelValues(h.lane.Name(), "select").Observe(float64(accSelectUs) / 1e6)
			LaneSimSeconds.WithLabelValues(h.lane.Name(), "tx").Observe(float64(accTxUs) / 1e6)
			LaneSimSeconds.WithLabelValues(h.lane.Name(), "sender_info").Observe(float64(accSenderInfoUs) / 1e6)
			LaneSimSeconds.WithLabelValues(h.lane.Name(), "skipped_sender").Observe(float64(accSkippedSenderUs) / 1e6)
			LaneSimSeconds.WithLabelValues(h.lane.Name(), "tx_info").Observe(float64(accTxInfoUs) / 1e6)
			LaneSimSeconds.WithLabelValues(h.lane.Name(), "limits").Observe(float64(accLimitsUs) / 1e6)
			LaneSimSeconds.WithLabelValues(h.lane.Name(), "match").Observe(float64(accMatchUs) / 1e6)
			LaneSimSeconds.WithLabelValues(h.lane.Name(), "proposal_contains").Observe(float64(accProposalContainsUs) / 1e6)
			LaneSimSeconds.WithLabelValues(h.lane.Name(), "verify").Observe(float64(accVerifyUs) / 1e6)
			LaneSimSeconds.WithLabelValues(h.lane.Name(), "include").Observe(float64(accIncludeUs) / 1e6)
			LaneSimSeconds.WithLabelValues(h.lane.Name(), "next").Observe(float64(accNextUs) / 1e6)
			LaneSimSeconds.WithLabelValues(h.lane.Name(), "logging").Observe(float64(accLoggingUs) / 1e6)
			LaneSimSeconds.WithLabelValues(h.lane.Name(), "flush").Observe(float64(accFlushUs) / 1e6)
			LaneSimSeconds.WithLabelValues(h.lane.Name(), "overhead").Observe(float64(overheadUs) / 1e6)
			LaneSimSeconds.WithLabelValues(h.lane.Name(), "other").Observe(float64(otherUs) / 1e6)
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
			tTx := time.Now()
			tx := iterator.Tx()
			accTxUs += time.Since(tTx).Microseconds()

			// Get sender and nonce from the mempool index key (set at Insert time from signers[0]).
			tSenderInfo := time.Now()
			var senderStr string
			var senderNonce uint64
			if sig, ok := iterator.(senderInfoGetter); ok {
				senderStr, senderNonce = sig.SenderInfo()
			}
			accSenderInfoUs += time.Since(tSenderInfo).Microseconds()

			iterCount++
			tInfo := time.Now()
			txInfo, err := h.lane.GetTxInfoLight(ctx, tx)
			accTxInfoUs += time.Since(tInfo).Microseconds()

			if err != nil {
				tLog := time.Now()
				h.lane.Logger().Info("failed to get hash of tx", "err", err)
				accLoggingUs += time.Since(tLog).Microseconds()

				txsToRemove = append(txsToRemove, tx)
				continue
			}

			// If the transaction is from a skipped sender, we skip it altogether. Allows to avoid sequence error when
			// first skipped tx is not included in the proposal.
			tSkippedSender := time.Now()
			isSkippedSender := false
			if senderStr != "" {
				_, isSkippedSender = skippedSenders[senderStr]
			}
			accSkippedSenderUs += time.Since(tSkippedSender).Microseconds()
			if isSkippedSender {
				tLog := time.Now()
				h.lane.Logger().Debug(
					"failed to select tx for lane; tx from skipped sender",
					"tx_hash", txInfo.Hash,
					"lane", h.lane.Name(),
				)
				accLoggingUs += time.Since(tLog).Microseconds()

				continue
			}

			tLimits := time.Now()
			gasLimitExceeded := txInfo.GasLimit > limit.MaxGasLimit
			accLimitsUs += time.Since(tLimits).Microseconds()
			if gasLimitExceeded {
				tLog := time.Now()
				h.lane.Logger().Debug(
					"failed to select tx for lane; gas limit above the maximum allowed",
					"lane", h.lane.Name(),
					"tx_gas", txInfo.GasLimit,
					"max_gas", limit.MaxGasLimit,
					"tx_hash", txInfo.Hash,
				)
				accLoggingUs += time.Since(tLog).Microseconds()

				txsToRemove = append(txsToRemove, tx)
				continue
			}

			tLimits = time.Now()
			txSizeExceeded := txInfo.Size > limit.MaxTxBytes
			accLimitsUs += time.Since(tLimits).Microseconds()
			if txSizeExceeded {
				tLog := time.Now()
				h.lane.Logger().Debug(
					"failed to select tx for lane; tx bytes above the maximum allowed",
					"lane", h.lane.Name(),
					"tx_size", txInfo.Size,
					"max_tx_bytes", limit.MaxTxBytes,
					"tx_hash", txInfo.Hash,
				)
				accLoggingUs += time.Since(tLog).Microseconds()

				txsToRemove = append(txsToRemove, tx)
				continue
			}

			// Double check that the transaction belongs to this lane.
			tMatch := time.Now()
			matchesLane := h.lane.Match(ctx, tx)
			accMatchUs += time.Since(tMatch).Microseconds()
			if !matchesLane {
				tLog := time.Now()
				h.lane.Logger().Debug(
					"failed to select tx for lane; tx does not belong to lane",
					"tx_hash", txInfo.Hash,
					"lane", h.lane.Name(),
				)
				accLoggingUs += time.Since(tLog).Microseconds()

				txsToRemove = append(txsToRemove, tx)
				continue
			}

			// if the transaction is already in the (partial) block proposal, we skip it.
			tProposalContains := time.Now()
			alreadyInProposal := proposal.Contains(txInfo.Hash)
			accProposalContainsUs += time.Since(tProposalContains).Microseconds()
			if alreadyInProposal {
				tLog := time.Now()
				h.lane.Logger().Debug(
					"failed to select tx for lane; tx is already in proposal",
					"tx_hash", txInfo.Hash,
					"lane", h.lane.Name(),
				)
				accLoggingUs += time.Since(tLog).Microseconds()

				continue
			}

			// If the transaction is too large, we skip it,
			// but also have to prevent anything from the same sender from being included during this iteration.
			tLimits = time.Now()
			updatedSize := totalSize + txInfo.Size
			updatedSizeExceeded := updatedSize > limit.MaxTxBytes
			accLimitsUs += time.Since(tLimits).Microseconds()
			if updatedSizeExceeded {
				tLog := time.Now()
				h.lane.Logger().Debug(
					"failed to select tx for lane; tx bytes above the maximum allowed",
					"lane", h.lane.Name(),
					"tx_size", txInfo.Size,
					"total_size", totalSize,
					"max_tx_bytes", limit.MaxTxBytes,
					"tx_hash", txInfo.Hash,
				)
				accLoggingUs += time.Since(tLog).Microseconds()

				// Early-break: if remaining byte budget is less than 1/1000 of the limit,
				// further scanning is unlikely to find a fitting tx — stop early.
				remainingSize := limit.MaxTxBytes - totalSize
				if remainingSize < minRemainingSizeToContinue {
					tLog = time.Now()
					h.lane.Logger().Debug(
						"stopping lane selection; remaining byte budget below continuation threshold",
						"lane", h.lane.Name(),
						"remaining_bytes", remainingSize,
						"continue_threshold_bytes", minRemainingSizeToContinue,
						"max_tx_bytes", limit.MaxTxBytes,
					)
					accLoggingUs += time.Since(tLog).Microseconds()
					break
				}

				// using bech32 sender string as map key directly
				if senderStr != "" {
					skippedSenders[senderStr] = struct{}{}
				}

				continue
			}

			// If the gas limit of the transaction is too large, we skip it,
			// but also have to prevent anything from the same sender from being included during this iteration.
			tLimits = time.Now()
			updatedGas := totalGas + txInfo.GasLimit
			updatedGasExceeded := updatedGas > limit.MaxGasLimit
			accLimitsUs += time.Since(tLimits).Microseconds()
			if updatedGasExceeded {
				tLog := time.Now()
				h.lane.Logger().Debug(
					"failed to select tx for lane; gas limit above the maximum allowed",
					"lane", h.lane.Name(),
					"tx_gas", txInfo.GasLimit,
					"total_gas", totalGas,
					"max_gas", limit.MaxGasLimit,
					"tx_hash", txInfo.Hash,
				)
				accLoggingUs += time.Since(tLog).Microseconds()

				// Early-break: if remaining gas budget is less than 1/1000 of the limit,
				// further scanning is unlikely to find a fitting tx — stop early.
				remainingGas := limit.MaxGasLimit - totalGas
				if remainingGas < minRemainingGasToContinue {
					tLog = time.Now()
					h.lane.Logger().Debug(
						"stopping lane selection; remaining gas budget below continuation threshold",
						"lane", h.lane.Name(),
						"remaining_gas", remainingGas,
						"continue_threshold_gas", minRemainingGasToContinue,
						"max_gas", limit.MaxGasLimit,
					)
					accLoggingUs += time.Since(tLog).Microseconds()
					break
				}

				// using bech32 sender string as map key directly
				if senderStr != "" {
					skippedSenders[senderStr] = struct{}{}
				}

				continue
			}

			// Verify nonce(s). When FastNonceVerifier is set, parse signature count
			// to choose the optimal path and avoid a full VerifyTx / GetSigners call.
			tVerify := time.Now()
			if h.fastNonceVerifier != nil {
				sigTx, ok := tx.(signing.SigVerifiableTx)
				if !ok {
					accVerifyUs += time.Since(tVerify).Microseconds()
					h.lane.Logger().Info("tx does not implement SigVerifiableTx", "tx_hash", txInfo.Hash)
					txsToRemove = append(txsToRemove, tx)
					continue
				}
				sigs, sigErr := sigTx.GetSignaturesV2()
				if sigErr != nil {
					accVerifyUs += time.Since(tVerify).Microseconds()
					h.lane.Logger().Info("failed to get signatures", "tx_hash", txInfo.Hash, "err", sigErr)
					txsToRemove = append(txsToRemove, tx)
					continue
				}

				switch len(sigs) {
				case 0:
					// No signatures at all — invalid tx, remove.
					accVerifyUs += time.Since(tVerify).Microseconds()
					h.lane.Logger().Info("tx has no signatures", "tx_hash", txInfo.Hash)
					txsToRemove = append(txsToRemove, tx)
					continue

				case 1:
					// Single signer: use the sender/nonce already extracted from the iterator key.
					// senderIndex is iterated in ascending nonce order, so:
					//   cmp < 0 (stale):  nonce < expected → just skip this tx, next nonce may match.
					//   cmp > 0 (future): nonce > expected → all subsequent nonces are larger too,
					//                      skip the entire sender to avoid scanning them all.
					//   err != nil:        non-nonce error → remove the tx.
					cmp, verifyErr := h.fastNonceVerifier(ctx, senderStr, senderNonce)
					if verifyErr != nil {
						accVerifyUs += time.Since(tVerify).Microseconds()
						h.lane.Logger().Info(
							"failed fast nonce verify (single signer), removing tx",
							"tx_hash", txInfo.Hash,
							"sender", senderStr,
							"err", verifyErr,
						)
						txsToRemove = append(txsToRemove, tx)
						continue
					}
					if cmp != 0 {
						accVerifyUs += time.Since(tVerify).Microseconds()
						if cmp > 0 {
							// future tx: nonce > expected, gap exists. Since senderIndex is
							// ascending, all subsequent txs from this sender have even higher
							// nonces — skip the entire sender for this block.
							if senderStr != "" {
								skippedSenders[senderStr] = struct{}{}
							}
						} else {
							// stale tx: nonce < expected, already committed on-chain.
							// Will never be valid again — remove from mempool.
							h.lane.Logger().Debug(
								"removing stale tx from mempool",
								"tx_hash", txInfo.Hash,
								"sender", senderStr,
								"nonce", senderNonce,
							)
							txsToRemove = append(txsToRemove, tx)
						}
						continue
					}

				default:
					// Multiple independent signers: pair GetSigners() with GetSignaturesV2().
					// This is rare; the GetSigners() call overhead here is acceptable.
					signers, signersErr := sigTx.GetSigners()
					if signersErr != nil {
						accVerifyUs += time.Since(tVerify).Microseconds()
						h.lane.Logger().Info("failed to get signers", "tx_hash", txInfo.Hash, "err", signersErr)
						txsToRemove = append(txsToRemove, tx)
						continue
					}
					if len(signers) != len(sigs) {
						accVerifyUs += time.Since(tVerify).Microseconds()
						h.lane.Logger().Info("signer/signature count mismatch", "tx_hash", txInfo.Hash)
						txsToRemove = append(txsToRemove, tx)
						continue
					}
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
					if multiErr != nil {
						accVerifyUs += time.Since(tVerify).Microseconds()
						txsToRemove = append(txsToRemove, tx)
						continue
					}
				}
			} else {
				// No FastNonceVerifier configured: fall back to full ante handler.
				if err = h.lane.VerifyTx(ctx, tx, false); err != nil {
					accVerifyUs += time.Since(tVerify).Microseconds()
					h.lane.Logger().Info(
						"failed to verify tx",
						"tx_hash", txInfo.Hash,
						"err", err,
					)
					txsToRemove = append(txsToRemove, tx)
					continue
				}
			}
			accVerifyUs += time.Since(tVerify).Microseconds()

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
