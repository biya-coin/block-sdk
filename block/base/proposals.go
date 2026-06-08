package base

import (
	"fmt"
	"runtime"
	"sync"
	"time"

	sdk "github.com/cosmos/cosmos-sdk/types"
	txtypes "github.com/cosmos/cosmos-sdk/types/tx"
	"github.com/cosmos/cosmos-sdk/x/auth/signing"

	"github.com/skip-mev/block-sdk/v2/block/proposals"
	"github.com/skip-mev/block-sdk/v2/block/utils"
)

// senderInfoGetter is implemented by PriorityNonceIterator to expose sender/nonce
// from the mempool index key without re-parsing the transaction.
type senderInfoGetter interface {
	SenderInfo() (sender string, nonce uint64)
}

type protoTxGetter interface {
	GetProtoTx() *txtypes.Tx
}

type exchangeCandidate struct {
	tx        sdk.Tx
	txInfo    utils.TxWithInfo
	sender    string
	nonce     uint64
	hasTxInfo bool
}

// FastNonceFlushSender is a sentinel sender value passed to FastNonceVerifier
// to clear the verifier's internal cache. The nonce argument is ignored.
const FastNonceFlushSender = "\x00flush\x00"

const tx_num = 10000

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

func signatureCount(tx sdk.Tx) (int, error) {
	if protoTx, ok := tx.(protoTxGetter); ok {
		tx := protoTx.GetProtoTx()
		if tx == nil || tx.AuthInfo == nil {
			return 0, fmt.Errorf("tx missing auth info")
		}
		if len(tx.Signatures) < len(tx.AuthInfo.SignerInfos) {
			return 0, fmt.Errorf("signature count %d below signer info count %d", len(tx.Signatures), len(tx.AuthInfo.SignerInfos))
		}
		return len(tx.AuthInfo.SignerInfos), nil
	}

	return 0, fmt.Errorf("tx does not expose proto tx")
}

func (h *DefaultProposalHandler) fillExchangeCandidateTxInfo(
	ctx sdk.Context,
	candidates []exchangeCandidate,
) []sdk.Tx {
	type txInfoResult struct {
		index    int
		info     utils.TxWithInfo
		matches  bool
		sigCount int
		err      error
	}

	jobs := make(chan int)
	results := make(chan txInfoResult, len(candidates))
	workers := runtime.GOMAXPROCS(0)
	if workers > len(candidates) {
		workers = len(candidates)
	}

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				txInfo, err := h.lane.GetTxInfoLight(ctx, candidates[index].tx)
				matches := false
				sigCount := 0
				if err == nil {
					matches = h.lane.Match(ctx, candidates[index].tx)
				}
				if err == nil && matches {
					sigCount, err = signatureCount(candidates[index].tx)
				}
				results <- txInfoResult{
					index:    index,
					info:     txInfo,
					matches:  matches,
					sigCount: sigCount,
					err:      err,
				}
			}
		}()
	}

	for i := range candidates {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	close(results)

	var txsToRemove []sdk.Tx
	for result := range results {
		if result.err != nil {
			h.lane.Logger().Info("failed to get candidate tx info", "err", result.err)
			txsToRemove = append(txsToRemove, candidates[result.index].tx)
			continue
		}
		result.info.Sender = candidates[result.index].sender
		result.info.Nonce = candidates[result.index].nonce
		if !result.matches {
			h.lane.Logger().Debug(
				"failed to select tx for lane; tx does not belong to lane",
				"tx_hash", utils.TxHash(result.info.TxBytes),
				"lane", h.lane.Name(),
			)
			txsToRemove = append(txsToRemove, candidates[result.index].tx)
			continue
		}
		if result.sigCount == 0 {
			h.lane.Logger().Info("tx has no signatures", "tx_hash", utils.TxHash(result.info.TxBytes))
			txsToRemove = append(txsToRemove, candidates[result.index].tx)
			continue
		}
		if result.sigCount > 1 {
			h.lane.Logger().Error(
				"exchange lane does not support multisig transactions",
				"tx_hash", utils.TxHash(result.info.TxBytes),
				"signature_count", result.sigCount,
			)
			txsToRemove = append(txsToRemove, candidates[result.index].tx)
			continue
		}
		candidates[result.index].txInfo = result.info
		candidates[result.index].hasTxInfo = true
	}

	return txsToRemove
}

func (h *DefaultProposalHandler) exchangePrepareLaneHandler() PrepareLaneHandler {
	return func(ctx sdk.Context, proposal proposals.Proposal, limit proposals.LaneLimits) ([]sdk.Tx, []utils.TxWithInfo, []sdk.Tx, error) {
		if h.fastNonceVerifier == nil {
			return nil, nil, nil, fmt.Errorf("exchange lane requires FastNonceVerifier")
		}

		t0 := time.Now()
		var (
			txsToInclude   []sdk.Tx
			txsWithInfo    []utils.TxWithInfo
			txsToRemove    []sdk.Tx
			skippedSenders = make(map[string]struct{})
		)

		// TODO(max): rewrite debugging to use LazyHash once this commit is available as part of cometbftv1 migration:
		// https://github.com/cometbft/cometbft/commit/fad150950f1b6095b89e140e1286de027fe9778f

		// Accumulated timing (nanoseconds) per block for lane_sim Prometheus metrics.
		// Each variable corresponds to one labelled step emitted in the defer below.
		var (
			accSenderInfoNs    int64 // extract sender/nonce from iterator key
			accTxInfoNs        int64 // GetTxInfoLight + lane match + signature count
			accSkippedSenderNs int64 // lookup + hit-branch for skipped senders
			accVerifySingleNs  int64 // fastNonceVerifier single-signer call
			accIterTxNs        int64 // iterator.Tx()
			accIncludeNs       int64 // append to txsToInclude / txsWithInfo
			accNextNs          int64 // iterator.Next
			accFlushNs         int64 // fastNonceVerifier flush sentinel
		)

		_ = limit

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
				accFlushNs += time.Since(tFlush).Nanoseconds()
			}

			if h.lane.Name() != "exchange" {
				return
			}

			totalNs := time.Since(t0).Nanoseconds()
			LanePrepareTotalSeconds.WithLabelValues(h.lane.Name()).Observe(float64(totalNs) / 1e9)

			obs := func(step string, ns int64) {
				LaneSimSeconds.WithLabelValues(h.lane.Name(), step).Observe(float64(ns) / 1e9)
			}
			obs("iter_tx", accIterTxNs)
			obs("sender_info", accSenderInfoNs)
			obs("tx_info", accTxInfoNs)
			obs("skipped_sender", accSkippedSenderNs)
			obs("verify_single", accVerifySingleNs)
			obs("include", accIncludeNs)
			obs("next", accNextNs)
			obs("flush", accFlushNs)

			accountedNs := accIterTxNs + accSenderInfoNs + accTxInfoNs +
				accSkippedSenderNs +
				accVerifySingleNs +
				accIncludeNs + accNextNs + accFlushNs
			otherNs := totalNs - accountedNs
			if otherNs < 0 {
				otherNs = 0
			}
			obs("other", otherNs)
		}()

		// Select all transactions in the mempool that are valid and not already in the
		// partial proposal.
		iterator := h.lane.Select(ctx, nil)

		for iterator != nil && len(txsToInclude) < tx_num {
			remainingTxs := tx_num - len(txsToInclude)
			candidates := make([]exchangeCandidate, 0, remainingTxs)

			for i := 0; i < remainingTxs && iterator != nil; i++ {
				tIterTx := time.Now()
				tx := iterator.Tx()
				accIterTxNs += time.Since(tIterTx).Nanoseconds()

				// ── sender_info ──────────────────────────────────────────────────────────
				// Get sender and nonce from the mempool index key (set at Insert time from signers[0]).
				tSender := time.Now()
				var senderStr string
				var senderNonce uint64
				if sig, ok := iterator.(senderInfoGetter); ok {
					senderStr, senderNonce = sig.SenderInfo()
				}
				accSenderInfoNs += time.Since(tSender).Nanoseconds()

				candidates = append(candidates, exchangeCandidate{
					tx:     tx,
					sender: senderStr,
					nonce:  senderNonce,
				})

				tNext := time.Now()
				iterator = iterator.Next()
				accNextNs += time.Since(tNext).Nanoseconds()
			}

			tTxInfo := time.Now()
			txInfoFailures := h.fillExchangeCandidateTxInfo(ctx, candidates)
			accTxInfoNs += time.Since(tTxInfo).Nanoseconds()
			txsToRemove = append(txsToRemove, txInfoFailures...)

			for _, candidate := range candidates {
				if !candidate.hasTxInfo {
					continue
				}

				tx := candidate.tx
				txInfo := candidate.txInfo
				senderStr := candidate.sender
				senderNonce := candidate.nonce

				// ── skipped_sender ────────────────────────────────────────────────────
				// If the transaction is from a skipped sender, we skip it altogether. Allows to avoid sequence error when
				// first skipped tx is not included in the proposal.
				tSkip := time.Now()
				isSkippedSender := senderStr != "" && func() bool { _, ok := skippedSenders[senderStr]; return ok }()
				if isSkippedSender {
					h.lane.Logger().Debug(
						"failed to select tx for lane; tx from skipped sender",
						"tx_hash", utils.TxHash(txInfo.TxBytes),
						"lane", h.lane.Name(),
					)
					accSkippedSenderNs += time.Since(tSkip).Nanoseconds()
					continue
				}
				accSkippedSenderNs += time.Since(tSkip).Nanoseconds()

				// ── already_in_proposal ───────────────────────────────────────────────
				// If the transaction is already in the (partial) block proposal, we skip it.
				if proposal.Contains(txInfo.Key()) {
					h.lane.Logger().Debug(
						"failed to select tx for lane; tx is already in proposal",
						"tx_hash", utils.TxHash(txInfo.TxBytes),
						"lane", h.lane.Name(),
					)
					continue
				}

				// verify_single: fastNonceVerifier for single-signer tx.
				// senderIndex is iterated in ascending nonce order, so:
				//   cmp < 0 (stale):  nonce < expected → remove (will never be valid again).
				//   cmp > 0 (future): nonce > expected → skip entire sender.
				//   err != nil:        non-nonce error → remove.
				tSingle := time.Now()
				cmp, verifyErr := h.fastNonceVerifier(ctx, senderStr, senderNonce)
				accVerifySingleNs += time.Since(tSingle).Nanoseconds()
				if verifyErr != nil {
					h.lane.Logger().Info(
						"failed fast nonce verify (single signer), removing tx",
						"tx_hash", utils.TxHash(txInfo.TxBytes),
						"sender", senderStr,
						"err", verifyErr,
					)
					txsToRemove = append(txsToRemove, tx)
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
						"tx_hash", utils.TxHash(txInfo.TxBytes),
						"sender", senderStr,
						"nonce", senderNonce,
					)
					txsToRemove = append(txsToRemove, tx)
					continue
				}

				// ── include ───────────────────────────────────────────────────────────
				tInclude := time.Now()
				txsToInclude = append(txsToInclude, tx)
				// Carry the already-computed TxWithInfo so the caller avoids a second GetTxInfo call.
				txsWithInfo = append(txsWithInfo, txInfo)
				accIncludeNs += time.Since(tInclude).Nanoseconds()
			}
		}

		return txsToInclude, txsWithInfo, txsToRemove, nil
	}
}

// DefaultPrepareLaneHandler returns a default implementation of the PrepareLaneHandler. It
// selects all transactions in the mempool that are valid and not already in the partial
// proposal. It will continue to reap transactions until the maximum blockspace/gas for this
// lane has been reached. Additionally, any transactions that are invalid will be returned.
func (h *DefaultProposalHandler) PrepareLaneHandler() PrepareLaneHandler {
	return func(ctx sdk.Context, proposal proposals.Proposal, limit proposals.LaneLimits) ([]sdk.Tx, []utils.TxWithInfo, []sdk.Tx, error) {
		if h.lane.Name() == "exchange" {
			return h.exchangePrepareLaneHandler()(ctx, proposal, limit)
		}

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

		// Accumulated timing (nanoseconds) per block for lane_sim Prometheus metrics.
		// Each variable corresponds to one labelled step emitted in the defer below.
		var (
			accSenderInfoNs     int64 // extract sender/nonce from iterator key
			accTxInfoNs         int64 // GetTxInfoLight
			accSkippedSenderNs  int64 // lookup + hit-branch for skipped senders
			accLaneMatchNs      int64 // h.lane.Match
			accVerifySigParseNs int64 // signature count / GetSignaturesV2 for multi-sig fallback
			accVerifySingleNs   int64 // fastNonceVerifier single-signer call
			accIterTxNs         int64 // iterator.Tx()
			accIncludeNs        int64 // append to txsToInclude / txsWithInfo
			accNextNs           int64 // iterator.Next
			accFlushNs          int64 // fastNonceVerifier flush sentinel
		)

		minRemainingSizeToContinue := limit.MaxTxBytes / 1000
		minRemainingGasToContinue := limit.MaxGasLimit / 1000

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
				accFlushNs += time.Since(tFlush).Nanoseconds()
			}

			if h.lane.Name() != "exchange" {
				return
			}

			totalNs := time.Since(t0).Nanoseconds()
			LanePrepareTotalSeconds.WithLabelValues(h.lane.Name()).Observe(float64(totalNs) / 1e9)

			obs := func(step string, ns int64) {
				LaneSimSeconds.WithLabelValues(h.lane.Name(), step).Observe(float64(ns) / 1e9)
			}
			obs("iter_tx", accIterTxNs)
			obs("sender_info", accSenderInfoNs)
			obs("tx_info", accTxInfoNs)
			obs("skipped_sender", accSkippedSenderNs)
			obs("lane_match", accLaneMatchNs)
			obs("verify_sig_parse", accVerifySigParseNs)
			obs("verify_single", accVerifySingleNs)
			obs("include", accIncludeNs)
			obs("next", accNextNs)
			obs("flush", accFlushNs)

			accountedNs := accIterTxNs + accSenderInfoNs + accTxInfoNs +
				accSkippedSenderNs +
				accLaneMatchNs +
				accVerifySigParseNs + accVerifySingleNs +
				accIncludeNs + accNextNs + accFlushNs
			otherNs := totalNs - accountedNs
			if otherNs < 0 {
				otherNs = 0
			}
			obs("other", otherNs)
		}()

		// Select all transactions in the mempool that are valid and not already in the
		// partial proposal.
		iterator := h.lane.Select(ctx, nil)

		for ; iterator != nil; func() {
			tNext := time.Now()
			iterator = iterator.Next()
			accNextNs += time.Since(tNext).Nanoseconds()
		}() {
			tIterTx := time.Now()
			tx := iterator.Tx()
			accIterTxNs += time.Since(tIterTx).Nanoseconds()

			// ── sender_info ──────────────────────────────────────────────────────────
			// Get sender and nonce from the mempool index key (set at Insert time from signers[0]).
			tSender := time.Now()
			var senderStr string
			var senderNonce uint64
			if sig, ok := iterator.(senderInfoGetter); ok {
				senderStr, senderNonce = sig.SenderInfo()
			}
			accSenderInfoNs += time.Since(tSender).Nanoseconds()

			// ── tx_info ──────────────────────────────────────────────────────────────
			tInfo := time.Now()
			txInfo, err := h.lane.GetTxInfoLight(ctx, tx)
			accTxInfoNs += time.Since(tInfo).Nanoseconds()
			if err != nil {
				h.lane.Logger().Info("failed to get tx info", "err", err)
				txsToRemove = append(txsToRemove, tx)
				continue
			}
			txInfo.Sender = senderStr
			txInfo.Nonce = senderNonce

			// ── skipped_sender ────────────────────────────────────────────────────
			// If the transaction is from a skipped sender, we skip it altogether. Allows to avoid sequence error when
			// first skipped tx is not included in the proposal.
			tSkip := time.Now()
			isSkippedSender := senderStr != "" && func() bool { _, ok := skippedSenders[senderStr]; return ok }()
			if isSkippedSender {
				h.lane.Logger().Debug(
					"failed to select tx for lane; tx from skipped sender",
					"tx_hash", utils.TxHash(txInfo.TxBytes),
					"lane", h.lane.Name(),
				)
				accSkippedSenderNs += time.Since(tSkip).Nanoseconds()
				continue
			}
			accSkippedSenderNs += time.Since(tSkip).Nanoseconds()

			// ── gas_limit ─────────────────────────────────────────────────────────
			// Reject single-tx gas that can never fit regardless of accumulated totals.
			if txInfo.GasLimit > limit.MaxGasLimit {
				h.lane.Logger().Debug(
					"failed to select tx for lane; gas limit above the maximum allowed",
					"lane", h.lane.Name(),
					"tx_gas", txInfo.GasLimit,
					"max_gas", limit.MaxGasLimit,
					"tx_hash", utils.TxHash(txInfo.TxBytes),
				)
				txsToRemove = append(txsToRemove, tx)
				continue
			}

			// ── tx_size ───────────────────────────────────────────────────────────
			// Reject single-tx size that can never fit regardless of accumulated totals.
			if txInfo.Size > limit.MaxTxBytes {
				h.lane.Logger().Debug(
					"failed to select tx for lane; tx bytes above the maximum allowed",
					"lane", h.lane.Name(),
					"tx_size", txInfo.Size,
					"max_tx_bytes", limit.MaxTxBytes,
					"tx_hash", utils.TxHash(txInfo.TxBytes),
				)
				txsToRemove = append(txsToRemove, tx)
				continue
			}

			// ── lane_match ────────────────────────────────────────────────────────
			// Double check that the transaction belongs to this lane.
			tMatch := time.Now()
			if !h.lane.Match(ctx, tx) {
				h.lane.Logger().Debug(
					"failed to select tx for lane; tx does not belong to lane",
					"tx_hash", utils.TxHash(txInfo.TxBytes),
					"lane", h.lane.Name(),
				)
				accLaneMatchNs += time.Since(tMatch).Nanoseconds()
				txsToRemove = append(txsToRemove, tx)
				continue
			}
			accLaneMatchNs += time.Since(tMatch).Nanoseconds()

			// ── already_in_proposal ───────────────────────────────────────────────
			// If the transaction is already in the (partial) block proposal, we skip it.
			if proposal.Contains(txInfo.Key()) {
				h.lane.Logger().Debug(
					"failed to select tx for lane; tx is already in proposal",
					"tx_hash", utils.TxHash(txInfo.TxBytes),
					"lane", h.lane.Name(),
				)
				continue
			}

			// ── updated_size ──────────────────────────────────────────────────────
			// If the transaction is too large, we skip it,
			// but also have to prevent anything from the same sender from being included during this iteration.
			updatedSize := totalSize + txInfo.Size
			if updatedSize > limit.MaxTxBytes {
				h.lane.Logger().Debug(
					"failed to select tx for lane; tx bytes above the maximum allowed",
					"lane", h.lane.Name(),
					"tx_size", txInfo.Size,
					"total_size", totalSize,
					"max_tx_bytes", limit.MaxTxBytes,
					"tx_hash", utils.TxHash(txInfo.TxBytes),
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
					break
				}
				markSkippedSender(senderStr)
				continue
			}

			// ── updated_gas ───────────────────────────────────────────────────────
			// If the gas limit of the transaction is too large, we skip it,
			// but also have to prevent anything from the same sender from being included during this iteration.
			updatedGas := totalGas + txInfo.GasLimit
			if updatedGas > limit.MaxGasLimit {
				h.lane.Logger().Debug(
					"failed to select tx for lane; gas limit above the maximum allowed",
					"lane", h.lane.Name(),
					"tx_gas", txInfo.GasLimit,
					"total_gas", totalGas,
					"max_gas", limit.MaxGasLimit,
					"tx_hash", utils.TxHash(txInfo.TxBytes),
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
					break
				}
				markSkippedSender(senderStr)
				continue
			}

			// ── verify ────────────────────────────────────────────────────────────
			// Verify nonce(s). When FastNonceVerifier is set, parse signature count
			// to choose the optimal path and avoid a full VerifyTx / GetSigners call.
			if h.fastNonceVerifier != nil {
				// verify_sig_parse: read signature count without constructing SignatureV2 when possible.
				tSigParse := time.Now()
				sigCount, sigErr := signatureCount(tx)
				accVerifySigParseNs += time.Since(tSigParse).Nanoseconds()
				if sigErr != nil {
					h.lane.Logger().Info("failed to get signature count", "tx_hash", utils.TxHash(txInfo.TxBytes), "err", sigErr)
					txsToRemove = append(txsToRemove, tx)
					continue
				}
				if sigCount == 0 {
					h.lane.Logger().Info("tx has no signatures", "tx_hash", utils.TxHash(txInfo.TxBytes))
					txsToRemove = append(txsToRemove, tx)
					continue
				}

				if sigCount == 1 {
					// verify_single: fastNonceVerifier for single-signer tx.
					// senderIndex is iterated in ascending nonce order, so:
					//   cmp < 0 (stale):  nonce < expected → remove (will never be valid again).
					//   cmp > 0 (future): nonce > expected → skip entire sender.
					//   err != nil:        non-nonce error → remove.
					tSingle := time.Now()
					cmp, verifyErr := h.fastNonceVerifier(ctx, senderStr, senderNonce)
					accVerifySingleNs += time.Since(tSingle).Nanoseconds()
					if verifyErr != nil {
						h.lane.Logger().Info(
							"failed fast nonce verify (single signer), removing tx",
							"tx_hash", utils.TxHash(txInfo.TxBytes),
							"sender", senderStr,
							"err", verifyErr,
						)
						txsToRemove = append(txsToRemove, tx)
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
							"tx_hash", utils.TxHash(txInfo.TxBytes),
							"sender", senderStr,
							"nonce", senderNonce,
						)
						txsToRemove = append(txsToRemove, tx)
						continue
					}
				} else {
					// verify_multi: multiple independent signers.
					// GetSigners() is needed here; the overhead is acceptable for multi-sig txs.
					tSigParse2 := time.Now()
					sigTx, ok := tx.(signing.SigVerifiableTx)
					if !ok {
						accVerifySigParseNs += time.Since(tSigParse2).Nanoseconds()
						h.lane.Logger().Info("tx does not implement SigVerifiableTx", "tx_hash", utils.TxHash(txInfo.TxBytes))
						txsToRemove = append(txsToRemove, tx)
						continue
					}
					sigs, sigErr := sigTx.GetSignaturesV2()
					accVerifySigParseNs += time.Since(tSigParse2).Nanoseconds()
					if sigErr != nil {
						h.lane.Logger().Info("failed to get signatures", "tx_hash", utils.TxHash(txInfo.TxBytes), "err", sigErr)
						txsToRemove = append(txsToRemove, tx)
						continue
					}
					tSigners := time.Now()
					signers, signersErr := sigTx.GetSigners()
					accVerifySigParseNs += time.Since(tSigners).Nanoseconds()
					if signersErr != nil {
						h.lane.Logger().Info("failed to get signers", "tx_hash", utils.TxHash(txInfo.TxBytes), "err", signersErr)
						txsToRemove = append(txsToRemove, tx)
						continue
					}
					if len(signers) != len(sigs) {
						h.lane.Logger().Info("signer/signature count mismatch", "tx_hash", utils.TxHash(txInfo.TxBytes))
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
								"tx_hash", utils.TxHash(txInfo.TxBytes),
								"signer", addrStr,
								"err", multiErr,
							)
							break
						}
					}
					if multiErr != nil {
						txsToRemove = append(txsToRemove, tx)
						continue
					}
				}
			} else {
				// verify_full: no FastNonceVerifier — fall back to full ante handler.
				if err = h.lane.VerifyTx(ctx, tx, false); err != nil {
					h.lane.Logger().Info(
						"failed to verify tx",
						"tx_hash", utils.TxHash(txInfo.TxBytes),
						"err", err,
					)
					txsToRemove = append(txsToRemove, tx)
					continue
				}
			}

			// ── include ───────────────────────────────────────────────────────────
			tInclude := time.Now()
			totalSize += txInfo.Size
			totalGas += txInfo.GasLimit
			txsToInclude = append(txsToInclude, tx)
			// Carry the already-computed TxWithInfo so the caller avoids a second GetTxInfo call.
			txsWithInfo = append(txsWithInfo, txInfo)
			accIncludeNs += time.Since(tInclude).Nanoseconds()
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
