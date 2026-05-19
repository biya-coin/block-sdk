package base

import (
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	comettypes "github.com/cometbft/cometbft/types"
	sdk "github.com/cosmos/cosmos-sdk/types"

	"github.com/skip-mev/block-sdk/v2/block/utils"
)

// TxInfoTiming holds per-call sub-step durations for GetTxInfo.
type TxInfoTiming struct {
	Count      int64
	TotalUs    int64
	EncodeUs   int64
	HashUs     int64
	CastUs     int64
	SignersUs  int64
	PriorityUs int64
	GasUs      int64
}

// per-lane accumulator flushed when block height changes.
var (
	txInfoMu     sync.Mutex
	txInfoHeight int64
	txInfoAccMap = make(map[string]TxInfoTiming)
)

// GetTxInfo returns various information about the transaction that
// belongs to the lane including its priority, signer's, sequence number,
// size and more. It accumulates timing per block and prints one log line
// per block height change.
func (l *BaseLane) GetTxInfo(ctx sdk.Context, tx sdk.Tx) (utils.TxWithInfo, error) {
	t0 := time.Now()
	txBytes, err := l.cfg.TxEncoder(tx)
	encodeUs := time.Since(t0).Microseconds()
	if err != nil {
		return utils.TxWithInfo{}, fmt.Errorf("failed to encode transaction: %w", err)
	}

	// TODO: Add an adapter to lanes so that this can be flexible to support EVM, etc.
	t1 := time.Now()
	hash := strings.ToUpper(hex.EncodeToString(comettypes.Tx(txBytes).Hash()))
	hashUs := time.Since(t1).Microseconds()

	tCast := time.Now()
	gasTx, ok := tx.(sdk.FeeTx)
	castUs := time.Since(tCast).Microseconds()
	if !ok {
		return utils.TxWithInfo{}, fmt.Errorf("failed to cast transaction to gas tx")
	}

	t2 := time.Now()
	signers, err := l.cfg.SignerExtractor.GetSigners(tx)
	signersUs := time.Since(t2).Microseconds()
	if err != nil {
		return utils.TxWithInfo{}, err
	}

	t3 := time.Now()
	priority := l.LaneMempool.Priority(ctx, tx)
	priorityUs := time.Since(t3).Microseconds()

	t4 := time.Now()
	gasLimit := gasTx.GetGas()
	gasUs := time.Since(t4).Microseconds()

	totalUs := encodeUs + hashUs + castUs + signersUs + priorityUs + gasUs

	// Accumulate and print once per block height per lane.
	h := ctx.BlockHeight()
	name := l.Name()
	txInfoMu.Lock()
	if txInfoHeight != h {
		// height changed: flush exchange lane only
		if acc, ok := txInfoAccMap["exchange"]; ok {
			fmt.Printf("msg=get_tx_info_timing lane=exchange height=%d count=%d total_us=%d encode_us=%d hash_us=%d cast_us=%d signers_us=%d priority_us=%d gas_us=%d\n",
				txInfoHeight, acc.Count, acc.TotalUs, acc.EncodeUs, acc.HashUs, acc.CastUs, acc.SignersUs, acc.PriorityUs, acc.GasUs)
		}
		txInfoHeight = h
		txInfoAccMap = make(map[string]TxInfoTiming)
	}
	acc := txInfoAccMap[name]
	acc.Count++
	acc.TotalUs += totalUs
	acc.EncodeUs += encodeUs
	acc.HashUs += hashUs
	acc.CastUs += castUs
	acc.SignersUs += signersUs
	acc.PriorityUs += priorityUs
	acc.GasUs += gasUs
	txInfoAccMap[name] = acc
	txInfoMu.Unlock()

	return utils.TxWithInfo{
		Hash:     hash,
		Size:     int64(len(txBytes)),
		GasLimit: gasLimit,
		TxBytes:  txBytes,
		Priority: priority,
		Signers:  signers,
	}, nil
}
