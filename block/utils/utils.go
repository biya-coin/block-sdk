package utils

import (
	"encoding/hex"
	"fmt"
	"strings"
	"sync"

	comettypes "github.com/cometbft/cometbft/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	sdkmempool "github.com/cosmos/cosmos-sdk/types/mempool"

	signerextraction "github.com/skip-mev/block-sdk/v2/adapters/signer_extraction_adapter"
)

// TxWithInfo contains the information required for a transaction to be
// included in a proposal.
type TxWithInfo struct {
	// Sender is the primary signer used to identify the transaction in proposal caches.
	Sender string
	// Nonce is the sender account sequence used to identify the transaction in proposal caches.
	Nonce uint64
	// Size is the size of the transaction in bytes.
	Size int64
	// GasLimit is the gas limit of the transaction.
	GasLimit uint64
	// TxBytes is the bytes of the transaction.
	TxBytes []byte
	// Priority defines the priority of the transaction.
	Priority any
	// Signers defines the signers of a transaction.
	Signers []signerextraction.SignerData
}

// NewTxInfo returns a new TxInfo instance.
func NewTxInfo(
	sender string,
	nonce uint64,
	size int64,
	gasLimit uint64,
	txBytes []byte,
	priority any,
	signers []signerextraction.SignerData,
) TxWithInfo {
	return TxWithInfo{
		Sender:   sender,
		Nonce:    nonce,
		Size:     size,
		GasLimit: gasLimit,
		TxBytes:  txBytes,
		Priority: priority,
		Signers:  signers,
	}
}

// Key returns the proposal-cache identity for this transaction.
func (t TxWithInfo) Key() string {
	return fmt.Sprintf("%s/%d", t.Sender, t.Nonce)
}

// String implements the fmt.Stringer interface.
func (t TxWithInfo) String() string {
	return fmt.Sprintf("TxWithInfo{Sender: %s, Nonce: %d, Size: %d, GasLimit: %d, Priority: %s, Signers: %v}",
		t.Sender, t.Nonce, t.Size, t.GasLimit, t.Priority, t.Signers)
}

// GetTxHash returns the string hash representation of a transaction.
func GetTxHash(encoder sdk.TxEncoder, tx sdk.Tx) (string, error) {
	txBz, err := encoder(tx)
	if err != nil {
		return "", fmt.Errorf("failed to encode transaction: %w", err)
	}

	return TxHash(txBz), nil
}

// TxHash returns the string hash representation of the given transactions.
func TxHash(txBytes []byte) string {
	return strings.ToUpper(hex.EncodeToString(comettypes.Tx(txBytes).Hash()))
}

// GetDecodedTxs returns the decoded transactions from the given bytes.
func GetDecodedTxs(txDecoder sdk.TxDecoder, txs [][]byte) ([]sdk.Tx, error) {
	decodedTxs := make([]sdk.Tx, len(txs))
	errs := make([]error, len(txs))

	var wg sync.WaitGroup
	for i, txBz := range txs {
		wg.Add(1)
		go func(index int, bz []byte) {
			defer wg.Done()

			tx, err := txDecoder(bz)
			if err != nil {
				errs[index] = fmt.Errorf("failed to decode transaction: %w", err)
				return
			}

			decodedTxs[index] = tx
		}(i, txBz)
	}
	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}

	return decodedTxs, nil
}

// GetEncodedTxs returns the encoded transactions from the given bytes.
func GetEncodedTxs(txEncoder sdk.TxEncoder, txs []sdk.Tx) ([][]byte, error) {
	encodedTxs := make([][]byte, len(txs))
	errs := make([]error, len(txs))

	var wg sync.WaitGroup
	for i, tx := range txs {
		wg.Add(1)
		go func(index int, tx sdk.Tx) {
			defer wg.Done()

			txBz, err := txEncoder(tx)
			if err != nil {
				errs[index] = fmt.Errorf("failed to encode transaction: %w", err)
				return
			}

			encodedTxs[index] = txBz
		}(i, tx)
	}
	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}

	return encodedTxs, nil
}

// RemoveTxsFromLane removes the transactions from the given lane's mempool.
func RemoveTxsFromLane(txs []sdk.Tx, mempool sdkmempool.Mempool) error {
	for _, tx := range txs {
		if err := mempool.Remove(tx); err != nil {
			return err
		}
	}

	return nil
}
