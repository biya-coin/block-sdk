package block

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"

	"cosmossdk.io/log"
	"cosmossdk.io/math"
	sdk "github.com/cosmos/cosmos-sdk/types"
	sdkmempool "github.com/cosmos/cosmos-sdk/types/mempool"

	signer_extraction "github.com/skip-mev/block-sdk/v2/adapters/signer_extraction_adapter"
)

var _ Mempool = (*LanedMempool)(nil)

type (
	// Mempool defines the Block SDK mempool interface.
	Mempool interface {
		sdkmempool.Mempool

		// Registry returns the lanes in the mempool.
		Registry() []Lane
		// Contains returns true if any of the lanes currently contain the transaction.
		Contains(tx sdk.Tx) bool
		// GetTxDistribution returns the number of transactions in each lane.
		GetTxDistribution() map[string]uint64
	}

	// TxSignerInfo stores signer data that was extracted before mempool
	// removal. The Index field maps this entry back to the original tx slice.
	TxSignerInfo struct {
		Index       int
		Tx          sdk.Tx
		Signers     []signer_extraction.SignerData
		LaneIndex   int
		LaneName    string
		LaneFound   bool
		LaneChecked bool
		Err         error
	}

	// LanedMempool defines the Block SDK mempool implementation. It contains a registry
	// of lanes, which allows for customizable block proposal construction.
	LanedMempool struct {
		logger log.Logger

		// registry contains the lanes in the mempool. The lanes are ordered
		// according to their priority. The first lane in the registry has the
		// highest priority and the last lane has the lowest priority.
		registry []Lane

		// txIndex tracks which signers have pending transactions in which lanes.
		txIndex *TxIndex
	}

	signerAwareLane interface {
		ContainsWithSigners(sdk.Tx, []signer_extraction.SignerData) bool
		RemoveWithSigners(sdk.Tx, []signer_extraction.SignerData) error
	}

	batchSignerAwareLane interface {
		ContainsManyWithSigners([]sdk.Tx, [][]signer_extraction.SignerData) []bool
	}
)

// NewLanedMempool returns a new Block SDK LanedMempool. The laned mempool comprises
// a registry of lanes. Each lane is responsible for selecting transactions according
// to its own selection logic. The lanes are ordered according to their priority. The
// first lane in the registry has the highest priority. Proposals are verified according
// to the order of the lanes in the registry. Each transaction SHOULD only belong in one lane.
func NewLanedMempool(
	logger log.Logger,
	lanes []Lane,
) (*LanedMempool, error) {
	mempool := &LanedMempool{
		logger:   logger,
		registry: lanes,
		txIndex:  NewTxIndex(),
	}

	if err := mempool.ValidateBasic(); err != nil {
		return nil, err
	}

	return mempool, nil
}

// CountTx returns the total number of transactions in the mempool. This will
// be the sum of the number of transactions in each lane.
func (m *LanedMempool) CountTx() int {
	var total int
	for _, lane := range m.registry {
		total += lane.CountTx()
	}

	return total
}

// GetTxDistribution returns the number of transactions in each lane.
func (m *LanedMempool) GetTxDistribution() map[string]uint64 {
	counts := make(map[string]uint64, len(m.registry))

	for _, lane := range m.registry {
		counts[lane.Name()] = uint64(lane.CountTx())
	}

	return counts
}

// Insert will insert a transaction into the mempool. It inserts the transaction
// into the first lane that it matches.
func (m *LanedMempool) Insert(ctx context.Context, tx sdk.Tx) (err error) {
	defer func() {
		if r := recover(); r != nil {
			m.logger.Error("panic in Insert", "err", r)
			err = fmt.Errorf("panic in Insert: %v", r)
		}
	}()

	sdkCtx := sdk.UnwrapSDKContext(ctx)

laneMatching:
	for index, lane := range m.registry {
		if lane.Match(sdkCtx, tx) {
			signersData, err := lane.SignerExtractor().GetSigners(tx)
			if err != nil {
				m.logger.Error("failed to extract signers upon insertion for tx", "tx", tx, "err", err)
				return nil
			}
			for _, signerData := range signersData {
				if m.txIndex.DoesExistInLowerPriorityLane(signerData.Signer.String(), index) {

					// If the transaction exists in a lower priority lane, do not insert it.
					// This is because it could cause account sequence mismatches.
					continue laneMatching
				}

			}

			err = lane.Insert(ctx, tx)
			if err != nil {
				return err
			}

			sig := signersData[0]
			firstSignerIdentifier := sig.Signer.String()
			firstSignerNonce := sig.Sequence

			for _, signerData := range signersData {
				m.txIndex.Insert(signerData.Signer.String(), lane.Name(), index, firstSignerIdentifier, firstSignerNonce)
			}

			return nil
		}
	}

	return nil
}

// Select returns a nil iterator.
//
// TODO:
// - Determine if it even makes sense to return an iterator. What does that even
// mean in the context where you have multiple lanes?
// - Perhaps consider implementing and returning a no-op iterator?
func (m *LanedMempool) Select(_ context.Context, _ [][]byte) sdkmempool.Iterator {
	return nil
}

// Remove removes a transaction from the mempool. This assumes that the transaction
// is contained in only one of the lanes.
func (m *LanedMempool) Remove(tx sdk.Tx) (err error) {
	defer func() {
		if r := recover(); r != nil {
			m.logger.Error("panic in Remove", "err", r)
			err = fmt.Errorf("panic in Remove: %v", r)
		}
	}()

	return m.removeLegacy(tx)
}

func (m *LanedMempool) removeLegacy(tx sdk.Tx) error {
	for _, lane := range m.registry {
		if lane.Contains(tx) {
			err := lane.Remove(tx)
			if err != nil {
				return err
			}

			signersData, err := lane.SignerExtractor().GetSigners(tx)
			if err != nil {
				m.logger.Error("failed to extract signers upon removal for tx", "tx", tx, "err", err)
				return nil
			}

			sig := signersData[0]
			firstSignerIdentifier := sig.Signer.String()
			firstSignerNonce := sig.Sequence

			for _, signerData := range signersData {
				m.txIndex.Remove(signerData.Signer.String(), lane.Name(), firstSignerIdentifier, firstSignerNonce)
			}

			return nil
		}
	}

	return nil
}

// PreExtractSigners extracts signer data for all transactions in parallel.
// The returned slice has the same order as txs.
func (m *LanedMempool) PreExtractSigners(txs []sdk.Tx) []TxSignerInfo {
	infos := make([]TxSignerInfo, len(txs))
	if len(txs) == 0 {
		return infos
	}

	workers := runtime.GOMAXPROCS(0)
	if workers > len(txs) {
		workers = len(txs)
	}
	if workers < 1 {
		workers = 1
	}

	jobs := make(chan int)
	var wg sync.WaitGroup
	wg.Add(workers)
	for worker := 0; worker < workers; worker++ {
		go func() {
			defer wg.Done()
			for index := range jobs {
				tx := txs[index]
				signers, err := m.extractSignersForRemoval(tx)
				infos[index] = TxSignerInfo{
					Index:     index,
					Tx:        tx,
					Signers:   signers,
					LaneIndex: -1,
					Err:       err,
				}
			}
		}()
	}

	for index := range txs {
		jobs <- index
	}
	close(jobs)
	wg.Wait()

	m.assignLanesWithSigners(infos)

	return infos
}

// PreExtractSignerInfo extracts signer data for callers that should not import
// Block SDK concrete types. Entries must be passed back to RemoveWithSignerInfo.
func (m *LanedMempool) PreExtractSignerInfo(txs []sdk.Tx) []any {
	infos := m.PreExtractSigners(txs)
	opaqueInfos := make([]any, len(infos))
	for i := range infos {
		opaqueInfos[i] = infos[i]
	}

	return opaqueInfos
}

// RemoveMany removes transactions using signer data extracted in parallel
// before the removal loop.
func (m *LanedMempool) RemoveMany(txs []sdk.Tx) (err error) {
	defer func() {
		if r := recover(); r != nil {
			m.logger.Error("panic in RemoveMany", "err", r)
			err = fmt.Errorf("panic in RemoveMany: %v", r)
		}
	}()

	infos := m.PreExtractSigners(txs)
	return m.RemoveManyWithSigners(infos)
}

// RemoveManyWithSigners removes transactions using signer data supplied by the
// caller. Entries with signer extraction errors fall back to the legacy Remove
// path to preserve existing behavior.
func (m *LanedMempool) RemoveManyWithSigners(infos []TxSignerInfo) error {
	for _, info := range infos {
		if err := m.removeWithSignerInfo(info); err != nil {
			return err
		}
	}

	return nil
}

// RemoveWithSigners removes one transaction using signer data supplied by the
// caller.
func (m *LanedMempool) RemoveWithSigners(tx sdk.Tx, signers []signer_extraction.SignerData) (err error) {
	defer func() {
		if r := recover(); r != nil {
			m.logger.Error("panic in RemoveWithSigners", "err", r)
			err = fmt.Errorf("panic in RemoveWithSigners: %v", r)
		}
	}()

	return m.removeWithSigners(tx, signers)
}

// RemoveWithSignerInfo removes one transaction using opaque signer info
// returned by PreExtractSignerInfo.
func (m *LanedMempool) RemoveWithSignerInfo(tx sdk.Tx, info any) (err error) {
	defer func() {
		if r := recover(); r != nil {
			m.logger.Error("panic in RemoveWithSignerInfo", "err", r)
			err = fmt.Errorf("panic in RemoveWithSignerInfo: %v", r)
		}
	}()

	switch typedInfo := info.(type) {
	case TxSignerInfo:
		if typedInfo.Tx == nil {
			typedInfo.Tx = tx
		}

		return m.removeWithSignerInfo(typedInfo)
	case *TxSignerInfo:
		if typedInfo == nil {
			return m.removeLegacy(tx)
		}
		if typedInfo.Tx == nil {
			typedInfo.Tx = tx
		}

		return m.removeWithSignerInfo(*typedInfo)
	default:
		return m.removeLegacy(tx)
	}
}

func (m *LanedMempool) extractSignersForRemoval(tx sdk.Tx) ([]signer_extraction.SignerData, error) {
	for _, lane := range m.registry {
		extractor := lane.SignerExtractor()
		if extractor == nil {
			continue
		}

		signers, err := extractor.GetSigners(tx)
		if err == nil {
			if len(signers) == 0 {
				return nil, fmt.Errorf("no signers found for tx during removal")
			}

			return signers, nil
		}
	}

	return nil, fmt.Errorf("failed to extract signers from all lanes")
}

func (m *LanedMempool) removeWithSigners(tx sdk.Tx, signersData []signer_extraction.SignerData) error {
	if len(signersData) == 0 {
		return fmt.Errorf("no signers found for tx during removal")
	}

	for _, lane := range m.registry {
		if !m.laneContainsWithSigners(lane, tx, signersData) {
			continue
		}

		if err := m.laneRemoveWithSigners(lane, tx, signersData); err != nil {
			return err
		}

		m.removeTxIndexWithSigners(lane.Name(), signersData)
		return nil
	}

	return nil
}

func (m *LanedMempool) removeWithSignerInfo(info TxSignerInfo) error {
	if info.Err != nil {
		m.logger.Error("failed to pre-extract signers upon removal for tx", "tx", info.Tx, "err", info.Err)
		return m.removeLegacy(info.Tx)
	}
	if len(info.Signers) == 0 {
		return m.removeLegacy(info.Tx)
	}

	if info.LaneFound && info.LaneIndex >= 0 && info.LaneIndex < len(m.registry) {
		lane := m.registry[info.LaneIndex]
		if info.LaneName == "" || lane.Name() == info.LaneName {
			if err := m.laneRemoveWithSigners(lane, info.Tx, info.Signers); err != nil {
				if errors.Is(err, sdkmempool.ErrTxNotFound) {
					return m.removeWithSigners(info.Tx, info.Signers)
				}

				return err
			}

			m.removeTxIndexWithSigners(lane.Name(), info.Signers)
			return nil
		}
	}
	if info.LaneChecked {
		return m.removeLegacy(info.Tx)
	}

	return m.removeWithSigners(info.Tx, info.Signers)
}

func (m *LanedMempool) assignLanesWithSigners(infos []TxSignerInfo) {
	pending := make([]int, 0, len(infos))
	for i := range infos {
		infos[i].LaneIndex = -1
		if infos[i].Err == nil && len(infos[i].Signers) > 0 {
			pending = append(pending, i)
		}
	}
	if len(pending) == 0 {
		return
	}

	for laneIndex, lane := range m.registry {
		if len(pending) == 0 {
			return
		}

		txs := make([]sdk.Tx, len(pending))
		signersList := make([][]signer_extraction.SignerData, len(pending))
		for i, infoIndex := range pending {
			txs[i] = infos[infoIndex].Tx
			signersList[i] = infos[infoIndex].Signers
		}

		contains := m.laneContainsManyWithSigners(lane, txs, signersList)
		nextPending := pending[:0]
		for i, infoIndex := range pending {
			if i < len(contains) && contains[i] {
				infos[infoIndex].LaneIndex = laneIndex
				infos[infoIndex].LaneName = lane.Name()
				infos[infoIndex].LaneFound = true
				infos[infoIndex].LaneChecked = true
				continue
			}

			nextPending = append(nextPending, infoIndex)
		}
		pending = nextPending
	}

	for _, infoIndex := range pending {
		infos[infoIndex].LaneChecked = true
	}
}

func (m *LanedMempool) removeTxIndexWithSigners(laneName string, signersData []signer_extraction.SignerData) {
	if len(signersData) == 0 {
		return
	}

	sig := signersData[0]
	firstSignerIdentifier := sig.Signer.String()
	firstSignerNonce := sig.Sequence

	for _, signerData := range signersData {
		m.txIndex.Remove(signerData.Signer.String(), laneName, firstSignerIdentifier, firstSignerNonce)
	}
}

func (m *LanedMempool) laneContainsWithSigners(lane Lane, tx sdk.Tx, signers []signer_extraction.SignerData) bool {
	if aware, ok := lane.(signerAwareLane); ok {
		return aware.ContainsWithSigners(tx, signers)
	}

	return lane.Contains(tx)
}

func (m *LanedMempool) laneContainsManyWithSigners(lane Lane, txs []sdk.Tx, signersList [][]signer_extraction.SignerData) []bool {
	if aware, ok := lane.(batchSignerAwareLane); ok {
		return aware.ContainsManyWithSigners(txs, signersList)
	}

	contains := make([]bool, len(txs))
	for i, tx := range txs {
		contains[i] = m.laneContainsWithSigners(lane, tx, signersList[i])
	}

	return contains
}

func (m *LanedMempool) laneRemoveWithSigners(lane Lane, tx sdk.Tx, signers []signer_extraction.SignerData) error {
	if aware, ok := lane.(signerAwareLane); ok {
		return aware.RemoveWithSigners(tx, signers)
	}

	return lane.Remove(tx)
}

// Contains returns true if the transaction is contained in any of the lanes.
func (m *LanedMempool) Contains(tx sdk.Tx) (contains bool) {
	defer func() {
		if r := recover(); r != nil {
			m.logger.Error("panic in Contains", "err", r)
			contains = false
		}
	}()

	for _, lane := range m.registry {
		if lane.Contains(tx) {
			return true
		}
	}

	return false
}

// Registry returns the lanes in the mempool.
func (m *LanedMempool) Registry() []Lane {
	return m.registry
}

// ValidateBasic validates the mempools configuration. ValidateBasic ensures
// the following:
// - The sum of the lane max block space percentages is less than or equal to 1.
// - There is no unused block space.
func (m *LanedMempool) ValidateBasic() error {
	if len(m.registry) == 0 {
		return fmt.Errorf("registry cannot be nil; must configure at least one lane")
	}

	sum := math.LegacyZeroDec()
	seenZeroMaxBlockSpace := false
	seenLanes := make(map[string]struct{})

	for _, lane := range m.registry {
		name := lane.Name()
		if _, seen := seenLanes[name]; seen {
			return fmt.Errorf("duplicate lane name %s", name)
		}

		maxBlockSpace := lane.GetMaxBlockSpace()
		if seenZeroMaxBlockSpace && maxBlockSpace.IsZero() {
			return fmt.Errorf("only one lane can have unlimited max block space")
		} else if maxBlockSpace.IsZero() {
			seenZeroMaxBlockSpace = true
		}

		sum = sum.Add(lane.GetMaxBlockSpace())
		seenLanes[name] = struct{}{}
	}

	switch {
	// Ensure that the sum of the lane max block space percentages is less than
	// or equal to 1.
	case sum.GT(math.LegacyOneDec()):
		return fmt.Errorf("sum of lane max block space percentages must be less than or equal to 1, got %s", sum)
	// Ensure that there is no unused block space.
	case sum.LT(math.LegacyOneDec()) && !seenZeroMaxBlockSpace:
		return fmt.Errorf("sum of total block space percentages will be less than 1")
	}

	return nil
}
