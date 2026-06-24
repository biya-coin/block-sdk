package checktx

import (
	"fmt"
	"runtime/debug"

	cmtproto "github.com/cometbft/cometbft/api/cometbft/types/v1"

	errorsmod "cosmossdk.io/errors"
	"cosmossdk.io/log"
	cometabci "github.com/cometbft/cometbft/abci/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	sdkerrors "github.com/cosmos/cosmos-sdk/types/errors"

	"github.com/skip-mev/block-sdk/v2/block"
)

// MempoolParityCheckTx is a CheckTx function that evicts txs that are not in the app-side mempool
// on ReCheckTx. This handler is used to enforce parity in the app-side / comet mempools.
type MempoolParityCheckTx struct {
	// logger
	logger log.Logger

	// app side mempool interface
	mempl block.Mempool

	// tx-decoder
	txDecoder sdk.TxDecoder

	// checkTxHandler to wrap
	checkTxHandler CheckTx

	// baseApp is utilized to retrieve the latest committed state and to call
	// baseapp's CheckTx method.
	baseApp BaseApp
}

// NewMempoolParityCheckTx returns a new MempoolParityCheckTx handler.
func NewMempoolParityCheckTx(
	logger log.Logger,
	mempl block.Mempool,
	txDecoder sdk.TxDecoder,
	checkTxHandler CheckTx,
	baseApp BaseApp,
) MempoolParityCheckTx {
	return MempoolParityCheckTx{
		logger:         logger,
		mempl:          mempl,
		txDecoder:      txDecoder,
		checkTxHandler: checkTxHandler,
		baseApp:        baseApp,
	}
}

// CheckTx returns a CheckTx handler that wraps a given CheckTx handler and evicts txs that are not
// in the app-side mempool on ReCheckTx.
func (m MempoolParityCheckTx) CheckTx() CheckTx {
	return func(req *cometabci.CheckTxRequest) (checkRes *cometabci.CheckTxResponse, checkErr error) {
		defer func(checkRes **cometabci.CheckTxResponse, checkErr *error) {
			if r := recover(); r != nil {
				m.logger.Error("panic in CheckTx (MempoolParityCheckTx)", "panic", r)
				debug.PrintStack()

				if err, ok := r.(error); ok {
					*checkRes = sdkerrors.CheckTxResponseWithEvents(
						err,
						0,
						0,
						nil,
						true,
					)
				} else {
					*checkRes = sdkerrors.CheckTxResponseWithEvents(
						fmt.Errorf("panic in CheckTx (MempoolParityCheckTx): %v", r),
						0,
						0,
						nil,
						true,
					)
				}
			}
		}(&checkRes, &checkErr)

		// decode tx
		tx, err := m.txDecoder(req.Tx)
		if err != nil {
			return sdkerrors.CheckTxResponseWithEvents(
				fmt.Errorf("failed to decode tx: %w", err),
				0,
				0,
				nil,
				false,
			), nil
		}

		// run the checkTxHandler
		res, checkTxError := m.checkTxHandler(req)

		// can fail for a variety of reasons, check the results of the checkTxHandler
		// need to remove from mempool if re-check fails and tx is in mempool.
		if isInvalidCheckTxExecution(res, checkTxError) {
			m.logger.Debug("failed base checkTx", "err", checkTxError, "res", fmt.Sprintf("%+v", res))
			return res, checkTxError
		}

		sdkCtx := m.GetContextForTx(req)
		lane, err := m.matchLane(sdkCtx, tx)
		if err != nil {

			m.logger.Debug("failed to match lane", "lane", lane, "err", err)
			return sdkerrors.CheckTxResponseWithEvents(
				err,
				0,
				0,
				nil,
				false,
			), nil
		}

		consensusParams := sdkCtx.ConsensusParams()
		blockMaxBytes := consensusParams.GetBlock().GetMaxBytes()

		var laneSizeBytes int64
		if laneBlockSpace := lane.GetMaxBlockSpace(); laneBlockSpace.IsZero() {
			// for default lane, we use the block max bytes
			laneSizeBytes = blockMaxBytes
		} else {
			// for other lanes, we use the lane's max block space
			laneSizeBytes = laneBlockSpace.MulInt64(blockMaxBytes).TruncateInt64()
		}

		txSize := int64(len(req.Tx))
		if txSize > laneSizeBytes {
			m.logger.Debug(
				"tx size exceeds max lane size bytes",
				"tx", tx,
				"tx size", txSize,
				"max bytes", laneSizeBytes,
			)

			return sdkerrors.CheckTxResponseWithEvents(
				errorsmod.Wrapf(sdkerrors.ErrTxTooLarge, "tx size exceeds max bytes for lane %s", lane.Name()),
				0,
				0,
				nil,
				false,
			), nil
		}

		return res, nil
	}
}

// matchLane returns a Lane if the given tx matches the Lane.
func (m MempoolParityCheckTx) matchLane(ctx sdk.Context, tx sdk.Tx) (block.Lane, error) {
	var lane block.Lane
	// find corresponding lane for this tx
	for _, l := range m.mempl.Registry() {
		if l.Match(ctx, tx) {
			lane = l
			break
		}
	}

	if lane == nil {
		m.logger.Debug(
			"failed match tx to lane",
			"tx", tx,
		)

		return nil, fmt.Errorf("failed match tx to lane")
	}

	return lane, nil
}

func isInvalidCheckTxExecution(resp *cometabci.CheckTxResponse, checkTxErr error) bool {
	return resp == nil || resp.Code != 0 || checkTxErr != nil
}

// GetContextForTx is returns the latest committed state and sets the context given
// the checkTx request.
func (m MempoolParityCheckTx) GetContextForTx(req *cometabci.CheckTxRequest) sdk.Context {
	// Retrieve the commit multi-store which is used to retrieve the latest committed state.
	ms := m.baseApp.CommitMultiStore().CacheMultiStore()

	// Create a new context based off of the latest committed state.
	header := cmtproto.Header{
		Height:  m.baseApp.LastBlockHeight(),
		ChainID: m.baseApp.ChainID(),
	}
	ctx, _ := sdk.NewContext(ms, header, true, m.baseApp.Logger()).CacheContext()

	// Set the remaining important context values.
	ctx = ctx.
		WithTxBytes(req.Tx).
		WithEventManager(sdk.NewEventManager()).
		WithConsensusParams(m.baseApp.GetConsensusParams(ctx))

	return ctx
}
