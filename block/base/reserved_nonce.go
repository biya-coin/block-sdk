package base

import sdk "github.com/cosmos/cosmos-sdk/types"

type reservedNonceContextKey struct{}

// ReservedNonceSet contains signer sequences already consumed by the uncommitted
// parent chain that the current proposal extends.
type ReservedNonceSet struct {
	bySender map[string]map[uint64]struct{}
}

func NewReservedNonceSet() *ReservedNonceSet {
	return &ReservedNonceSet{bySender: make(map[string]map[uint64]struct{})}
}

func (r *ReservedNonceSet) Add(sender string, nonce uint64) {
	if r == nil || sender == "" {
		return
	}
	nonces, ok := r.bySender[sender]
	if !ok {
		nonces = make(map[uint64]struct{})
		r.bySender[sender] = nonces
	}
	nonces[nonce] = struct{}{}
}

func (r *ReservedNonceSet) Contains(sender string, nonce uint64) bool {
	if r == nil {
		return false
	}
	nonces, ok := r.bySender[sender]
	if !ok {
		return false
	}
	_, ok = nonces[nonce]
	return ok
}

func (r *ReservedNonceSet) ApplyToExpected(sender string, expected uint64) uint64 {
	if r == nil {
		return expected
	}
	nonces, ok := r.bySender[sender]
	if !ok {
		return expected
	}
	for {
		if _, ok := nonces[expected]; !ok {
			return expected
		}
		expected++
	}
}

func (r *ReservedNonceSet) Empty() bool {
	return r == nil || len(r.bySender) == 0
}

func (r *ReservedNonceSet) NumSenders() int {
	if r == nil {
		return 0
	}
	return len(r.bySender)
}

func (r *ReservedNonceSet) NumNonces() int {
	if r == nil {
		return 0
	}
	total := 0
	for _, nonces := range r.bySender {
		total += len(nonces)
	}
	return total
}

func WithReservedNonceSet(ctx sdk.Context, reserved *ReservedNonceSet) sdk.Context {
	return ctx.WithValue(reservedNonceContextKey{}, reserved)
}

func ReservedNonceSetFromContext(ctx sdk.Context) *ReservedNonceSet {
	reserved, _ := ctx.Value(reservedNonceContextKey{}).(*ReservedNonceSet)
	return reserved
}
