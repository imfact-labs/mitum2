package isaacstates

import (
	"testing"

	"github.com/imfact-labs/mitum2/base"
	"github.com/imfact-labs/mitum2/isaac"
	"github.com/imfact-labs/mitum2/util"
	"github.com/pkg/errors"
)

func TestPersistConsensusProgressFailureStopsBeforeTransition(t *testing.T) {
	want := errors.New("snapshot write failed")
	whenCalled := false
	args := newVoteproofHandlerArgs()
	args.persistConsensusProgress = func(base.Voteproof, isaac.LastVoteproofs) error { return want }
	args.whenNewVoteproof = func(base.Voteproof, isaac.LastVoteproofs) error {
		whenCalled = true
		return nil
	}
	handler := &voteproofHandler{args: &args}
	err := handler.newVoteproofWithLVPS(nil, isaac.LastVoteproofs{})
	if !errors.Is(err, want) {
		t.Fatalf("persist error not propagated: %v", err)
	}
	if whenCalled {
		t.Fatal("transition continued after persist failure")
	}
}

func TestPersistConsensusProgressRunsBeforeWhenNewVoteproof(t *testing.T) {
	stop := util.ErrNotImplemented.Errorf("stop after order observation")
	order := make([]string, 0, 2)
	args := newVoteproofHandlerArgs()
	args.persistConsensusProgress = func(base.Voteproof, isaac.LastVoteproofs) error {
		order = append(order, "persist")
		return nil
	}
	args.whenNewVoteproof = func(base.Voteproof, isaac.LastVoteproofs) error {
		order = append(order, "when")
		return stop
	}
	handler := &voteproofHandler{args: &args}
	vp := isaac.NewINITVoteproof(base.RawPoint(33, 3))
	err := handler.newVoteproofWithLVPS(vp, isaac.LastVoteproofs{})
	if !errors.Is(err, stop) {
		t.Fatalf("whenNewVoteproof error not propagated: %v", err)
	}
	if len(order) != 2 || order[0] != "persist" || order[1] != "when" {
		t.Fatalf("unexpected transition order: %v", order)
	}
}
