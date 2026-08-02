package temporalbeads

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFileBeadStorePersistsReadyTransitionAndOutboxAtomically(t *testing.T) {
	path := filepath.Join(t.TempDir(), "beads.json")
	store, err := OpenFileBeadStore(path, NewManualClock(time.Unix(100, 0)))
	require.NoError(t, err)

	event, err := store.TransitionReady(
		context.Background(),
		"city",
		"run",
		"gc-1",
		1,
		validFormulaRef(),
	)
	require.NoError(t, err)

	reopened, err := OpenFileBeadStore(path, NewManualClock(time.Unix(200, 0)))
	require.NoError(t, err)
	record, err := reopened.Inspect(context.Background(), "gc-1")
	require.NoError(t, err)
	require.Equal(t, BeadStatusReady, record.Status)
	require.Equal(t, int64(1), record.Generation)

	pending, err := reopened.PendingReadyEvents(context.Background())
	require.NoError(t, err)
	require.Equal(t, []ReadyEvent{event}, pending)
}

func TestFileBeadStoreConcurrentClaimHasOneOwner(t *testing.T) {
	store := newReadyStore(t)
	const claimers = 32

	var wait sync.WaitGroup
	wait.Add(claimers)
	leases := make(chan ClaimLease, claimers)
	errs := make(chan error, claimers)
	for index := 0; index < claimers; index++ {
		go func(index int) {
			defer wait.Done()
			independent, err := OpenFileBeadStore(store.path, store.clock)
			if err != nil {
				errs <- err
				return
			}
			lease, err := independent.Claim(context.Background(), ClaimRequest{
				BeadID:     "gc-1",
				Generation: 1,
				WorkflowID: "workflow-" + string(rune('a'+index)),
			})
			if err != nil {
				errs <- err
				return
			}
			leases <- lease
		}(index)
	}
	wait.Wait()
	close(leases)
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	var acquired []ClaimLease
	for lease := range leases {
		if lease.Acquired {
			acquired = append(acquired, lease)
		}
	}
	require.Len(t, acquired, 1)
	require.NotEmpty(t, acquired[0].Token)

	record, err := store.Inspect(context.Background(), "gc-1")
	require.NoError(t, err)
	require.Equal(t, acquired[0].Token, record.ClaimToken)
}

func TestFileBeadStoreRetryReacquiresSameLease(t *testing.T) {
	store := newReadyStore(t)
	request := ClaimRequest{BeadID: "gc-1", Generation: 1, WorkflowID: "workflow-a"}

	first, err := store.Claim(context.Background(), request)
	require.NoError(t, err)
	second, err := store.Claim(context.Background(), request)
	require.NoError(t, err)

	require.True(t, first.Acquired)
	require.Equal(t, first, second)
}

func TestFileBeadStoreIdempotentCompletionRejectsChangedArtifacts(t *testing.T) {
	store := newReadyStore(t)
	lease, err := store.Claim(context.Background(), ClaimRequest{
		BeadID: "gc-1", Generation: 1, WorkflowID: "workflow-a",
	})
	require.NoError(t, err)
	completion := Completion{
		BeadID: "gc-1", Generation: 1, ClaimToken: lease.Token,
		SessionID: "session-1", Outcome: OutcomeCompleted,
		ArtifactRefs: []ArtifactRef{testArtifact()},
	}
	require.NoError(t, store.Complete(context.Background(), completion))
	require.NoError(t, store.Complete(context.Background(), completion))

	changed := completion
	changed.ArtifactRefs = nil
	require.ErrorContains(
		t,
		store.Complete(context.Background(), changed),
		"conflicts with existing terminal outcome",
	)
}

func TestFileBeadStoreDeduplicatesAttemptFailure(t *testing.T) {
	store := newReadyStore(t)
	lease, err := store.Claim(context.Background(), ClaimRequest{
		BeadID: "gc-1", Generation: 1, WorkflowID: "workflow-a",
	})
	require.NoError(t, err)
	failure := AttemptFailure{
		BeadID: "gc-1", Generation: 1, ClaimToken: lease.Token,
		Attempt: 1, Code: "agent-execution-failed",
	}
	require.NoError(t, store.RecordAttemptFailure(context.Background(), failure))
	require.NoError(t, store.RecordAttemptFailure(context.Background(), failure))
	record, err := store.Inspect(context.Background(), "gc-1")
	require.NoError(t, err)
	require.Equal(t, []AttemptFailure{failure}, record.AttemptFailure)
}

func TestStaleGenerationCannotOverwriteCurrentOutcome(t *testing.T) {
	store := newReadyStore(t)
	oldLease, err := store.Claim(context.Background(), ClaimRequest{
		BeadID: "gc-1", Generation: 1, WorkflowID: "workflow-old",
	})
	require.NoError(t, err)

	_, err = store.TransitionReady(
		context.Background(), "city", "run", "gc-1", 2, validFormulaRef(),
	)
	require.NoError(t, err)
	currentLease, err := store.Claim(context.Background(), ClaimRequest{
		BeadID: "gc-1", Generation: 2, WorkflowID: "workflow-current",
	})
	require.NoError(t, err)

	oldCompletion := Completion{
		BeadID: "gc-1", Generation: 1, ClaimToken: oldLease.Token,
		SessionID: "old-session", Outcome: OutcomeCompleted,
	}
	require.ErrorIs(t, store.Complete(context.Background(), oldCompletion), ErrStaleFence)

	before, err := store.Inspect(context.Background(), "gc-1")
	require.NoError(t, err)
	require.Empty(t, before.Outcome)
	require.Equal(t, currentLease.Token, before.ClaimToken)

	currentCompletion := Completion{
		BeadID: "gc-1", Generation: 2, ClaimToken: currentLease.Token,
		SessionID: "current-session", Outcome: OutcomeCompleted,
	}
	require.NoError(t, store.Complete(context.Background(), currentCompletion))
	after, err := store.Inspect(context.Background(), "gc-1")
	require.NoError(t, err)
	require.Equal(t, OutcomeCompleted, after.Outcome)
	require.Equal(t, "current-session", after.SessionID)
}

func TestTemporalUnavailableLeavesReadyFactInspectable(t *testing.T) {
	store := newReadyStore(t)

	record, err := store.Inspect(context.Background(), "gc-1")
	require.NoError(t, err)
	require.Equal(t, BeadStatusReady, record.Status)
	require.Empty(t, record.ClaimToken)

	pending, err := store.PendingReadyEvents(context.Background())
	require.NoError(t, err)
	require.Len(t, pending, 1)
}

func newReadyStore(t *testing.T) *FileBeadStore {
	t.Helper()
	store, err := OpenFileBeadStore(
		filepath.Join(t.TempDir(), "beads.json"),
		NewManualClock(time.Date(2026, 7, 30, 12, 0, 0, 0, time.UTC)),
	)
	require.NoError(t, err)
	_, err = store.TransitionReady(
		context.Background(), "city", "run", "gc-1", 1, validFormulaRef(),
	)
	require.NoError(t, err)
	return store
}

func TestCompleteRejectsInvalidTerminalFactsWithoutMutatingBead(t *testing.T) {
	store := newReadyStore(t)
	event := onlyPendingEvent(t, store)
	workflowID, err := WorkflowID(event.CityID, event.RunID, event.BeadID)
	require.NoError(t, err)
	lease, err := store.Claim(context.Background(), ClaimRequest{
		BeadID: event.BeadID, Generation: event.Generation, WorkflowID: workflowID,
	})
	require.NoError(t, err)
	require.True(t, lease.Acquired)

	err = store.Complete(context.Background(), Completion{
		BeadID: event.BeadID, Generation: event.Generation, ClaimToken: lease.Token,
		Outcome: OutcomeCompleted,
	})
	require.ErrorContains(t, err, "session id")
	err = store.Complete(context.Background(), Completion{
		BeadID: event.BeadID, Generation: event.Generation, ClaimToken: lease.Token,
		SessionID: "session-1", Outcome: Outcome("failed"),
	})
	require.ErrorContains(t, err, "completion outcome")

	record, err := store.Inspect(context.Background(), event.BeadID)
	require.NoError(t, err)
	require.Equal(t, BeadStatusClaimed, record.Status)
	require.Empty(t, record.Outcome)
}

func TestNewGenerationSupersedesUndeliveredOutboxEvent(t *testing.T) {
	store := newReadyStore(t)
	first := onlyPendingEvent(t, store)
	second, err := store.TransitionReady(
		context.Background(),
		first.CityID,
		first.RunID,
		first.BeadID,
		first.Generation+1,
		first.Formula,
	)
	require.NoError(t, err)

	pending, err := store.PendingReadyEvents(context.Background())
	require.NoError(t, err)
	require.Equal(t, []ReadyEvent{second}, pending)
}

func TestReadyGenerationRejectsChangedFormulaSnapshot(t *testing.T) {
	store := newReadyStore(t)
	event := onlyPendingEvent(t, store)
	changed := event.Formula
	changed.Hash = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"

	_, err := store.TransitionReady(
		context.Background(),
		event.CityID,
		event.RunID,
		event.BeadID,
		event.Generation,
		changed,
	)

	require.ErrorContains(t, err, "different formula snapshot")
	require.Equal(t, event, onlyPendingEvent(t, store))
}

func (s *FileBeadStore) forceUnsafeCompletionForTest(completion Completion) {
	s.mu.Lock()
	defer s.mu.Unlock()
	release, err := s.lockState()
	if err != nil {
		panic(err)
	}
	defer release()
	record := s.state.Beads[completion.BeadID]
	record.Status = BeadStatusCompleted
	record.SessionID = completion.SessionID
	record.Outcome = completion.Outcome
	s.state.Beads[completion.BeadID] = record
	if err := s.persistLocked(); err != nil {
		panic(err)
	}
}
