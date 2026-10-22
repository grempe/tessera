package posix_test

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/transparency-dev/tessera"
	"github.com/transparency-dev/tessera/storage/posix"
	"golang.org/x/mod/sumdb/note"
)

// TestGarbageCollectionPerPassLimit builds a 400-bundle log with garbage
// collection disabled, then reopens it with garbage collection every second
// and records how far each pass gets, from the driver's .state/gcState file.
func TestGarbageCollectionPerPassLimit(t *testing.T) {
	const bundles = 400
	ctx := t.Context()
	dir := t.TempDir()
	skey, _, err := note.GenerateKey(rand.Reader, "example.com/gc-repro")
	if err != nil {
		t.Fatal(err)
	}
	signer, err := note.NewSigner(skey)
	if err != nil {
		t.Fatal(err)
	}
	open := func(gcInterval time.Duration) (*tessera.Appender, func(context.Context) error, tessera.LogReader) {
		t.Helper()
		d, err := posix.New(ctx, posix.Config{Path: dir})
		if err != nil {
			t.Fatal(err)
		}
		a, shutdown, r, err := tessera.NewAppender(ctx, d, tessera.NewAppendOptions().
			WithCheckpointSigner(signer).
			WithCheckpointInterval(time.Second).
			WithGarbageCollectionInterval(gcInterval))
		if err != nil {
			t.Fatal(err)
		}
		return a, shutdown, r
	}

	// Build the backlog with garbage collection disabled.
	a, shutdown, r := open(0)
	var last tessera.IndexFuture
	for i := range bundles * 256 {
		last = a.Add(ctx, tessera.NewEntry(fmt.Appendf(nil, "entry %d", i)))
	}
	if _, _, err := tessera.NewPublicationAwaiter(ctx, r.ReadCheckpoint, 100*time.Millisecond).Await(ctx, last); err != nil {
		t.Fatal(err)
	}
	if err := shutdown(ctx); err != nil {
		t.Fatal(err)
	}

	// Reopen with garbage collection every second, and watch it work through
	// the backlog.
	_, shutdown, _ = open(time.Second)
	defer func() { _ = shutdown(context.Background()) }()
	var steps []uint64
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		raw, err := os.ReadFile(filepath.Join(dir, ".state", "gcState"))
		if err != nil {
			continue // no pass has completed yet
		}
		var s struct{ FromSize uint64 }
		if err := json.Unmarshal(raw, &s); err != nil {
			continue // caught mid-write
		}
		if len(steps) == 0 || s.FromSize != steps[len(steps)-1] {
			steps = append(steps, s.FromSize)
			t.Logf("garbage collected up to entry %d", s.FromSize)
		}
		if s.FromSize == bundles*256 {
			break
		}
	}
	if len(steps) > 1 {
		t.Errorf("garbage collection took %d passes to cover a backlog of %d bundles (%d entries): %v", len(steps), bundles, bundles*256, steps)
	}
}
