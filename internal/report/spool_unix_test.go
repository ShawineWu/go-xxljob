//go:build unix

package report

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ShawineWu/go-xxljob/internal/wire"
)

func TestSpoolDoesNotBlockOnAFIFO(t *testing.T) {
	// Scenario: a FIFO named like a saved file is in the Dir when results are loaded (it may be swapped in after the
	//           directory was listed, so readSpoolFile itself must not block on it)
	// Expect: reading it fails at once instead of blocking with the spool locked
	path := filepath.Join(t.TempDir(), "callback-1-1-1.claimed")
	require.NoError(t, syscall.Mkfifo(path, 0o600))
	done := make(chan error, 1)

	go func() { _, _, err := readSpoolFile(path, 0); done <- err }()

	select {
	case err := <-done:
		require.ErrorContains(t, err, "not a regular file")
	case <-time.After(2 * time.Second):
		t.Fatal("readSpoolFile blocked on a FIFO")
	}
}

func TestSpoolLoadsOnlyWhatItWrites(t *testing.T) {
	// Scenario: a symlink to a file outside the Dir is read as a saved file; a saved file has a result with a 900 KB
	//           message and a line over 1 MiB; the Dir holds a leftover temporary file and a bad file
	// Expect: the symlink is refused; the message is cut to what the admin accepts and the long line is skipped
	//         (the rest of the file still loads); adopt removes the temporary file; the bad file does not count
	//         against the size limit
	dir := t.TempDir()
	outside := filepath.Join(t.TempDir(), "secret")
	require.NoError(t, os.WriteFile(outside, []byte(`{"logId":99}`+"\n"), 0o600))
	link := filepath.Join(dir, "callback-1-1-1.claimed")
	require.NoError(t, os.Symlink(outside, link))
	big := filepath.Join(dir, "callback-2-1-1.jsonl")
	require.NoError(t, os.WriteFile(big, []byte(`{"logId":1,"msg":"`+strings.Repeat("m", 900_000)+`"}`+"\n"+
		strings.Repeat("x", 3<<20)+"\n"+`{"logId":2}`), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "callback-3-1-1.tmp"), make([]byte, 100), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "callback-4-1-1.jsonl.bad"), make([]byte, 1<<20), 0o600))
	sp, err := newSpool(dir, 0)
	require.NoError(t, err)

	_, _, linkErr := readSpoolFile(link, 0)
	got, skipped, bigErr := readSpoolFile(big, 0)
	require.NoError(t, sp.adopt())
	used, err := sp.used()
	require.NoError(t, err)

	require.ErrorContains(t, linkErr, "not a regular file")
	require.NoError(t, bigErr)
	require.Len(t, got, 2)
	assert.Equal(t, wire.TruncateMsg(strings.Repeat("m", 900_000)), got[0].Msg)
	assert.Equal(t, int64(2), got[1].LogID)
	assert.Equal(t, 1, skipped)
	assert.NoFileExists(t, filepath.Join(dir, "callback-3-1-1.tmp"))
	bigInfo, err := os.Stat(big)
	require.NoError(t, err)
	assert.Equal(t, bigInfo.Size(), used, "only the saved file counts, not the bad one")
}

func TestSpoolFilesArePrivate(t *testing.T) {
	// Scenario: results are saved
	// Expect: the file is readable and writable by the owner only (it holds job results)
	dir := t.TempDir()
	sp, err := newSpool(dir, 0)
	require.NoError(t, err)

	require.NoError(t, sp.save(results(1, 1)))
	files, _ := filepath.Glob(filepath.Join(dir, "*"+spoolExt))
	require.Len(t, files, 1)
	info, err := os.Stat(files[0])
	require.NoError(t, err)

	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}
