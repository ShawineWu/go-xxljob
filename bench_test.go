package xxljob

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// BenchmarkTrigger measures accepting a trigger over the executor's HTTP handler (authentication, decoding, block
// strategy, queueing), spread over 64 jobs. Execution and callbacks run in the background.
func BenchmarkTrigger(b *testing.B) {
	h := newHarness(b, map[string]Handler{"T": HandlerFunc(noop)})
	handler := h.e.Handler()
	now := time.Now().UnixMilli()
	var logID int64
	b.ReportAllocs()
	for b.Loop() {
		logID++
		body, _ := json.Marshal(runReq{JobID: logID % 64, LogID: logID, LogDateTime: now, ExecutorHandler: "T"})
		req := httptest.NewRequest(http.MethodPost, "/run", bytes.NewReader(body))
		req.Header.Set(tokenHeader, testToken)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if !strings.Contains(rec.Body.String(), `"code":200`) {
			b.Fatalf("trigger rejected: %s", rec.Body.String())
		}
	}
}

// BenchmarkRun measures one complete run as Invoke performs it: context setup, start and end lines in the execution
// log, a handler that logs one line, and the result.
func BenchmarkRun(b *testing.B) {
	h := HandlerFunc(func(ctx context.Context, req *Request) error {
		Logger(ctx).Info("processing", "params", req.Params)
		return nil
	})
	req := &Request{Handler: "T", Params: `{"n":1}`}
	b.ReportAllocs()
	for b.Loop() {
		if res := Invoke(context.Background(), h, req); res.Err != nil {
			b.Fatal(res.Err)
		}
	}
}

// BenchmarkMemoryLogStoreWrite measures appending a typical log line, including eviction once the store is full.
func BenchmarkMemoryLogStoreWrite(b *testing.B) {
	s := NewMemoryLogStore(1 << 20)
	line := `time=2026-01-02T15:04:05.000+08:00 level=INFO msg="synced batch" batch=12 size=500`
	var i int64
	b.ReportAllocs()
	for b.Loop() {
		i++
		_ = s.Write(LogRef{LogID: i / 100}, line)
	}
}
