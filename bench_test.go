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

// BenchmarkTrigger measures accepting a trigger over the executor's HTTP handler (authentication, decoding,
// validation, block strategy, queueing), spread over 64 jobs. Runs and callbacks happen in the background.
func BenchmarkTrigger(b *testing.B) {
	h := newHarness(b, map[string]Handler{"T": HandlerFunc(noop)}, func(c *Config) { c.MaxActiveTriggers = 1 << 20 })
	handler := h.e.HTTPHandler()
	now := time.Now().UnixMilli()
	var logID int64
	b.ReportAllocs()
	for b.Loop() {
		logID++
		body, _ := json.Marshal(trigger{JobID: logID%64 + 1, LogID: logID, LogDateTime: now, ExecutorHandler: "T"})
		req := httptest.NewRequest(http.MethodPost, "/run", bytes.NewReader(body))
		req.Header.Set("Xxl-Job-Access-Token", testToken)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if !strings.Contains(rec.Body.String(), `"code":200`) {
			b.Fatalf("trigger rejected: %s", rec.Body.String())
		}
	}
}

// BenchmarkRun measures one complete run as Invoke performs it: the execution log's start and end lines, a handler
// that logs one line, and the result.
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

// BenchmarkMemoryLogStoreWrite measures appending a typical line to a full store, where every write evicts.
func BenchmarkMemoryLogStoreWrite(b *testing.B) {
	s := NewMemoryLogStore(1 << 20)
	line := `time=2026-01-02T15:04:05.000+08:00 level=INFO msg="synced batch" batch=12 size=500`
	var i int64
	b.ReportAllocs()
	for b.Loop() {
		i++
		_ = s.Write(LogRef{LogID: i / 10}, line)
		if i%10 == 9 {
			_ = s.End(LogRef{LogID: i / 10})
		}
	}
}
