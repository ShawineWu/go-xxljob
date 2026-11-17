package xxljob

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf8"
)

// FuzzHandler sends arbitrary bodies to every executor endpoint, authenticated or not, under both protocols.
// Whatever arrives, the executor must not panic and must answer with a JSON envelope whose code is 200 or 500.
func FuzzHandler(f *testing.F) {
	for _, seed := range []string{
		`{"jobId":1,"logId":1,"executorHandler":"T","executorParams":"x","executorTimeout":1}`,
		`{"jobId":1,"logId":2,"executorHandler":"T","executorBlockStrategy":"COVER_EARLY","glueType":"BEAN"}`,
		`{"jobId":2,"logId":3,"executorHandler":"missing"}`,
		`{"jobId":1,"glueType":"GLUE_SHELL","glueSource":"rm -rf /"}`,
		`{"logId":1,"logDateTim":0,"fromLineNum":-5}`,
		`{"logId":1,"logDateTime":1700000000000,"fromLineNum":2147483647}`,
		`[]`, `null`, `{`, ``, `{"jobId":"1"}`, `{"jobId":1e40}`,
	} {
		for path := range 6 {
			f.Add(uint8(path), true, []byte(seed))
		}
	}
	executors := map[string]*Executor{}
	for _, version := range []string{AdminVersion2, AdminVersion35} {
		h := newHarness(f, map[string]Handler{"T": HandlerFunc(noop)}, func(c *Config) {
			c.AdminVersion = version
			c.MaxQueueSize = 4
		})
		executors[version] = h.e
	}
	paths := func(e *Executor) []string {
		return []string{e.proto.runPath(), "/kill", "/beat", "/idleBeat", "/log", "/unknown"}
	}

	f.Fuzz(func(t *testing.T, path uint8, authenticated bool, body []byte) {
		for version, e := range executors {
			p := paths(e)[int(path)%6]
			req := httptest.NewRequest(http.MethodPost, p, bytes.NewReader(body))
			if authenticated {
				req.Header.Set(tokenHeader, testToken)
				req.Header.Set(appNameHeader, e.cfg.AppName)
			}
			rec := httptest.NewRecorder()

			e.Handler().ServeHTTP(rec, req)

			if p == "/unknown" {
				if rec.Code != http.StatusNotFound {
					t.Fatalf("%s %s: status %d, want 404", version, p, rec.Code)
				}
				continue
			}
			var res struct {
				Code *int `json:"code"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil || res.Code == nil {
				t.Fatalf("%s %s: response is not a JSON envelope: %q", version, p, rec.Body.String())
			}
			if *res.Code != codeSuccess && *res.Code != codeFail {
				t.Fatalf("%s %s: unexpected code %d", version, p, *res.Code)
			}
			if !authenticated && *res.Code == codeSuccess {
				t.Fatalf("%s %s: unauthenticated request succeeded", version, p)
			}
		}
	})
}

// FuzzChunkOf checks the paging invariants of execution log reads for arbitrary logs and starting lines.
func FuzzChunkOf(f *testing.F) {
	f.Add("a\nb\nc", 1, true)
	f.Add("", 0, false)
	f.Add("only", 5, true)
	f.Add(strings.Repeat("x", 3000)+"\ny", -3, true)
	f.Fuzz(func(t *testing.T, text string, from int, ended bool) {
		var lines []string
		if text != "" {
			lines = strings.Split(text, "\n")
		}

		c := chunkOf(lines, from, ended)

		if want := max(from, 1); c.FromLine != want {
			t.Fatalf("FromLine = %d, want %d", c.FromLine, want)
		}
		n := c.ToLine - c.FromLine + 1
		if n < 0 || (n == 0) != (c.Content == "") || strings.Count(c.Content, "\n") != n {
			t.Fatalf("lines %d..%d do not match content %q", c.FromLine, c.ToLine, c.Content)
		}
		if n > 0 && c.Content != strings.Join(lines[c.FromLine-1:c.ToLine], "\n")+"\n" {
			t.Fatalf("content does not match lines %d..%d", c.FromLine, c.ToLine)
		}
		if n > 1 && len(c.Content) > MaxLogChunkBytes {
			t.Fatalf("chunk of %d lines has %d bytes", n, len(c.Content))
		}
		if c.FromLine <= len(lines) && n == 0 {
			t.Fatalf("no progress reading from line %d of %d", c.FromLine, len(lines))
		}
		if c.End != (ended && c.ToLine >= len(lines)) {
			t.Fatalf("End = %v with ended=%v, ToLine=%d of %d", c.End, ended, c.ToLine, len(lines))
		}
	})
}

// FuzzLogLine checks that the execution log stores any record as a single bounded line, and that truncation never
// produces invalid UTF-8 from valid input.
func FuzzLogLine(f *testing.F) {
	f.Add([]byte("level=INFO msg=hello\n"))
	f.Add([]byte(strings.Repeat("错", 4000) + "\n"))
	f.Add([]byte{0xff, 0xfe, '\n'})
	f.Fuzz(func(t *testing.T, record []byte) {
		store := NewMemoryLogStore(0)
		w := &lineWriter{store: store, ref: LogRef{LogID: 1}}

		n, err := w.Write(record)

		if err != nil || n != len(record) {
			t.Fatalf("Write = %d, %v", n, err)
		}
		c, err := store.Read(LogRef{LogID: 1}, 1)
		if err != nil {
			t.Fatal(err)
		}
		line := strings.TrimSuffix(c.Content, "\n")
		if len(line) > maxLogLineBytes+len("... (truncated)") {
			t.Fatalf("line of %d bytes exceeds the cap", len(line))
		}
		if utf8.Valid(record) && !utf8.ValidString(line) {
			t.Fatalf("truncation broke UTF-8: %q", line)
		}
	})
}
