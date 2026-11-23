package xxljob

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// FuzzHandler sends arbitrary requests to every path of the executor, authenticated or not, under both protocols.
// Whatever arrives, the executor must not panic, must answer HTTP 200 with a JSON envelope whose code is 200 or 500,
// and must never accept an unauthenticated request.
func FuzzHandler(f *testing.F) {
	now := time.Now().UnixMilli()
	for _, seed := range []string{
		`{"jobId":1,"logId":1,"executorHandler":"T","executorParams":"x","executorTimeout":1,"logDateTime":` + itoa(now) + `}`,
		`{"jobId":1,"logId":2,"executorHandler":"T","executorBlockStrategy":"COVER_EARLY","glueType":"BEAN"}`,
		`{"jobId":2,"logId":3,"executorHandler":"missing"}`,
		`{"jobId":1,"logId":4,"glueType":"GLUE_SHELL","glueSource":"rm -rf /"}`,
		`{"jobId":1,"logId":5,"executorHandler":"T","executorTimeout":9223372037}`,
		`{"logId":1,"logDateTim":0,"fromLineNum":-5}`,
		`{"logId":1,"logDateTime":1700000000000,"fromLineNum":2147483647}`,
		`[]`, `null`, `{`, ``, `{"jobId":"1"}`, `{"jobId":1e40}`,
	} {
		for path := range 6 {
			f.Add(uint8(path), true, []byte(seed))
		}
	}
	executors := map[string]*Executor{}
	for _, version := range versions {
		h := newHarness(f, map[string]Handler{"T": HandlerFunc(noop)}, func(c *Config) {
			c.AdminVersion = version
			c.MaxQueueSize = 4
			c.MaxActiveTriggers = 50
		})
		executors[version] = h.e
	}

	f.Fuzz(func(t *testing.T, path uint8, authenticated bool, body []byte) {
		for version, e := range executors {
			p := []string{e.proto.RunPath(), "/kill", "/beat", "/idleBeat", "/log", "/unknown"}[int(path)%6]
			req := httptest.NewRequest(http.MethodPost, p, bytes.NewReader(body))
			if authenticated {
				req.Header.Set("Xxl-Job-Access-Token", testToken)
				req.Header.Set("Xxl-Job-Appname", e.cfg.AppName)
			}
			rec := httptest.NewRecorder()

			e.Handler().ServeHTTP(rec, req)

			var res struct {
				Code *int `json:"code"`
			}
			if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &res) != nil || res.Code == nil {
				t.Fatalf("%s %s: not a JSON envelope: %d %q", version, p, rec.Code, rec.Body.String())
			}
			if *res.Code != CodeSuccess && *res.Code != CodeFailure {
				t.Fatalf("%s %s: unexpected code %d", version, p, *res.Code)
			}
			if !authenticated && *res.Code == CodeSuccess {
				t.Fatalf("%s %s: unauthenticated request succeeded", version, p)
			}
		}
	})
}

func itoa(n int64) string { b, _ := json.Marshal(n); return string(b) }

// FuzzChunkOf checks the invariants of log reads for arbitrary logs and starting lines.
func FuzzChunkOf(f *testing.F) {
	f.Add("a\nb\nc", 1)
	f.Add("", 0)
	f.Add("only", 5)
	f.Add(strings.Repeat("x", 3000)+"\ny", -3)
	f.Fuzz(func(t *testing.T, text string, from int) {
		var lines []string
		if text != "" {
			lines = strings.Split(text, "\n")
		}

		c := chunkOf(lines, from)

		if want := max(from, 1); c.FromLine != want {
			t.Fatalf("FromLine = %d, want %d", c.FromLine, want)
		}
		n := c.ToLine - c.FromLine + 1
		if n < 0 || (n == 0) != (c.Content == "") || strings.Count(c.Content, "\n") != n {
			t.Fatalf("lines %d..%d do not match content %q", c.FromLine, c.ToLine, c.Content)
		}
		if c.FromLine <= len(lines) && (c.ToLine != len(lines) || c.Content != strings.Join(lines[c.FromLine-1:], "\n")+"\n") {
			t.Fatalf("reading from line %d of %d did not return the rest of the log", c.FromLine, len(lines))
		}
	})
}

// FuzzLogLine checks that any record is stored as one bounded line, and that truncation keeps valid UTF-8 valid.
func FuzzLogLine(f *testing.F) {
	f.Add([]byte("level=INFO msg=hello\n"))
	f.Add([]byte(strings.Repeat("错", 4000) + "\n"))
	f.Add([]byte{0xff, 0xfe, '\n'})
	f.Fuzz(func(t *testing.T, record []byte) {
		store := NewMemoryLogStore(0)
		w := &execLog{store: store, ref: LogRef{LogID: 1}, app: quiet}

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
