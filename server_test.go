package xxljob

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var versions = []string{AdminVersion2, AdminVersion35}

func TestRunReportsResult(t *testing.T) {
	for _, v := range versions {
		t.Run(v, func(t *testing.T) {
			// Scenario: the admin triggers a job that succeeds, one that fails and one that panics
			// Expect: each callback carries the admin's log time, the right code and message, in the callback body
			//         format of the protocol version
			h := newHarness(t, map[string]Handler{
				"OK":    HandlerFunc(func(ctx context.Context, _ *Request) error { SetResult(ctx, "done"); return nil }),
				"Fail":  HandlerFunc(func(context.Context, *Request) error { return errors.New("bad input") }),
				"Panic": HandlerFunc(func(context.Context, *Request) error { panic("boom") }),
			}, func(c *Config) { c.AdminVersion = v })
			form := map[string]string{AdminVersion2: "array", AdminVersion35: "callbackList"}[v]
			logTime := time.Now().UnixMilli()

			for i, name := range []string{"OK", "Fail", "Panic"} {
				r := h.run(t, trigger{JobID: int64(i + 1), LogID: int64(i + 1), LogDateTime: logTime, ExecutorHandler: name})
				require.Equal(t, CodeSuccess, r.Code, r.Msg)
			}

			assert.Equal(t, callback{logTime, CodeSuccess, "done", form}, h.admin.callback(t, 1))
			assert.Equal(t, callback{logTime, CodeFailure, "bad input", form}, h.admin.callback(t, 2))
			assert.Equal(t, callback{logTime, CodeFailure, "panic: boom", form}, h.admin.callback(t, 3))
		})
	}
}

func TestAuthentication(t *testing.T) {
	for _, v := range versions {
		t.Run(v, func(t *testing.T) {
			// Scenario: requests without the token, with a wrong token, or (3.5) a wrong AppName
			// Expect: rejected with the official wording before anything runs; with the right headers it runs
			ran := make(chan struct{}, 10)
			h := newHarness(t, map[string]Handler{"T": HandlerFunc(func(context.Context, *Request) error {
				ran <- struct{}{}
				return nil
			})}, func(c *Config) { c.AdminVersion = v })
			body := trigger{JobID: 1, LogID: 1, LogDateTime: time.Now().UnixMilli(), ExecutorHandler: "T"}
			wrongApp := h.authHeaders()
			wrongApp.Set("Xxl-Job-Appname", "other")

			none := h.post(t, h.e.proto.RunPath(), nil, body)
			wrong := h.post(t, h.e.proto.RunPath(), http.Header{"Xxl-Job-Access-Token": {"nope"}, "Xxl-Job-Appname": {"demo"}}, body)
			ok := h.post(t, h.e.proto.RunPath(), h.authHeaders(), body)

			if v == AdminVersion2 {
				assert.Equal(t, reply{Code: CodeFailure, Msg: "The access token is wrong."}, none)
				assert.Equal(t, reply{Code: CodeFailure, Msg: "The access token is wrong."}, wrong)
			} else {
				assert.Equal(t, reply{Code: CodeFailure, Msg: "invalid request, accessToken or appname is empty."}, none)
				assert.Equal(t, reply{Code: CodeFailure, Msg: "invalid request, accessToken or appname invalid."}, wrong)
				body.LogID = 2
				assert.Equal(t, CodeFailure, h.post(t, h.e.proto.RunPath(), wrongApp, body).Code)
			}
			assert.Equal(t, CodeSuccess, ok.Code)
			h.admin.callback(t, 1)
			assert.Len(t, ran, 1)
		})
	}
}

func TestUnknownPathAndMethod(t *testing.T) {
	// Scenario: a 3.5 admin calls /trigger on an executor using the 2.x protocol; a GET request; an unauthenticated
	//           request to an unknown path
	// Expect: HTTP 200 with the official messages, plus a hint at Config.AdminVersion for the wrong run path; an
	//         unknown path is not revealed before authentication
	h := newHarness(t, nil)

	wrongVersion := h.post(t, "/trigger", h.authHeaders(), struct{}{})
	resp, err := http.Get(h.srv.URL + "/run")
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	unauth := h.post(t, "/whatever", nil, struct{}{})

	assert.Equal(t, CodeFailure, wrongVersion.Code)
	assert.Equal(t, "invalid request, uri-mapping(/trigger) not found. (this executor serves /run: check Config.AdminVersion)", wrongVersion.Msg)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/json;charset=UTF-8", resp.Header.Get("Content-Type"))
	assert.JSONEq(t, `{"code":500,"msg":"invalid request, HttpMethod not support."}`, string(body))
	assert.Equal(t, "The access token is wrong.", unauth.Msg)
}

func TestTriggerValidation(t *testing.T) {
	// Scenario: run requests the executor must refuse
	// Expect: a failure explaining why, nothing runs, and each refusal is counted in Stats.Rejected
	h := newHarness(t, map[string]Handler{"T": HandlerFunc(noop)})
	now := time.Now().UnixMilli()
	cases := []struct {
		name string
		tr   trigger
		want string
	}{
		{"unknown handler", trigger{JobID: 1, LogID: 1, ExecutorHandler: "Nope"}, "job handler [Nope] not found."},
		{"glue script", trigger{JobID: 1, LogID: 2, ExecutorHandler: "T", GlueType: "GLUE_SHELL"}, "glue type [GLUE_SHELL] is not supported, only BEAN"},
		{"zero job id", trigger{LogID: 3, ExecutorHandler: "T"}, "invalid trigger: jobId 0, logId 3"},
		{"negative log id", trigger{JobID: 1, LogID: -4, ExecutorHandler: "T"}, "invalid trigger: jobId 1, logId -4"},
		{"timeout overflow", trigger{JobID: 1, LogID: 5, ExecutorHandler: "T", ExecutorTimeout: math.MaxInt64 / int64(time.Second) * 2}, "invalid executorTimeout"},
		{"log time in the future", trigger{JobID: 1, LogID: 6, ExecutorHandler: "T", LogDateTime: now + int64(30*24*time.Hour/time.Millisecond)}, "invalid logDateTime"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := h.run(t, c.tr)

			assert.Equal(t, CodeFailure, r.Code)
			assert.Contains(t, r.Msg, c.want)
		})
	}
	assert.Equal(t, int64(len(cases)), h.e.Stats().Rejected)
	assert.Zero(t, h.e.Stats().Accepted)
}

func TestKillBeatAndIdleBeat(t *testing.T) {
	// Scenario: kill a running job, kill an idle one; beat and idleBeat while busy and idle
	// Expect: the run is reported killed; an idle kill answers "job thread already killed."; idleBeat reports busy
	//         only while the job runs
	g := newGate()
	h := newHarness(t, map[string]Handler{"T": g})
	h.run(t, trigger{JobID: 1, LogID: 1, ExecutorHandler: "T"})
	g.waitStart(t)
	job := map[string]int64{"jobId": 1}

	busy := h.post(t, "/idleBeat", h.authHeaders(), job)
	beat := h.post(t, "/beat", h.authHeaders(), struct{}{})
	killed := h.post(t, "/kill", h.authHeaders(), job)
	cb := h.admin.callback(t, 1)
	require.Eventually(t, func() bool { return h.post(t, "/idleBeat", h.authHeaders(), job).Code == CodeSuccess }, time.Second, 5*time.Millisecond)
	again := h.post(t, "/kill", h.authHeaders(), job)

	assert.Equal(t, reply{Code: CodeFailure, Msg: "job thread is running or has trigger queue."}, busy)
	assert.Equal(t, CodeSuccess, beat.Code)
	assert.Equal(t, CodeSuccess, killed.Code)
	assert.Equal(t, callback{cb.LogTime, CodeFailure, ErrKilled.Error(), "array"}, cb)
	assert.Equal(t, reply{Code: CodeSuccess, Msg: "job thread already killed."}, again)
}

func TestTimeoutReportedAtDeadline(t *testing.T) {
	// Scenario: a job with a 1s timeout whose handler ignores its context for 3s and then returns nil
	// Expect: the admin gets 502 at the deadline, not success when the handler finally returns
	h := newHarness(t, map[string]Handler{"Stubborn": HandlerFunc(func(context.Context, *Request) error {
		time.Sleep(3 * time.Second)
		return nil
	})})

	start := time.Now()
	h.run(t, trigger{JobID: 1, LogID: 1, ExecutorHandler: "Stubborn", ExecutorTimeout: 1})
	cb := h.admin.callback(t, 1)

	assert.Equal(t, CodeTimeout, cb.Code)
	assert.Equal(t, ErrTimeout.Error(), cb.Msg)
	assert.Less(t, time.Since(start), 2500*time.Millisecond)
}

func TestExecutionLog(t *testing.T) {
	for _, v := range versions {
		t.Run(v, func(t *testing.T) {
			// Scenario: the admin's log viewer reads a finished run's log, then asks past its end, then for an
			//           unknown log
			// Expect: one read returns the whole log (start line, job lines, end line) with isEnd; past the end
			//         nothing new; an unknown log returns a notice the viewer shows (toLineNum >= fromLineNum)
			h := newHarness(t, map[string]Handler{"T": HandlerFunc(func(ctx context.Context, _ *Request) error {
				Logger(ctx).Info("step one", "rows", 3)
				Logger(ctx).Warn("step two")
				return nil
			})}, func(c *Config) { c.AdminVersion = v })
			now := time.Now().UnixMilli()
			timeField := map[string]string{AdminVersion2: "logDateTim", AdminVersion35: "logDateTime"}[v]
			h.run(t, trigger{JobID: 1, LogID: 1, LogDateTime: now, ExecutorHandler: "T", ExecutorParams: "p"})
			h.admin.callback(t, 1)
			read := func(logID int64, from int) *logPayload {
				return h.post(t, "/log", h.authHeaders(), map[string]any{"logId": logID, timeField: now, "fromLineNum": from}).Log
			}

			first := read(1, 1)
			next := read(1, first.ToLineNum+1)
			missing := read(404, 1)

			require.NotNil(t, first)
			lines := strings.Split(strings.TrimSuffix(first.LogContent, "\n"), "\n")
			require.Len(t, lines, 4)
			assert.Contains(t, lines[0], `msg="job start" handler=T params=p`)
			assert.Contains(t, lines[1], `level=INFO msg="step one" rows=3`)
			assert.Contains(t, lines[2], `level=WARN msg="step two"`)
			assert.Contains(t, lines[3], `msg="job succeeded"`)
			assert.Equal(t, logPayload{FromLineNum: 1, ToLineNum: 4, LogContent: first.LogContent, IsEnd: true}, *first)
			assert.Equal(t, logPayload{FromLineNum: 5, ToLineNum: 4, IsEnd: true}, *next)
			assert.Equal(t, 1, missing.ToLineNum)
			assert.True(t, missing.IsEnd)
			assert.Contains(t, missing.LogContent, "log not found on this executor (log_id=404)")
		})
	}
}

func TestSharding(t *testing.T) {
	// Scenario: a sharding broadcast trigger, and a plain one
	// Expect: the handler sees the shard index and total; a plain trigger is shard 0 of 1
	got := make(chan string, 2)
	h := newHarness(t, map[string]Handler{"S": HandlerFunc(func(_ context.Context, req *Request) error {
		got <- fmt.Sprintf("%d/%d", req.ShardIndex, req.ShardTotal)
		return nil
	})})

	h.run(t, trigger{JobID: 1, LogID: 1, ExecutorHandler: "S", BroadcastIndex: 2, BroadcastTotal: 3})
	h.admin.callback(t, 1)
	h.run(t, trigger{JobID: 2, LogID: 2, ExecutorHandler: "S"})
	h.admin.callback(t, 2)

	assert.Equal(t, "2/3", <-got)
	assert.Equal(t, "0/1", <-got)
}
