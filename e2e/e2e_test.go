//go:build e2e

// Package e2e verifies the xxljob executor against a real xxl-job-admin (docker-compose.yml; the version is selected by
// XXL_JOB_VERSION: 2.4.1 / 3.4.2 / 3.5.0). The executor runs on the host. The tests prepare executors and jobs and check
// trigger results through the database, and trigger, kill, and read execution logs through the admin's management API
// (these steps must go through the admin to exercise the protocol).
//
// Run with make e2e (a single version) or make e2e-all (all versions); see run.sh.
package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"os/exec"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ShawineWu/go-xxljob"
)

const (
	appName = "go-e2e"
	token   = "e2e-token" // 2.x / 3.4: the global xxl.job.accessToken in docker-compose; 3.5+: stored on the executor record
	// Default admin account created by the schema SQL (exists only in the local test container).
	adminUser     = "admin"
	adminPassword = "123456"
	mysqlService  = "xxljob-e2e-mysql-1"
)

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

var (
	adminBase    = env("XXLJOB_E2E_ADMIN", "http://127.0.0.1:18080/xxl-job-admin")
	adminVersion = env("XXLJOB_E2E_VERSION", "2.4.1")
	executorPort = env("XXLJOB_E2E_PORT", "9999")
)

// profile holds the per-version admin differences the tests rely on: client config, login endpoint, table columns.
type profile struct {
	protocol   string // xxljob.Config.AdminVersion
	runPath    string // trigger path: /run -> /trigger since 3.5
	loginPath  string // login moved to /auth/doLogin in 3.2
	groupName  string // executor name column: title -> name since 3.5
	jobName    string // job name column: job_desc -> name since 3.5
	groupToken bool   // since 3.5 the AccessToken is stored per executor in xxl_job_group.access_token
}

func profileOf(t *testing.T, version string) profile {
	var major, minor int
	_, err := fmt.Sscanf(version, "%d.%d", &major, &minor)
	require.NoError(t, err, "XXLJOB_E2E_VERSION %q", version)
	at := func(ma, mi int) bool { return major > ma || major == ma && minor >= mi }
	p := profile{protocol: xxljob.AdminVersion2, runPath: "/run", loginPath: "/login", groupName: "title", jobName: "job_desc"}
	if at(3, 2) {
		p.loginPath = "/auth/doLogin"
	}
	if at(3, 5) {
		p.protocol, p.runPath, p.groupName, p.jobName, p.groupToken = xxljob.AdminVersion35, "/trigger", "name", "name", true
	}
	return p
}

// ---------------------------------------------------------------------------------------------
// Database (prepare data, check results; see profile for per-version schema differences)
// ---------------------------------------------------------------------------------------------

func mysql(t *testing.T, query string) string {
	t.Helper()
	out, err := exec.Command("docker", "exec", mysqlService, "mysql", "-uroot", "-proot", "--default-character-set=utf8mb4",
		"-N", "-s", "xxl_job", "-e", query).CombinedOutput()
	require.NoError(t, err, "%s: %s", query, out)
	var lines []string
	for _, l := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if !strings.HasPrefix(l, "mysql: [Warning]") {
			lines = append(lines, l)
		}
	}
	return strings.Join(lines, "\n")
}

func createGroup(t *testing.T, p profile) int {
	cols, vals := "app_name, "+p.groupName+", address_type, update_time", fmt.Sprintf("'%s', 'Go E2E', 0, NOW()", appName)
	if p.groupToken {
		cols, vals = cols+", access_token", vals+", '"+token+"'"
	}
	id, err := strconv.Atoi(mysql(t, fmt.Sprintf("INSERT INTO xxl_job_group (%s) VALUES (%s); SELECT LAST_INSERT_ID();", cols, vals)))
	require.NoError(t, err)
	return id
}

type jobOpts struct {
	block   string
	timeout int
	route   string
	cron    string // if set, schedule by cron and start the job (the admin triggers it itself)
}

func createJob(t *testing.T, p profile, group int, handler string, o jobOpts) int {
	t.Helper()
	if o.block == "" {
		o.block = xxljob.BlockSerial
	}
	if o.route == "" {
		o.route = "FIRST"
	}
	scheduleType, status, next := "NONE", 0, int64(0)
	if o.cron != "" {
		scheduleType, status, next = "CRON", 1, time.Now().UnixMilli()
	}
	q := fmt.Sprintf(`INSERT INTO xxl_job_info (job_group, %s, add_time, update_time, author, alarm_email, schedule_type, schedule_conf,
		misfire_strategy, executor_route_strategy, executor_handler, executor_param, executor_block_strategy, executor_timeout,
		executor_fail_retry_count, glue_type, glue_source, glue_remark, glue_updatetime, child_jobid, trigger_status,
		trigger_last_time, trigger_next_time)
		VALUES (%d, 'e2e %s', NOW(), NOW(), 'e2e', '', '%s', '%s', 'DO_NOTHING', '%s', '%s', '', '%s', %d, 0, 'BEAN', '', 'e2e',
		NOW(), '', %d, 0, %d); SELECT LAST_INSERT_ID();`,
		p.jobName, group, handler, scheduleType, o.cron, o.route, handler, o.block, o.timeout, status, next)
	id, err := strconv.Atoi(mysql(t, q))
	require.NoError(t, err)
	return id
}

type jobLog struct {
	ID          int64
	TriggerCode int
	TriggerMsg  string
	HandleCode  int
	HandleMsg   string
}

// waitLog waits until the job has a trigger log record that satisfies pred.
func waitLog(t *testing.T, jobID int, pred func(jobLog) bool) jobLog {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		out := mysql(t, fmt.Sprintf(`SELECT id, trigger_code, handle_code, REPLACE(IFNULL(handle_msg,''), '\t', ' '),
			REPLACE(REPLACE(IFNULL(trigger_msg,''), '\t', ' '), '\n', ' ') FROM xxl_job_log WHERE job_id=%d ORDER BY id`, jobID))
		for _, line := range strings.Split(out, "\n") {
			f := strings.SplitN(line, "\t", 5)
			if len(f) < 5 {
				continue
			}
			l := jobLog{TriggerMsg: f[4], HandleMsg: f[3]}
			l.ID, _ = strconv.ParseInt(f[0], 10, 64)
			l.TriggerCode, _ = strconv.Atoi(f[1])
			l.HandleCode, _ = strconv.Atoi(f[2])
			if pred(l) {
				return l
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("job %d: no matching log", jobID)
	return jobLog{}
}

func finished(l jobLog) bool { return l.HandleCode > 0 }

// ---------------------------------------------------------------------------------------------
// Admin management API: only for trigger, kill, and execution logs (these must go through the admin -> executor protocol)
// ---------------------------------------------------------------------------------------------

type console struct {
	t  *testing.T
	hc *http.Client
}

// apiResult decodes both the 2.x ReturnT (content) and the 3.3+ Response (data).
type apiResult struct {
	Code    int             `json:"code"`
	Msg     string          `json:"msg"`
	Content json.RawMessage `json:"content"`
	Data    json.RawMessage `json:"data"`
}

func (r apiResult) payload() json.RawMessage {
	if len(r.Data) > 0 && string(r.Data) != "null" {
		return r.Data
	}
	return r.Content
}

func newConsole(t *testing.T, p profile) *console {
	jar, err := cookiejar.New(nil)
	require.NoError(t, err)
	c := &console{t: t, hc: &http.Client{Jar: jar, Timeout: 10 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	r := c.call(p.loginPath, url.Values{"userName": {adminUser}, "password": {adminPassword}})
	require.Equal(t, 200, r.Code, "login: %s", r.Msg)
	return c
}

func (c *console) call(path string, form url.Values) apiResult {
	c.t.Helper()
	resp, err := c.hc.PostForm(adminBase+path, form)
	require.NoError(c.t, err)
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	require.NoError(c.t, err)
	require.Equal(c.t, http.StatusOK, resp.StatusCode, "%s: %s", path, body)
	var r apiResult
	require.NoError(c.t, json.Unmarshal(body, &r), "%s: %s", path, body)
	return r
}

func (c *console) trigger(jobID int, param string) {
	c.t.Helper()
	r := c.call("/jobinfo/trigger", url.Values{"id": {strconv.Itoa(jobID)}, "executorParam": {param}, "addressList": {""}})
	require.Equal(c.t, 200, r.Code, "trigger %d: %s", jobID, r.Msg)
}

func (c *console) kill(logID int64) apiResult {
	c.t.Helper()
	return c.call("/joblog/logKill", url.Values{"id": {strconv.FormatInt(logID, 10)}})
}

func (c *console) logContent(logID int64) string {
	c.t.Helper()
	r := c.call("/joblog/logDetailCat", url.Values{"logId": {strconv.FormatInt(logID, 10)}, "fromLineNum": {"1"}})
	require.Equal(c.t, 200, r.Code, "logDetailCat: %s", r.Msg)
	var lr struct {
		LogContent string `json:"logContent"`
	}
	require.NoError(c.t, json.Unmarshal(r.payload(), &lr))
	return lr.LogContent
}

// ---------------------------------------------------------------------------------------------
// Executor
// ---------------------------------------------------------------------------------------------

// gauge tracks the number of concurrent executions and its peak.
type gauge struct{ cur, max atomic.Int32 }

func (g *gauge) enter() func() {
	n := g.cur.Add(1)
	for {
		m := g.max.Load()
		if n <= m || g.max.CompareAndSwap(m, n) {
			break
		}
	}
	return func() { g.cur.Add(-1) }
}

// slow blocks until ctx is canceled, then takes another 500ms to return (simulating slow cleanup).
func slow(g *gauge) xxljob.Handler {
	return xxljob.HandlerFunc(func(ctx context.Context, req *xxljob.Request) error {
		defer g.enter()()
		xxljob.Logger(ctx).Info("waiting for cancellation")
		select {
		case <-ctx.Done():
			time.Sleep(500 * time.Millisecond)
			return ctx.Err()
		case <-time.After(time.Minute):
			return nil
		}
	})
}

func postExecutor(t *testing.T, path string, headers map[string]string, body string) string {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "http://127.0.0.1:"+executorPort+path, strings.NewReader(body))
	require.NoError(t, err)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return string(b)
}

// ---------------------------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------------------------

func TestExecutorAgainstRealAdmin(t *testing.T) {
	// Scenario: the executor works with a real xxl-job-admin (XXLJOB_E2E_VERSION), covering registration, execution, logs,
	//           kill, timeout, block strategies, sharding, authentication, and shutdown
	// Given: an admin started by docker-compose; the executor (protocol version chosen to match the admin version) runs on
	//        the host and is reached via host.docker.internal
	// Expect: the trigger and execution results recorded by the admin match the executor's semantics (see each subtest)
	p := profileOf(t, adminVersion)
	t.Logf("xxl-job-admin %s, executor protocol %s", adminVersion, p.protocol)
	group := createGroup(t, p) // since 3.5 the executor must exist in the admin (with its token) before it can register

	port, err := strconv.Atoi(executorPort)
	require.NoError(t, err)
	exec, err := xxljob.New(xxljob.Config{
		AdminAddresses:   []string{adminBase},
		AdminVersion:     p.protocol,
		AccessToken:      token,
		AppName:          appName,
		Port:             port,
		Address:          "http://host.docker.internal:" + executorPort + "/",
		RegistryInterval: 5 * time.Second,
	}, xxljob.WithLogger(e2eLogger()))
	require.NoError(t, err)

	killGauge := &gauge{}
	var shard atomic.Value
	type okParams struct {
		N int `json:"n"`
	}
	exec.MustRegister("OK", xxljob.HandleJSON(func(ctx context.Context, _ *xxljob.Request, p okParams) error {
		for i := range p.N {
			xxljob.Logger(ctx).Info("step", "i", i)
		}
		xxljob.SetResult(ctx, fmt.Sprintf("handled n=%d", p.N))
		return nil
	}))
	exec.MustRegister("Fail", xxljob.HandlerFunc(func(context.Context, *xxljob.Request) error { return errors.New("business failed") }))
	exec.MustRegister("Panic", xxljob.HandlerFunc(func(context.Context, *xxljob.Request) error { panic("boom") }))
	exec.MustRegister("Slow", slow(&gauge{}))
	exec.MustRegister("KillMe", slow(killGauge))
	var busyMu sync.Mutex
	busyActive, busyOverlaps := map[int64]int{}, 0
	exec.MustRegister("Busy", xxljob.HandlerFunc(func(ctx context.Context, req *xxljob.Request) error {
		busyMu.Lock()
		busyActive[req.JobID]++
		if busyActive[req.JobID] > 1 {
			busyOverlaps++
		}
		busyMu.Unlock()
		defer func() {
			busyMu.Lock()
			busyActive[req.JobID]--
			busyMu.Unlock()
		}()
		select { // 0-1.5s: triggered every second, so a run may overlap the next trigger
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(rand.IntN(1500)) * time.Millisecond):
			return nil
		}
	}))
	exec.MustRegister("Shard", xxljob.HandlerFunc(func(_ context.Context, req *xxljob.Request) error {
		shard.Store(fmt.Sprintf("%d/%d", req.ShardIndex, req.ShardTotal))
		return nil
	}))
	require.NoError(t, exec.Start())
	shutdown := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return exec.Shutdown(ctx)
	}
	t.Cleanup(func() { _ = shutdown() })

	// The admin refreshes the executor's address list from the registry every 30 seconds.
	address := "http://host.docker.internal:" + executorPort + "/"
	require.Eventually(t, func() bool {
		return mysql(t, fmt.Sprintf("SELECT IFNULL(address_list,'') FROM xxl_job_group WHERE id=%d", group)) == address
	}, 70*time.Second, 2*time.Second, "executor never showed up in the admin")
	c := newConsole(t, p)

	t.Run("success with params, result message and execution log", func(t *testing.T) {
		job := createJob(t, p, group, "OK", jobOpts{})
		c.trigger(job, `{"n":3}`)
		l := waitLog(t, job, finished)

		content := c.logContent(l.ID)

		assert.Equal(t, 200, l.HandleCode)
		assert.Equal(t, "handled n=3", l.HandleMsg)
		assert.Equal(t, 3, strings.Count(content, "msg=step"))
		assert.Contains(t, content, "job succeeded")
	})

	t.Run("error and panic are failures", func(t *testing.T) {
		fail := createJob(t, p, group, "Fail", jobOpts{})
		pnc := createJob(t, p, group, "Panic", jobOpts{})
		c.trigger(fail, "")
		c.trigger(pnc, "")

		f, pn := waitLog(t, fail, finished), waitLog(t, pnc, finished)

		assert.Equal(t, [2]any{500, "business failed"}, [2]any{f.HandleCode, f.HandleMsg})
		assert.Equal(t, [2]any{500, "panic: boom"}, [2]any{pn.HandleCode, pn.HandleMsg})
	})

	t.Run("timeout is reported as 502", func(t *testing.T) {
		job := createJob(t, p, group, "Slow", jobOpts{timeout: 2})
		c.trigger(job, "")

		l := waitLog(t, job, finished)

		assert.Equal(t, 502, l.HandleCode)
		assert.Equal(t, "xxljob: job execution timeout: context deadline exceeded", l.HandleMsg)
	})

	t.Run("kill, then trigger again without concurrency", func(t *testing.T) {
		job := createJob(t, p, group, "KillMe", jobOpts{})
		c.trigger(job, "")
		first := waitLog(t, job, func(l jobLog) bool { return l.TriggerCode == 200 })
		time.Sleep(300 * time.Millisecond)

		killed := c.kill(first.ID)
		c.trigger(job, "")
		second := waitLog(t, job, func(l jobLog) bool { return l.ID != first.ID && l.TriggerCode > 0 })
		busy := postExecutor(t, "/idleBeat", map[string]string{"XXL-JOB-ACCESS-TOKEN": token, "XXL-JOB-APPNAME": appName},
			fmt.Sprintf(`{"jobId":%d}`, job))
		time.Sleep(time.Second)
		c.kill(second.ID)
		firstDone := waitLog(t, job, func(l jobLog) bool { return l.ID == first.ID && l.HandleCode > 0 })

		assert.Equal(t, 200, killed.Code, killed.Msg)
		assert.Equal(t, 200, second.TriggerCode, "trigger after kill is queued, not rejected")
		assert.Contains(t, busy, "job thread is running or has trigger queue")
		assert.Equal(t, 500, firstDone.HandleCode)
		assert.Equal(t, int32(1), killGauge.max.Load(), "killed run and next run must never overlap")
	})

	t.Run("discard later rejects a second trigger", func(t *testing.T) {
		job := createJob(t, p, group, "Slow", jobOpts{block: xxljob.BlockDiscardLater})
		c.trigger(job, "")
		first := waitLog(t, job, func(l jobLog) bool { return l.TriggerCode == 200 })
		time.Sleep(300 * time.Millisecond)

		c.trigger(job, "")
		second := waitLog(t, job, func(l jobLog) bool { return l.ID != first.ID && l.TriggerCode > 0 })
		c.kill(first.ID)

		assert.Equal(t, 500, second.TriggerCode)
		assert.Contains(t, second.TriggerMsg, "DISCARD_LATER")
	})

	t.Run("cover early cancels the running trigger", func(t *testing.T) {
		job := createJob(t, p, group, "Slow", jobOpts{block: xxljob.BlockCoverEarly})
		c.trigger(job, "")
		first := waitLog(t, job, func(l jobLog) bool { return l.TriggerCode == 200 })
		time.Sleep(300 * time.Millisecond)

		c.trigger(job, "")
		firstDone := waitLog(t, job, func(l jobLog) bool { return l.ID == first.ID && l.HandleCode > 0 })
		second := waitLog(t, job, func(l jobLog) bool { return l.ID != first.ID && l.TriggerCode == 200 })
		c.kill(second.ID)

		assert.Equal(t, 500, firstDone.HandleCode)
		assert.Equal(t, "xxljob: job covered by a newer trigger: context canceled", firstDone.HandleMsg)
	})

	t.Run("sharding broadcast params", func(t *testing.T) {
		job := createJob(t, p, group, "Shard", jobOpts{route: "SHARDING_BROADCAST"})
		c.trigger(job, "")

		l := waitLog(t, job, finished)

		assert.Equal(t, 200, l.HandleCode)
		assert.Equal(t, "0/1", shard.Load())
	})

	t.Run("many jobs scheduled concurrently by cron", func(t *testing.T) {
		// The admin itself triggers 8 jobs every second via cron (mixing the three block strategies); run times are random
		// and overlap with the next trigger.
		strategies := []string{xxljob.BlockSerial, xxljob.BlockDiscardLater, xxljob.BlockCoverEarly}
		ids := make([]string, 8)
		blockOf := map[string]string{}
		for i := range ids {
			ids[i] = strconv.Itoa(createJob(t, p, group, "Busy", jobOpts{block: strategies[i%3], cron: "* * * * * ?"}))
			blockOf[ids[i]] = strategies[i%3]
		}
		in := strings.Join(ids, ",")
		time.Sleep(15 * time.Second)
		mysql(t, "UPDATE xxl_job_info SET trigger_status=0 WHERE id IN ("+in+")")

		// After the jobs are stopped, the admin may still fire a few already prefetched triggers: wait until every
		// successful trigger has a result and nothing changes for a while.
		pending := func() string {
			return mysql(t, "SELECT COUNT(*) FROM xxl_job_log WHERE job_id IN ("+in+") AND trigger_code=200 AND handle_code=0")
		}
		if !assert.Eventually(t, func() bool { return pending() == "0" }, 120*time.Second, time.Second, "results never arrived") {
			t.Logf("triggers without a result:\n%s", mysql(t, "SELECT id, job_id, trigger_time, LEFT(IFNULL(trigger_msg,''),80) FROM xxl_job_log "+
				"WHERE job_id IN ("+in+") AND trigger_code=200 AND handle_code=0"))
			t.Logf("executor stats: %+v", exec.Stats())
			_ = pprof.Lookup("goroutine").WriteTo(os.Stderr, 1) // where the executor's goroutines are waiting
			t.FailNow()
		}
		time.Sleep(6 * time.Second)
		require.Eventually(t, func() bool { return pending() == "0" }, 30*time.Second, time.Second)
		rows := mysql(t, "SELECT job_id, handle_code, LEFT(IFNULL(handle_msg,''),60) FROM xxl_job_log WHERE job_id IN ("+in+") AND trigger_code=200")
		triggered := strings.Count(rows, "\n") + 1

		for _, line := range strings.Split(rows, "\n") {
			f := append(strings.SplitN(line, "\t", 3), "") // handle_msg is empty on success, and the trailing tab is trimmed
			require.GreaterOrEqual(t, len(f), 3, line)
			switch blockOf[f[0]] {
			case xxljob.BlockCoverEarly: // runs replaced by the next trigger fail as covered
				assert.True(t, f[1] == "200" || strings.Contains(f[2], "covered"), "job %s: %s %s", f[0], f[1], f[2])
			default:
				assert.Equal(t, "200", f[1], "job %s: %s", f[0], f[2])
			}
		}
		busyMu.Lock()
		overlaps := busyOverlaps
		busyMu.Unlock()
		assert.Zero(t, overlaps, "a job ran concurrently with itself")
		// The count only shows that the scenario produced real concurrent load: the admin's scheduler needs a few
		// seconds to warm up, so on a busy CI runner it fires well under one trigger per job per second.
		assert.GreaterOrEqual(t, triggered, 8*5, "too few triggers: %d", triggered)
		t.Logf("%d triggers from 8 cron jobs, all called back", triggered)
	})

	// Whether AccessToken is required depends on the admin version (optional for 2.x / 3.4, covered by the unit test
	// TestAuthentication); it is configured here, so verify that it is enforced.
	t.Run("with AccessToken configured, requests without it are rejected", func(t *testing.T) {
		res := postExecutor(t, p.runPath, map[string]string{"XXL-JOB-APPNAME": appName}, `{"jobId":1,"logId":999999,"executorHandler":"OK"}`)

		assert.Contains(t, res, "The access token is wrong.")
	})

	t.Run("shutdown deregisters the executor", func(t *testing.T) {
		before := exec.Stats()
		err := shutdown()

		require.NoError(t, err)
		after := exec.Stats()
		assert.True(t, before.Registered, "registered with the real admin before shutdown")
		assert.False(t, after.Registered)
		assert.Equal(t, after.Accepted, after.Succeeded+after.Failed+after.TimedOut, "every accepted trigger has an outcome")
		assert.Equal(t, "0", mysql(t, "SELECT COUNT(*) FROM xxl_job_registry WHERE registry_key='"+appName+"'"))
	})
}

// e2eLogger writes the executor's warnings and errors to stderr, so a failure can be diagnosed from the test output;
// XXLJOB_E2E_VERBOSE=1 includes every level.
func e2eLogger() *slog.Logger {
	level := slog.LevelWarn
	if os.Getenv("XXLJOB_E2E_VERBOSE") != "" {
		level = slog.LevelDebug
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
}
