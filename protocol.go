package xxljob

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
)

// The XXL-JOB executor protocol.
//
// Admin → executor (POST {Address}{action}, JSON body):
//
//	run / trigger  run a job once (the path depends on the admin version)
//	kill           kill a running job
//	beat           liveness probe (used by the FAILOVER route strategy)
//	idleBeat       busy probe (used by the BUSYOVER route strategy)
//	log            read an execution log
//
// Executor → admin (POST {AdminAddress}/api/{action}):
//
//	registry        register / renew (the admin drops executors silent for 90s)
//	registryRemove  deregister
//	callback        report results (batched)
//
// Everything that differs between protocol generations (run path, auth headers, field names, response envelope,
// callback body) lives in the protocol implementations: protocol_v2.go (admin 2.3.0 ~ 3.4.x) and protocol_v35.go
// (admin 3.5.0+). The rest of the executor only deals with the neutral types in this file. Supporting a new
// generation means adding one implementation and registering it in newProtocol.

// Admin protocol versions (Config.AdminVersion).
const (
	// AdminVersion2 is the protocol of XXL-JOB admin 2.3.0 ~ 3.4.x (the default).
	AdminVersion2 = "2"
	// AdminVersion35 is the protocol of XXL-JOB admin 3.5.0 and later.
	AdminVersion35 = "3.5"
)

// protocol is one generation of the admin protocol: inbound paths, authentication and encoding, outbound headers
// and the callback body.
type protocol interface {
	// runPath returns the path the admin triggers runs on.
	runPath() string
	// authenticate checks a request from the admin; the error message is returned to the caller.
	authenticate(h http.Header) error
	// decodeLogQuery parses a request to read an execution log.
	decodeLogQuery(body []byte) (logQuery, error)
	// response builds the response body.
	response(r reply) any
	// setHeaders sets the headers of a request to the admin.
	setHeaders(h http.Header)
	// callbackBody builds the body of a callback request.
	callbackBody(results []result) any
}

// newProtocol picks the protocol for Config.AdminVersion and validates what that version requires.
func newProtocol(c Config) (protocol, error) {
	switch c.AdminVersion {
	case AdminVersion2:
		return protocolV2{token: c.AccessToken}, nil
	case AdminVersion35:
		if c.AccessToken == "" {
			return nil, errors.New("xxljob: AccessToken is required for xxl-job-admin 3.5+ (the executor's token in the admin console)")
		}
		return protocolV35{token: c.AccessToken, appName: c.AppName}, nil
	}
	return nil, fmt.Errorf("xxljob: unsupported AdminVersion %q (use %q for 2.3.0~3.4.x or %q for 3.5.0+)",
		c.AdminVersion, AdminVersion2, AdminVersion35)
}

// ---------------------------------------------------------------------------------------------
// Neutral types used inside the executor, independent of the protocol version
// ---------------------------------------------------------------------------------------------

const (
	codeSuccess = 200
	codeFail    = 500
	// codeTimeout reports a run that exceeded its timeout (shown as "timeout" in the admin).
	codeTimeout = 502

	registryGroupExecutor = "EXECUTOR"
	glueTypeBean          = "BEAN"

	tokenHeader   = "XXL-JOB-ACCESS-TOKEN" //nolint:gosec // G101: a header name, not a credential
	appNameHeader = "XXL-JOB-APPNAME"
)

// reply is the executor's response to an admin request.
type reply struct {
	code    int
	msg     string
	payload any // logResult when reading a log
}

func success(msg string) reply { return reply{code: codeSuccess, msg: msg} }
func failure(msg string) reply { return reply{code: codeFail, msg: msg} }

// result is the outcome of one run, reported back to the admin.
type result struct {
	LogID   int64
	LogTime int64 // the log time (ms) sent by the admin, reported back unchanged
	Code    int
	Msg     string
}

// logQuery is a request to read an execution log.
type logQuery struct {
	LogID    int64
	LogTime  int64
	FromLine int
}

// ---------------------------------------------------------------------------------------------
// Messages that are identical across protocol generations
// ---------------------------------------------------------------------------------------------

// Block strategies decide what happens to a new trigger while the previous run of the same job is still running
// (or queued). They are the values of Request.BlockStrategy.
const (
	// BlockSerial queues the trigger and runs it after the previous one (the default).
	BlockSerial = "SERIAL_EXECUTION"
	// BlockDiscardLater rejects the new trigger.
	BlockDiscardLater = "DISCARD_LATER"
	// BlockCoverEarly cancels the running trigger, discards queued ones, and runs the new one.
	BlockCoverEarly = "COVER_EARLY"
)

// runReq triggers one run.
type runReq struct {
	JobID                 int64  `json:"jobId"`
	ExecutorHandler       string `json:"executorHandler"`
	ExecutorParams        string `json:"executorParams"`
	ExecutorBlockStrategy string `json:"executorBlockStrategy"`
	ExecutorTimeout       int64  `json:"executorTimeout"` // seconds; applies when > 0
	LogID                 int64  `json:"logId"`
	LogDateTime           int64  `json:"logDateTime"` // unix milliseconds
	GlueType              string `json:"glueType"`
	GlueSource            string `json:"glueSource"`
	GlueUpdatetime        int64  `json:"glueUpdatetime"`
	BroadcastIndex        int    `json:"broadcastIndex"`
	BroadcastTotal        int    `json:"broadcastTotal"`
}

type killReq struct {
	JobID int64 `json:"jobId"`
}

type idleBeatReq struct {
	JobID int64 `json:"jobId"`
}

// logResult is the payload of a log response.
type logResult struct {
	FromLineNum int    `json:"fromLineNum"`
	ToLineNum   int    `json:"toLineNum"`
	LogContent  string `json:"logContent"`
	IsEnd       bool   `json:"isEnd"`
}

type registryParam struct {
	RegistryGroup string `json:"registryGroup"`
	RegistryKey   string `json:"registryKey"`
	RegistryValue string `json:"registryValue"`
}

// Authentication failure messages. They are returned to the admin as-is and shown in its console, so they match
// the official executor's wording (capitalized, ending with a period).
var (
	errWrongToken   = errors.New("The access token is wrong.") //nolint:staticcheck // ST1005: matches the official executor
	errWrongAppName = errors.New("The appname is wrong.")      //nolint:staticcheck // ST1005: matches the official executor
)

// tokenMatches compares in constant time so the token cannot be guessed from response timing.
func tokenMatches(got, want string) bool {
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}
