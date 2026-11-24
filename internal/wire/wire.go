// Package wire implements the XXL-JOB executor protocol: the messages exchanged with the admin and the differences
// between protocol generations. It knows nothing about scheduling or HTTP serving.
//
// Admin → executor (POST {executor address}{path}, JSON body): run (/run, or /trigger since 3.5), /kill, /beat,
// /idleBeat, /log. Executor → admin (POST {admin address}/api/{action}): registry, registryRemove, callback.
package wire

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// Result codes understood by the admin.
const (
	CodeSuccess = 200
	CodeFail    = 500
	// CodeTimeout reports a run that exceeded its timeout (shown as "timeout" in the admin).
	CodeTimeout = 502
)

// Protocol versions.
const (
	// Version2 is the protocol of admin 2.3.0 – 3.4.x.
	Version2 = "2"
	// Version35 is the protocol of admin 3.5.0 and later.
	Version35 = "3.5"
)

const (
	tokenHeader   = "XXL-JOB-ACCESS-TOKEN" //nolint:gosec // G101: a header name, not a credential
	appNameHeader = "XXL-JOB-APPNAME"
)

// Paths the admin calls on the executor, besides the run path, which depends on the version (Protocol.RunPath).
const (
	PathKill     = "/kill"
	PathBeat     = "/beat"
	PathIdleBeat = "/idleBeat"
	PathLog      = "/log"
)

// Actions the executor calls on the admin (AdminURL).
const (
	ActionRegistry       = "registry"
	ActionRegistryRemove = "registryRemove"
	ActionCallback       = "callback"
)

// Messages of the official executor that the admin console shows as they are.
const (
	MsgAlreadyKilled = "job thread already killed."
	MsgBusy          = "job thread is running or has trigger queue."
)

// HandlerNotFound is the official executor's message for a trigger of an unknown handler.
func HandlerNotFound(name string) string { return "job handler [" + name + "] not found." }

// AdminURL is the URL of an action on the admin at addr (without a trailing slash).
func AdminURL(addr, action string) string { return addr + "/api/" + action }

// DecodeAdminReply checks the body of the admin's answer: an error unless its code is CodeSuccess.
func DecodeAdminReply(data []byte) error {
	var rt struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	}
	if err := json.Unmarshal(data, &rt); err != nil {
		return fmt.Errorf("invalid response %.200q: %w", data, err)
	}
	if rt.Code != CodeSuccess {
		return fmt.Errorf("code %d: %s", rt.Code, rt.Msg)
	}
	return nil
}

// Protocol is one generation of the executor protocol: everything that differs between admin versions.
type Protocol interface {
	// RunPath is the path the admin triggers runs on.
	RunPath() string
	// Authenticate checks the headers of a request from the admin. The error text is returned to the admin.
	Authenticate(h http.Header) error
	// DecodeLogRequest parses a request to read an execution log.
	DecodeLogRequest(body []byte) (LogRequest, error)
	// EncodeReply builds the response body for the admin.
	EncodeReply(r Reply) any
	// SetHeaders sets the headers of a request to the admin.
	SetHeaders(h http.Header)
	// CallbackBody builds the body of a callback request.
	CallbackBody(results []Result) any
}

// New returns the protocol for version ("2" or "3.5"). The 3.5 protocol requires a token.
func New(version, token, appName string) (Protocol, error) {
	switch version {
	case Version2:
		return v2{token: token}, nil
	case Version35:
		if token == "" {
			return nil, errors.New("AccessToken is required for xxl-job-admin 3.5+ (the executor's token in the admin console)")
		}
		return v35{token: token, appName: appName}, nil
	}
	return nil, fmt.Errorf("unsupported AdminVersion %q (use %q for 2.3.0 – 3.4.x or %q for 3.5.0+)", version, Version2, Version35)
}

// TriggerRequest asks the executor to run a job once.
type TriggerRequest struct {
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

// GlueBean is the only supported glue type: handlers compiled into the executor.
const GlueBean = "BEAN"

// JobRequest is the body of /kill and /idleBeat.
type JobRequest struct {
	JobID int64 `json:"jobId"`
}

// LogRequest asks for an execution log from line FromLine (1-based).
type LogRequest struct {
	LogID    int64
	LogTime  int64 // unix milliseconds, as sent by the admin
	FromLine int
}

// LogResult is the payload of a /log reply.
type LogResult struct {
	FromLineNum int    `json:"fromLineNum"`
	ToLineNum   int    `json:"toLineNum"`
	LogContent  string `json:"logContent"`
	IsEnd       bool   `json:"isEnd"`
}

// Reply is the executor's answer to an admin request.
type Reply struct {
	Code    int
	Msg     string
	Payload any // LogResult for /log
}

// OK is a successful reply.
func OK() Reply { return Reply{Code: CodeSuccess} }

// Fail is a failed reply with a message.
func Fail(format string, args ...any) Reply {
	return Reply{Code: CodeFail, Msg: fmt.Sprintf(format, args...)}
}

// Result is the outcome of one run, reported to the admin.
type Result struct {
	LogID   int64  `json:"logId"`
	LogTime int64  `json:"logTime"` // the log time sent by the admin, reported back unchanged
	Code    int    `json:"code"`
	Msg     string `json:"msg"`
}

// RegistryParam is the body of registry and registryRemove.
type RegistryParam struct {
	RegistryGroup string `json:"registryGroup"`
	RegistryKey   string `json:"registryKey"`
	RegistryValue string `json:"registryValue"`
}

// NewRegistryParam returns the registry body for an executor.
func NewRegistryParam(appName, address string) RegistryParam {
	return RegistryParam{RegistryGroup: "EXECUTOR", RegistryKey: appName, RegistryValue: address}
}

// MaxResultMsg caps the result message reported to the admin, in runes, like the official executor.
const MaxResultMsg = 50000

// TruncateMsg shortens msg to MaxResultMsg runes.
func TruncateMsg(msg string) string {
	if len(msg) <= MaxResultMsg { // bytes >= runes, so it already fits
		return msg
	}
	n := 0
	for i := range msg {
		if n == MaxResultMsg {
			return msg[:i] + "..."
		}
		n++
	}
	return msg
}

// Errors returned by the HTTP layer, with the official executor's wording.
var (
	ErrMethodNotAllowed = errors.New("invalid request, HttpMethod not support.") //nolint:staticcheck // ST1005: official wording
)

// NotFound is the reply for an unknown path, with the official wording and a hint at the usual cause.
func NotFound(path string, p Protocol) Reply {
	return Fail("invalid request, uri-mapping(%s) not found. (this executor serves %s: check Config.AdminVersion)", path, p.RunPath())
}

// tokenEqual compares in constant time, including the length (both sides are hashed first).
func tokenEqual(got, want string) bool {
	g, w := sha256.Sum256([]byte(got)), sha256.Sum256([]byte(want))
	return subtle.ConstantTimeCompare(g[:], w[:]) == 1
}

// blank reports whether a header value counts as absent.
func blank(s string) bool { return strings.TrimSpace(s) == "" }
