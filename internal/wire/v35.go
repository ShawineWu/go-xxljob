package wire

import (
	"encoding/json"
	"errors"
	"net/http"
)

// The official 3.5 executor's answers to unauthenticated requests.
var (
	errAuthMissing = errors.New("invalid request, accessToken or appname is empty.") //nolint:staticcheck // ST1005: official wording
	errAuthInvalid = errors.New("invalid request, accessToken or appname invalid.")  //nolint:staticcheck // ST1005: official wording
)

// v35 is the protocol of admin 3.5.0 and later:
//   - runs are triggered on /trigger;
//   - requests carry the executor's own token (configured per executor in the admin, required) and its AppName;
//   - the log request names its time field logDateTime;
//   - callbacks are an object {"callbackList": [{logId, logDateTime, handleCode, handleMsg}]};
//   - replies are {code, msg, data}.
type v35 struct {
	token   string
	appName string
}

func (v35) RunPath() string { return "/trigger" }

func (p v35) Authenticate(h http.Header) error {
	token, app := h.Get(tokenHeader), h.Get(appNameHeader)
	if blank(token) || blank(app) {
		return errAuthMissing
	}
	if !tokenEqual(token, p.token) || app != p.appName {
		return errAuthInvalid
	}
	return nil
}

func (v35) DecodeLogRequest(body []byte) (LogRequest, error) {
	var req struct {
		LogDateTime int64 `json:"logDateTime"`
		LogID       int64 `json:"logId"`
		FromLineNum int   `json:"fromLineNum"`
	}
	err := json.Unmarshal(body, &req)
	return LogRequest{LogID: req.LogID, LogTime: req.LogDateTime, FromLine: req.FromLineNum}, err
}

func (v35) EncodeReply(r Reply) any {
	return struct {
		Code int    `json:"code"`
		Msg  string `json:"msg,omitempty"`
		Data any    `json:"data,omitempty"`
	}{r.Code, r.Msg, r.Payload}
}

func (p v35) SetHeaders(h http.Header) {
	h.Set(tokenHeader, p.token)
	h.Set(appNameHeader, p.appName)
}

func (v35) CallbackBody(results []Result) any {
	type item struct {
		LogID       int64  `json:"logId"`
		LogDateTime int64  `json:"logDateTime"`
		HandleCode  int    `json:"handleCode"`
		HandleMsg   string `json:"handleMsg"`
	}
	items := make([]item, len(results))
	for i, r := range results {
		items[i] = item{r.LogID, r.LogTime, r.Code, r.Msg}
	}
	return struct {
		CallbackList []item `json:"callbackList"`
	}{items}
}
