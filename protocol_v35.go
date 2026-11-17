package xxljob

import (
	"encoding/json"
	"net/http"
)

// protocolV35 is the protocol of XXL-JOB admin 3.5.0 and later:
//   - runs are triggered on /trigger;
//   - authentication uses both XXL-JOB-ACCESS-TOKEN and XXL-JOB-APPNAME; the token is configured per executor in
//     the admin and is required;
//   - the log request carries the time in logDateTime;
//   - the callback body is an object {"callbackList": [{logId, logDateTime, handleCode, handleMsg}]};
//   - responses are {code, msg, data}.
type protocolV35 struct {
	token   string
	appName string
}

func (protocolV35) runPath() string { return "/trigger" }

func (p protocolV35) authenticate(h http.Header) error {
	if !tokenMatches(h.Get(tokenHeader), p.token) {
		return errWrongToken
	}
	if h.Get(appNameHeader) != p.appName {
		return errWrongAppName // the admin sends the token and app name of the target executor: a mismatch means a misrouted request
	}
	return nil
}

func (protocolV35) decodeLogQuery(body []byte) (logQuery, error) {
	var req struct {
		LogDateTime int64 `json:"logDateTime"`
		LogID       int64 `json:"logId"`
		FromLineNum int   `json:"fromLineNum"`
	}
	err := json.Unmarshal(body, &req)
	return logQuery{LogID: req.LogID, LogTime: req.LogDateTime, FromLine: req.FromLineNum}, err
}

func (protocolV35) response(r reply) any {
	return struct {
		Code int    `json:"code"`
		Msg  string `json:"msg,omitempty"`
		Data any    `json:"data,omitempty"`
	}{r.code, r.msg, r.payload}
}

func (p protocolV35) setHeaders(h http.Header) {
	h.Set(tokenHeader, p.token)
	h.Set(appNameHeader, p.appName)
}

func (protocolV35) callbackBody(results []result) any {
	type item struct {
		LogID       int64  `json:"logId"`
		LogDateTime int64  `json:"logDateTime"`
		HandleCode  int    `json:"handleCode"`
		HandleMsg   string `json:"handleMsg"`
	}
	items := make([]item, len(results))
	for i, r := range results {
		items[i] = item{LogID: r.LogID, LogDateTime: r.LogTime, HandleCode: r.Code, HandleMsg: r.Msg}
	}
	return struct {
		CallbackList []item `json:"callbackList"`
	}{items}
}
