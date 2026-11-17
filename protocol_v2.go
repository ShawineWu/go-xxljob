package xxljob

import (
	"encoding/json"
	"net/http"
)

// protocolV2 is the protocol of XXL-JOB admin 2.3.0 ~ 3.4.x:
//   - runs are triggered on /run;
//   - authentication uses the XXL-JOB-ACCESS-TOKEN header with the admin's global token, and is skipped when no
//     token is configured;
//   - the log request carries the time in logDateTim;
//   - the callback body is an array [{logId, logDateTim, handleCode, handleMsg}];
//   - responses are {code, msg, content}. Since 3.3 the admin reads data instead (ReturnT became Response while
//     the rest of the protocol stayed the same), so the payload is put in both content and data and one
//     implementation covers the whole 2.3 ~ 3.4 range.
type protocolV2 struct {
	token string
}

func (protocolV2) runPath() string { return "/run" }

func (p protocolV2) authenticate(h http.Header) error {
	if p.token != "" && !tokenMatches(h.Get(tokenHeader), p.token) {
		return errWrongToken
	}
	return nil
}

func (protocolV2) decodeLogQuery(body []byte) (logQuery, error) {
	var req struct {
		LogDateTim  int64 `json:"logDateTim"`
		LogID       int64 `json:"logId"`
		FromLineNum int   `json:"fromLineNum"`
	}
	err := json.Unmarshal(body, &req)
	return logQuery{LogID: req.LogID, LogTime: req.LogDateTim, FromLine: req.FromLineNum}, err
}

func (protocolV2) response(r reply) any {
	return struct {
		Code    int    `json:"code"`
		Msg     string `json:"msg,omitempty"`
		Content any    `json:"content,omitempty"` // 2.3 ~ 3.2 (ReturnT)
		Data    any    `json:"data,omitempty"`    // 3.3 ~ 3.4 (Response)
	}{r.code, r.msg, r.payload, r.payload}
}

func (p protocolV2) setHeaders(h http.Header) {
	if p.token != "" {
		h.Set(tokenHeader, p.token)
	}
}

func (protocolV2) callbackBody(results []result) any {
	type item struct {
		LogID      int64  `json:"logId"`
		LogDateTim int64  `json:"logDateTim"`
		HandleCode int    `json:"handleCode"`
		HandleMsg  string `json:"handleMsg"`
	}
	items := make([]item, len(results))
	for i, r := range results {
		items[i] = item{LogID: r.LogID, LogDateTim: r.LogTime, HandleCode: r.Code, HandleMsg: r.Msg}
	}
	return items
}
