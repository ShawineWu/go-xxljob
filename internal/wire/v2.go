package wire

import (
	"encoding/json"
	"errors"
	"net/http"
)

// errWrongToken is the official 2.x executor's answer to a request with a wrong token.
var errWrongToken = errors.New("The access token is wrong.") //nolint:staticcheck // ST1005: official wording

// v2 is the protocol of admin 2.3.0 – 3.4.x:
//   - runs are triggered on /run;
//   - requests carry the admin's global token in XXL-JOB-ACCESS-TOKEN; without a configured token nothing is checked;
//   - the log request names its time field logDateTim;
//   - callbacks are an array [{logId, logDateTim, handleCode, handleMsg}];
//   - replies are {code, msg, content}. Since 3.3 the admin reads data instead, so the payload goes in both.
type v2 struct {
	token string
}

func (v2) RunPath() string { return "/run" }

func (p v2) Authenticate(h http.Header) error {
	if p.token != "" && !tokenEqual(h.Get(tokenHeader), p.token) {
		return errWrongToken
	}
	return nil
}

func (v2) DecodeLogRequest(body []byte) (LogRequest, error) {
	var req struct {
		LogDateTim  int64 `json:"logDateTim"`
		LogID       int64 `json:"logId"`
		FromLineNum int   `json:"fromLineNum"`
	}
	err := json.Unmarshal(body, &req)
	return LogRequest{LogID: req.LogID, LogTime: req.LogDateTim, FromLine: req.FromLineNum}, err
}

func (v2) EncodeReply(r Reply) any {
	return struct {
		Code    int    `json:"code"`
		Msg     string `json:"msg,omitempty"`
		Content any    `json:"content,omitempty"` // 2.3 – 3.2 (ReturnT)
		Data    any    `json:"data,omitempty"`    // 3.3 – 3.4 (Response)
	}{r.Code, r.Msg, r.Payload, r.Payload}
}

// SetHeaders always sends the token header, even when it is empty: admin 3.3.0 and 3.3.1 declare it as a required
// header and reject requests without it, like the official executor does.
func (p v2) SetHeaders(h http.Header) { h.Set(tokenHeader, p.token) }

func (v2) CallbackBody(results []Result) any {
	type item struct {
		LogID      int64  `json:"logId"`
		LogDateTim int64  `json:"logDateTim"`
		HandleCode int    `json:"handleCode"`
		HandleMsg  string `json:"handleMsg"`
	}
	items := make([]item, len(results))
	for i, r := range results {
		items[i] = item{r.LogID, r.LogTime, r.Code, r.Msg}
	}
	return items
}
