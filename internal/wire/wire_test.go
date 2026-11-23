package wire

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func headers(kv ...string) http.Header {
	h := http.Header{}
	for i := 0; i+1 < len(kv); i += 2 {
		h.Set(kv[i], kv[i+1])
	}
	return h
}

func TestNew(t *testing.T) {
	// Scenario: the protocol is chosen from Config.AdminVersion
	// Expect: "2" and "3.5" are supported; 3.5 requires a token; other versions are rejected with a hint
	v2p, err := New(Version2, "", "app")
	require.NoError(t, err)
	_, err35 := New(Version35, "", "app")
	_, errBad := New("4", "t", "app")

	assert.Equal(t, "/run", v2p.RunPath())
	require.ErrorContains(t, err35, "AccessToken is required")
	require.ErrorContains(t, errBad, `unsupported AdminVersion "4"`)
}

func TestAuthenticate(t *testing.T) {
	// Scenario: requests from the admin are authenticated per protocol version
	// Expect: the official executor's behaviour and wording for every case
	cases := []struct {
		name, version, token string
		h                    http.Header
		want                 string // "" means accepted
	}{
		{"v2 no token configured", Version2, "", headers(), ""},
		{"v2 right token", Version2, "s3cret", headers(tokenHeader, "s3cret"), ""},
		{"v2 wrong token", Version2, "s3cret", headers(tokenHeader, "nope"), "The access token is wrong."},
		{"v2 missing token", Version2, "s3cret", headers(), "The access token is wrong."},
		{"v2 token prefix", Version2, "s3cret", headers(tokenHeader, "s3cre"), "The access token is wrong."},
		{"v35 right", Version35, "s3cret", headers(tokenHeader, "s3cret", appNameHeader, "app"), ""},
		{"v35 missing appname", Version35, "s3cret", headers(tokenHeader, "s3cret"), "invalid request, accessToken or appname is empty."},
		{"v35 blank token", Version35, "s3cret", headers(tokenHeader, " ", appNameHeader, "app"), "invalid request, accessToken or appname is empty."},
		{"v35 wrong token", Version35, "s3cret", headers(tokenHeader, "nope", appNameHeader, "app"), "invalid request, accessToken or appname invalid."},
		{"v35 wrong appname", Version35, "s3cret", headers(tokenHeader, "s3cret", appNameHeader, "other"), "invalid request, accessToken or appname invalid."},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p, err := New(c.version, c.token, "app")
			require.NoError(t, err)

			got := p.Authenticate(c.h)

			if c.want == "" {
				assert.NoError(t, got)
			} else {
				assert.EqualError(t, got, c.want)
			}
		})
	}
}

func TestEncoding(t *testing.T) {
	// Scenario: replies, callbacks, log requests and outbound headers are encoded per protocol version
	// Expect: v2 puts the payload in content and data (2.3 – 3.4), names the time logDateTim, sends callbacks as an
	//         array and always sends the token header (admin 3.3.0/3.3.1 require it); v3.5 uses data,
	//         logDateTime, {"callbackList": [...]} and sends the token and AppName
	results := []Result{{LogID: 7, LogTime: 1700000000000, Code: CodeTimeout, Msg: "slow"}}
	payload := LogResult{FromLineNum: 1, ToLineNum: 2, LogContent: "a\nb\n", IsEnd: true}
	cases := []struct {
		version, token, reply, callback, logBody string
		headers                                  http.Header
	}{
		{
			version:  Version2,
			reply:    `{"code":200,"content":{"fromLineNum":1,"toLineNum":2,"logContent":"a\nb\n","isEnd":true},"data":{"fromLineNum":1,"toLineNum":2,"logContent":"a\nb\n","isEnd":true}}`,
			callback: `[{"logId":7,"logDateTim":1700000000000,"handleCode":502,"handleMsg":"slow"}]`,
			logBody:  `{"logDateTim":1700000000000,"logId":7,"fromLineNum":3}`,
			headers:  headers(tokenHeader, ""),
		},
		{
			version:  Version35,
			token:    "s3cret",
			reply:    `{"code":200,"data":{"fromLineNum":1,"toLineNum":2,"logContent":"a\nb\n","isEnd":true}}`,
			callback: `{"callbackList":[{"logId":7,"logDateTime":1700000000000,"handleCode":502,"handleMsg":"slow"}]}`,
			logBody:  `{"logDateTime":1700000000000,"logId":7,"fromLineNum":3}`,
			headers:  headers(tokenHeader, "s3cret", appNameHeader, "app"),
		},
	}
	for _, c := range cases {
		t.Run(c.version, func(t *testing.T) {
			p, err := New(c.version, c.token, "app")
			require.NoError(t, err)
			reply, err := json.Marshal(p.EncodeReply(Reply{Code: CodeSuccess, Payload: payload}))
			require.NoError(t, err)
			callback, err := json.Marshal(p.CallbackBody(results))
			require.NoError(t, err)
			logReq, err := p.DecodeLogRequest([]byte(c.logBody))
			require.NoError(t, err)
			h := http.Header{}
			p.SetHeaders(h)

			assert.JSONEq(t, c.reply, string(reply))
			assert.JSONEq(t, c.callback, string(callback))
			assert.Equal(t, LogRequest{LogID: 7, LogTime: 1700000000000, FromLine: 3}, logReq)
			assert.Equal(t, c.headers, h)
		})
	}
}

func TestEncodeFailure(t *testing.T) {
	// Scenario: a failure reply carries only code and message
	p, err := New(Version2, "", "app")
	require.NoError(t, err)

	b, err := json.Marshal(p.EncodeReply(Fail("job handler [%s] not found.", "X")))

	require.NoError(t, err)
	assert.JSONEq(t, `{"code":500,"msg":"job handler [X] not found."}`, string(b))
}

func TestNotFound(t *testing.T) {
	// Scenario: a 3.5 admin calls /trigger on an executor configured for the 2.x protocol
	// Expect: the official wording plus a hint at Config.AdminVersion
	p, err := New(Version2, "", "app")
	require.NoError(t, err)

	r := NotFound("/trigger", p)

	assert.Equal(t, CodeFail, r.Code)
	assert.Equal(t, "invalid request, uri-mapping(/trigger) not found. (this executor serves /run: check Config.AdminVersion)", r.Msg)
}

func TestTruncateMsg(t *testing.T) {
	// Scenario: result messages are capped like the official executor (50000 characters, then "...")
	// Expect: short messages are unchanged; long ones keep exactly MaxResultMsg runes and stay valid UTF-8
	long := strings.Repeat("错", MaxResultMsg+10)

	got := TruncateMsg(long)

	assert.Equal(t, "ok", TruncateMsg("ok"))
	assert.Equal(t, strings.Repeat("x", MaxResultMsg), TruncateMsg(strings.Repeat("x", MaxResultMsg)))
	assert.Equal(t, MaxResultMsg+3, utf8.RuneCountInString(got))
	assert.True(t, strings.HasSuffix(got, "错..."))
	assert.True(t, utf8.ValidString(got))
}
