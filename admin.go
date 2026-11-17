package xxljob

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// maxResponseBytes caps how much of an admin response is read.
const maxResponseBytes = 1 << 20

// adminClient talks to the admin. Addresses are tried in order (admins share one database, so any of them will
// do); every request has its own timeout.
type adminClient struct {
	addrs   []string
	proto   protocol
	client  *http.Client
	timeout time.Duration
}

func newAdminClient(c Config, proto protocol, client *http.Client) *adminClient {
	if client == nil {
		client = &http.Client{}
	}
	return &adminClient{addrs: c.AdminAddresses, proto: proto, client: client, timeout: c.RequestTimeout}
}

// callback reports a batch of results; the body format depends on the protocol version.
func (a *adminClient) callback(ctx context.Context, batch []result) error {
	return a.post(ctx, "callback", a.proto.callbackBody(batch))
}

// post calls /api/{action}. A response code other than 200 is an error. If every address fails, the errors of all
// addresses are joined.
func (a *adminClient) post(ctx context.Context, action string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("xxljob: marshal %s: %w", action, err)
	}
	var errs []error
	for _, addr := range a.addrs {
		err := a.postOne(ctx, addr+"/api/"+action, body)
		if err == nil {
			return nil
		}
		errs = append(errs, err)
		if ctx.Err() != nil {
			break
		}
	}
	return errors.Join(errs...)
}

func (a *adminClient) postOne(ctx context.Context, url string, body []byte) error {
	ctx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("xxljob: build request %s: %w", url, err)
	}
	req.Header.Set("Content-Type", "application/json;charset=UTF-8")
	a.proto.setHeaders(req.Header)
	resp, err := a.client.Do(req)
	if err != nil {
		return fmt.Errorf("xxljob: post %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return fmt.Errorf("xxljob: read response of %s: %w", url, err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("xxljob: post %s: http status %d: %.200s", url, resp.StatusCode, data)
	}
	var rt struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	}
	if err := json.Unmarshal(data, &rt); err != nil {
		return fmt.Errorf("xxljob: post %s: invalid response %.200q: %w", url, data, err)
	}
	if rt.Code != codeSuccess {
		return fmt.Errorf("xxljob: post %s: code %d: %s", url, rt.Code, rt.Msg)
	}
	return nil
}
