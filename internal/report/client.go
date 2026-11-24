// Package report talks to the admin: registration heartbeats, deregistration and result callbacks.
package report

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sync/atomic"
	"time"

	"github.com/ShawineWu/go-xxljob/internal/wire"
)

// maxResponseBytes caps how much of an admin response is read.
const maxResponseBytes = 1 << 20

// Client posts to the admin's /api endpoints. With several admin addresses it starts with the one that answered
// last, so a dead address costs one timeout instead of one per request. It never follows redirects: they would
// carry the access token to another host.
type Client struct {
	addrs   []string
	proto   wire.Protocol
	http    *http.Client
	timeout time.Duration
	next    atomic.Int64 // index of the address to try first
}

// NewClient returns a client. hc may be nil; it is copied, never modified.
func NewClient(addrs []string, proto wire.Protocol, hc *http.Client, timeout time.Duration) *Client {
	c := http.Client{}
	if hc != nil {
		c = *hc //nolint:govet // copying a Client is safe; it holds no locks
	}
	c.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{addrs: slices.Clone(addrs), proto: proto, http: &c, timeout: timeout}
}

// Post calls an action on the admin (wire.Action*). A response code other than 200 is an error. It tries every address once, starting with
// the last one that worked, and joins the errors if all fail.
func (c *Client) Post(ctx context.Context, action string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", action, err)
	}
	start := int(c.next.Load())
	var errs []error
	for i := range c.addrs {
		idx := (start + i) % len(c.addrs)
		err := c.post(ctx, wire.AdminURL(c.addrs[idx], action), body)
		if err == nil {
			c.next.Store(int64(idx))
			return nil
		}
		errs = append(errs, err)
		if ctx.Err() != nil {
			break
		}
	}
	return errors.Join(errs...)
}

func (c *Client) post(ctx context.Context, url string, body []byte) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request %s: %w", url, err)
	}
	req.Header.Set("Content-Type", "application/json;charset=UTF-8")
	c.proto.SetHeaders(req.Header)
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("post %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return fmt.Errorf("read response of %s: %w", url, err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("post %s: http status %d: %.200s", url, resp.StatusCode, data)
	}
	if err := wire.DecodeAdminReply(data); err != nil {
		return fmt.Errorf("post %s: %w", url, err)
	}
	return nil
}
