package xxljob

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/ShawineWu/go-xxljob/internal/wire"
)

// maxRequestBytes caps the body of a request from the admin. Job parameters are at most 512 characters in the
// admin's database.
const maxRequestBytes = 256 << 10

// Handler returns the HTTP handler of the executor port: the run path (/run, or /trigger for admin 3.5+), /kill,
// /beat, /idleBeat and /log. The executor serves it on Config.Port; with WithoutServer, mount it on your own server
// (use http.StripPrefix for a sub-path) before calling Start. Like the official executor it always answers with
// HTTP 200 and a JSON body whose code tells success from failure.
func (e *Executor) Handler() http.Handler {
	mux := http.NewServeMux()
	route := func(path string, fn func(body []byte) wire.Reply) {
		mux.Handle("POST "+path, e.endpoint(fn))
	}
	route(e.proto.RunPath(), func(body []byte) wire.Reply {
		var req wire.TriggerRequest
		if err := json.Unmarshal(body, &req); err != nil {
			return wire.Fail("invalid run request: %v", err)
		}
		return e.trigger(req)
	})
	route("/kill", func(body []byte) wire.Reply {
		var req wire.JobRequest
		if err := json.Unmarshal(body, &req); err != nil {
			return wire.Fail("invalid kill request: %v", err)
		}
		if !e.sched.Kill(req.JobID) {
			return wire.Reply{Code: CodeSuccess, Msg: "job thread already killed."}
		}
		return wire.OK()
	})
	route("/beat", func([]byte) wire.Reply { return e.beat() })
	route("/idleBeat", func(body []byte) wire.Reply {
		var req wire.JobRequest
		if err := json.Unmarshal(body, &req); err != nil {
			return wire.Fail("invalid idleBeat request: %v", err)
		}
		if r := e.beat(); r.Code != CodeSuccess {
			return r
		}
		if e.sched.Busy(req.JobID) {
			return wire.Fail("job thread is running or has trigger queue.")
		}
		return wire.OK()
	})
	route("/log", func(body []byte) wire.Reply {
		q, err := e.proto.DecodeLogRequest(body)
		if err != nil {
			return wire.Fail("invalid log request: %v", err)
		}
		return e.readLog(q)
	})
	mux.Handle("/", e.endpoint(nil)) // any other method or path
	return mux
}

// endpoint checks the method, authenticates the request and reads its body before calling fn; nil fn answers with
// "not found". Nothing runs for a request that fails authentication.
func (e *Executor) endpoint(fn func(body []byte) wire.Reply) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var res wire.Reply
		switch err := e.proto.Authenticate(r.Header); {
		case r.Method != http.MethodPost:
			res = wire.Fail("%s", wire.ErrMethodNotAllowed)
		case err != nil:
			if ok, n := e.rejectLog.Allow(); ok {
				e.log.Warn("xxljob: rejected unauthenticated request", "path", r.URL.Path, "remote", r.RemoteAddr,
					"reason", err.Error(), "suppressed", n)
			}
			res = wire.Fail("%s", err)
		case fn == nil:
			res = wire.NotFound(r.URL.Path, e.proto)
		default:
			body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBytes))
			if err != nil {
				res = wire.Fail("invalid request body: %v", err)
			} else {
				res = fn(body)
			}
		}
		w.Header().Set("Content-Type", "application/json;charset=UTF-8")
		_ = json.NewEncoder(w).Encode(e.proto.EncodeReply(res))
	})
}

// beat is the liveness probe. It fails before Start and during shutdown, so failover routing sends triggers to
// other instances.
func (e *Executor) beat() wire.Reply {
	switch e.life.current() {
	case stateNew:
		return wire.Fail("executor is not started")
	case stateClosing:
		return wire.Fail("%s", ErrShutdown)
	}
	return wire.OK()
}

// readLog reads an execution log for the admin's log viewer.
func (e *Executor) readLog(q wire.LogRequest) wire.Reply {
	chunk, err := e.logs.Read(LogRef{LogID: q.LogID, LogTime: time.UnixMilli(q.LogTime)}, q.FromLine)
	if errors.Is(err, ErrLogNotFound) {
		// The viewer only shows content when toLineNum >= fromLineNum.
		from := max(q.FromLine, 1)
		return wire.Reply{Code: CodeSuccess, Payload: wire.LogResult{FromLineNum: from, ToLineNum: from, IsEnd: true,
			LogContent: fmt.Sprintf("log not found on this executor (log_id=%d): it may have been cleaned up, "+
				"the run happened on another instance, or the executor restarted", q.LogID)}}
	}
	if err != nil {
		e.log.Error("xxljob: read log", "log_id", q.LogID, "err", err)
		return wire.Fail("read log failed")
	}
	return wire.Reply{Code: CodeSuccess, Payload: wire.LogResult{
		FromLineNum: chunk.FromLine, ToLineNum: chunk.ToLine, LogContent: chunk.Content, IsEnd: chunk.End}}
}
