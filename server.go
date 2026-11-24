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

// Limits on requests from the admin, whose largest field (the job parameters) is at most 512 characters in its
// database.
const (
	maxRequestBytes = 64 << 10
	maxHeaderBytes  = 64 << 10
)

// maxLogReads bounds the /log requests served at once: a read holds a run's whole log (up to its cap) in memory a
// few times over. Further requests wait for a turn.
const maxLogReads = 4

// HTTPHandler returns the HTTP handler of the executor port: the run path (/run, or /trigger for admin 3.5+), /kill,
// /beat, /idleBeat and /log. The executor serves it on Config.Port; with WithoutServer, mount it on your own server
// (use http.StripPrefix for a sub-path) before calling Start. Like the official executor it always answers with
// HTTP 200 and a JSON body whose code tells success from failure. Every call returns the same handler.
func (e *Executor) HTTPHandler() http.Handler { return e.mux }

// newMux builds the handler returned by HTTPHandler.
func (e *Executor) newMux() http.Handler {
	mux := http.NewServeMux()
	route := func(path string, fn func(body []byte) wire.Reply) {
		mux.Handle("POST "+path, e.endpoint(fn))
	}
	route(e.proto.RunPath(), func(body []byte) wire.Reply {
		var req wire.TriggerRequest
		if err := json.Unmarshal(body, &req); err != nil {
			e.rejected.Add(1)
			return wire.Fail("invalid run request: %v", err)
		}
		return e.trigger(req)
	})
	route(wire.PathKill, func(body []byte) wire.Reply {
		var req wire.JobRequest
		if err := json.Unmarshal(body, &req); err != nil {
			return wire.Fail("invalid kill request: %v", err)
		}
		if !e.sched.Kill(req.JobID) {
			return wire.Reply{Code: CodeSuccess, Msg: wire.MsgAlreadyKilled}
		}
		return wire.OK()
	})
	route(wire.PathBeat, func([]byte) wire.Reply { return e.beat() })
	route(wire.PathIdleBeat, func(body []byte) wire.Reply {
		var req wire.JobRequest
		if err := json.Unmarshal(body, &req); err != nil {
			return wire.Fail("invalid idleBeat request: %v", err)
		}
		if r := e.beat(); r.Code != CodeSuccess {
			return r
		}
		if e.sched.Busy(req.JobID) {
			return wire.Fail("%s", wire.MsgBusy)
		}
		return wire.OK()
	})
	route(wire.PathLog, func(body []byte) wire.Reply {
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
		w.Header().Set("Content-Type", wire.ContentType)
		_ = json.NewEncoder(w).Encode(e.proto.EncodeReply(res))
	})
}

// beat is the liveness probe. It fails before Start and during shutdown, so failover routing sends triggers to
// other instances.
func (e *Executor) beat() wire.Reply {
	if err := e.life.accepting(); err != nil {
		return wire.Fail("%s", err)
	}
	return wire.OK()
}

// readLog reads an execution log for the admin's log viewer.
func (e *Executor) readLog(q wire.LogRequest) wire.Reply {
	e.logReads <- struct{}{}
	defer func() { <-e.logReads }()
	chunk, err := e.readStore(LogRef{LogID: q.LogID, LogTime: time.UnixMilli(q.LogTime)}, q.FromLine)
	if errors.Is(err, ErrLogNotFound) {
		return wire.LogNotice(q.FromLine, fmt.Sprintf("log not found on this executor (log_id=%d): it may have been "+
			"cleaned up, the run happened on another instance, or the executor restarted", q.LogID))
	}
	if err != nil {
		e.log.Error("xxljob: read log", "log_id", q.LogID, "err", err)
		return wire.Fail("read log failed")
	}
	return wire.Reply{Code: CodeSuccess, Payload: wire.LogResult{
		FromLineNum: chunk.FromLine, ToLineNum: chunk.ToLine, LogContent: chunk.Content, IsEnd: chunk.End}}
}

// readStore reads from the log store, which may be the application's own, turning a panic into an error.
func (e *Executor) readStore(ref LogRef, fromLine int) (chunk LogChunk, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("log store Read panicked: %v", p)
		}
	}()
	return e.logs.Read(ref, fromLine)
}
