package xxljob

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// maxRequestBytes caps the body of a request from the admin.
const maxRequestBytes = 1 << 20

// Handler returns the HTTP handler of the executor port: the run path (/run or /trigger depending on the admin
// version), /kill, /beat, /idleBeat and /log. Start serves it on its own port. With WithoutServer, mount it on your
// own server (use http.StripPrefix for a sub-path) and set Config.Address accordingly.
func (e *Executor) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle(e.proto.runPath(), e.endpoint(func(body []byte) reply {
		var req runReq
		if err := json.Unmarshal(body, &req); err != nil {
			return failure("invalid run request")
		}
		return e.dispatch(req)
	}))
	mux.Handle("/kill", e.endpoint(func(body []byte) reply {
		var req killReq
		if err := json.Unmarshal(body, &req); err != nil {
			return failure("invalid kill request")
		}
		return e.kill(req.JobID)
	}))
	mux.Handle("/beat", e.endpoint(func([]byte) reply { return e.beat() }))
	mux.Handle("/idleBeat", e.endpoint(func(body []byte) reply {
		var req idleBeatReq
		if err := json.Unmarshal(body, &req); err != nil {
			return failure("invalid idleBeat request")
		}
		return e.idleBeat(req.JobID)
	}))
	mux.Handle("/log", e.endpoint(func(body []byte) reply {
		q, err := e.proto.decodeLogQuery(body)
		if err != nil {
			return failure("invalid log request")
		}
		return e.readLog(q)
	}))
	return mux
}

// endpoint authenticates the request (per protocol version), reads its body and writes the JSON response.
// Nothing runs when authentication fails.
func (e *Executor) endpoint(fn func(body []byte) reply) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var res reply
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			res = failure("method not allowed")
		} else if err := e.proto.authenticate(r.Header); err != nil {
			e.log.Warn("xxljob: rejected request", "path", r.URL.Path, "remote", r.RemoteAddr, "reason", err.Error())
			res = failure(err.Error())
		} else if body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBytes)); err != nil {
			res = failure("invalid request body")
		} else {
			res = fn(body)
		}
		w.Header().Set("Content-Type", "application/json;charset=UTF-8")
		_ = json.NewEncoder(w).Encode(e.proto.response(res))
	})
}

// beat is the liveness probe. It fails while shutting down so failover routing sends triggers elsewhere.
func (e *Executor) beat() reply {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state != stateRunning {
		return failure(ErrShutdown.Error())
	}
	return success("")
}

// idleBeat is the busy probe: a job with a running or queued trigger is busy.
func (e *Executor) idleBeat(jobID int64) reply {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.state != stateRunning {
		return failure(ErrShutdown.Error())
	}
	if e.threads[jobID] != nil {
		return failure("job thread is running or has trigger queue.")
	}
	return success("")
}

// readLog reads an execution log.
func (e *Executor) readLog(q logQuery) reply {
	chunk, err := e.logs.Read(LogRef{LogID: q.LogID, LogTime: time.UnixMilli(q.LogTime)}, q.FromLine)
	if errors.Is(err, ErrLogNotFound) {
		return reply{code: codeSuccess, payload: logResult{
			FromLineNum: q.FromLine,
			ToLineNum:   0,
			LogContent:  fmt.Sprintf("log not found on this executor (log_id=%d): it may have been cleaned up, or the executor restarted", q.LogID),
			IsEnd:       true,
		}}
	}
	if err != nil {
		e.log.Error("xxljob: read log", "log_id", q.LogID, "err", err)
		return failure("read log failed")
	}
	return reply{code: codeSuccess, payload: logResult{
		FromLineNum: chunk.FromLine,
		ToLineNum:   chunk.ToLine,
		LogContent:  chunk.Content,
		IsEnd:       chunk.End,
	}}
}
