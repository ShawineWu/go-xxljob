package xxljob

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
)

// handlerSet holds the registered handlers and middlewares. Middleware chains are built outside its lock, so a
// middleware constructor can do anything, including calling the Executor.
type handlerSet struct {
	mu     sync.RWMutex
	byName map[string]registered
	global []Middleware
}

type registered struct {
	h   Handler
	mws []Middleware
}

func newHandlerSet() *handlerSet {
	return &handlerSet{byName: map[string]registered{}}
}

// add registers handlers atomically: either all of them or, on error, none.
func (s *handlerSet) add(entries map[string]registered) error {
	for name, r := range entries {
		if name == "" {
			return errors.New("xxljob: empty handler name")
		}
		if r.h == nil {
			return fmt.Errorf("xxljob: nil handler %q", name)
		}
		if err := checkMiddlewares(r.mws); err != nil {
			return fmt.Errorf("%w (handler %q)", err, name)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for name := range entries {
		if _, dup := s.byName[name]; dup {
			return fmt.Errorf("xxljob: duplicate handler %q", name)
		}
	}
	maps.Copy(s.byName, entries)
	return nil
}

func (s *handlerSet) has(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.byName[name]
	return ok
}

func (s *handlerSet) use(mws []Middleware) error {
	if err := checkMiddlewares(mws); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.global = append(s.global, mws...)
	return nil
}

// resolve returns the handler for name wrapped in the global and its own middlewares, outermost first. A
// middleware constructor that panics fails the resolution instead of the caller.
func (s *handlerSet) resolve(name string) (h Handler, err error) {
	s.mu.RLock()
	r, ok := s.byName[name]
	mws := slices.Concat(s.global, r.mws)
	s.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("job handler [%s] not found.", name) //nolint:staticcheck // ST1005: official wording
	}
	defer func() {
		if p := recover(); p != nil {
			h, err = nil, fmt.Errorf("xxljob: middleware for handler %q panicked: %s", name, fmt.Sprint(p))
		}
	}()
	h = r.h
	for _, mw := range slices.Backward(mws) {
		if h = mw(h); h == nil {
			return nil, fmt.Errorf("xxljob: a middleware returned a nil handler for %q", name)
		}
	}
	return h, nil
}

func checkMiddlewares(mws []Middleware) error {
	if slices.ContainsFunc(mws, func(m Middleware) bool { return m == nil }) {
		return errors.New("xxljob: nil middleware")
	}
	return nil
}
