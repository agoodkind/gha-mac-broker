// Package reservation tracks pre-flight capacity promises from the /capacity
// endpoint so the webhook handler can confirm that a CI planner already
// verified slot availability before committing a warm VM to a job.
package reservation

import (
	"sync"
	"time"
)

// defaultTTL is how long a reservation is held before it expires.
const defaultTTL = 5 * time.Minute

// entry records when a reservation was made.
type entry struct {
	reservedAt time.Time
}

// Store holds active reservations keyed by run_id. The now field is
// overridable in white-box tests; the default is [time.Now].
type Store struct {
	mu  sync.Mutex
	m   map[string]entry
	now func() time.Time
	ttl time.Duration
}

// New returns a Store with a 5-minute TTL and [time.Now] as the clock.
func New() *Store {
	return &Store{
		mu:  sync.Mutex{},
		m:   make(map[string]entry),
		now: time.Now,
		ttl: defaultTTL,
	}
}

// Reserve records a reservation for runID if there is capacity. capacity is
// typically pool.FreeSlots(). It returns false if the number of outstanding
// (non-expired) reservations already meets or exceeds capacity, or if runID
// already has a live reservation.
func (s *Store) Reserve(runID string, capacity int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sweep()
	if len(s.m) >= capacity {
		return false
	}
	if _, ok := s.m[runID]; ok {
		return false
	}
	s.m[runID] = entry{reservedAt: s.now()}
	return true
}

// Consume removes and returns whether a live (non-expired) reservation exists
// for runID. It returns false if runID was never reserved or the reservation
// has expired.
func (s *Store) Consume(runID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.m[runID]
	if !ok {
		return false
	}
	if s.now().Sub(e.reservedAt) > s.ttl {
		delete(s.m, runID)
		return false
	}
	delete(s.m, runID)
	return true
}

// sweep removes expired entries. It must be called with s.mu held.
func (s *Store) sweep() {
	now := s.now()
	for id, e := range s.m {
		if now.Sub(e.reservedAt) > s.ttl {
			delete(s.m, id)
		}
	}
}
