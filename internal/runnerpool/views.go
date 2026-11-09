package runnerpool

import (
	"time"
)

// Job identifies one queued GitHub Actions workflow job.
type Job struct {
	Repo  string
	JobID int64
	RunID int64
}

// Options configures a persistent worker pool.
type Options struct {
	RunnerCount    int
	JobsPerVM      int
	Image          string
	MaxIdle        time.Duration
	MaxAge         time.Duration
	MaxBind        time.Duration
	PickupTimeout  time.Duration
	RunToken       string
	WarmRetryDelay time.Duration
	Now            func() time.Time
}

// Snapshot stores pool readiness and queue counts.
type Snapshot struct {
	RunnerCount int  `json:"runner_count"`
	Idle        int  `json:"idle"`
	Busy        int  `json:"busy"`
	Queued      int  `json:"queued"`
	Healthy     bool `json:"healthy"`
	Ready       bool `json:"ready"`
}

// WorkerView stores the status of one worker and its slots.
type WorkerView struct {
	Index          int        `json:"index"`
	VM             string     `json:"vm"`
	Phase          string     `json:"phase"`
	RunID          int64      `json:"run_id"`
	BindAgeSeconds int64      `json:"bind_age_seconds"`
	ActiveJob      *bool      `json:"active_job"`
	LastError      string     `json:"last_error"`
	Slots          []SlotView `json:"slots,omitempty"`
}

// SlotView stores the status of one runner slot in a VM.
type SlotView struct {
	Index          int    `json:"index"`
	Phase          string `json:"phase"`
	RunID          int64  `json:"run_id"`
	JobID          int64  `json:"job_id"`
	BindAgeSeconds int64  `json:"bind_age_seconds"`
	ActiveJob      *bool  `json:"active_job"`
	LastError      string `json:"last_error"`
}
