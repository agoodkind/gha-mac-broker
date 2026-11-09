package ghapp

// jitConfigRequest is the body of generate-jitconfig.
type jitConfigRequest struct {
	Name          string   `json:"name"`
	RunnerGroupID int64    `json:"runner_group_id"`
	Labels        []string `json:"labels"`
	WorkFolder    string   `json:"work_folder"`
}

// runnerInfo identifies the registered ephemeral runner.
type runnerInfo struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

// Runner stores one GitHub repository runner.
type Runner struct {
	// ID is the GitHub runner id used for deletion.
	ID int64 `json:"id"`
	// Name is the registered runner name.
	Name string `json:"name"`
	// Status is the GitHub runner status, usually "online" or "offline".
	Status string `json:"status"`
	// Busy reports whether GitHub currently has a job assigned to the runner.
	Busy bool `json:"busy"`
}

type runnerListResponse struct {
	TotalCount int      `json:"total_count"`
	Runners    []Runner `json:"runners"`
}

// inProgressRunsResponse is the relevant subset of
// GET /repos/{owner}/{repo}/actions/runs?status=in_progress.
type inProgressRunsResponse struct {
	TotalCount   int `json:"total_count"`
	WorkflowRuns []struct {
		ID int64 `json:"id"`
	} `json:"workflow_runs"`
}

// runsByHeadResponse is the relevant subset of
// GET /repos/{owner}/{repo}/actions/runs?head_sha={sha}.
type runsByHeadResponse struct {
	TotalCount   int `json:"total_count"`
	WorkflowRuns []struct {
		ID     int64  `json:"id"`
		Status string `json:"status"`
	} `json:"workflow_runs"`
}

// runJobsResponse is the relevant subset of
// GET /repos/{owner}/{repo}/actions/runs/{runID}/jobs.
type runJobsResponse struct {
	TotalCount int `json:"total_count"`
	Jobs       []struct {
		ID     int64    `json:"id"`
		Status string   `json:"status"`
		Labels []string `json:"labels"`
	} `json:"jobs"`
}

// JITConfig stores the configuration for one generated runner.
type JITConfig struct {
	// EncodedJITConfig supplies the runner's --jitconfig argument.
	EncodedJITConfig string     `json:"encoded_jit_config"`
	Runner           runnerInfo `json:"runner"`
}
