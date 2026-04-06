package check

import (
	"github.com/storacha/guppy/pkg/preparation/types/id"
)

// CheckReport contains the results of checking one or more uploads.
type CheckReport struct {
	UploadID       id.UploadID
	Checks         []CheckResult
	OverallPass    bool
	RepairsApplied int
}

// CheckResult represents the result of a single check.
type CheckResult struct {
	Name    string   // e.g., "Upload Scanned Check"
	Passed  bool     // Overall pass/fail for this check
	Issues  []Issue  // Problems found
	Repairs []Repair // Repairs attempted
}

// Issue represents a problem found during a check.
type Issue struct {
	Type        IssueType // Error or Warning
	Description string    // Short description of the issue
	Details     string    // Additional context (optional)
}

// IssueType indicates the severity of an issue.
type IssueType string

const (
	IssueTypeError   IssueType = "error"
	IssueTypeWarning IssueType = "warning"
)

// Repair represents an attempted fix for an issue.
type Repair struct {
	Description string // What was done
	Applied     bool   // Whether the repair was successfully applied
	Error       error  // Error if repair failed
}
