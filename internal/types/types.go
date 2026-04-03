package types

import "time"

// RunningDevice describes the live container/pod state for one device.
type RunningDevice struct {
	Serial          string    `json:"serial"`
	AppiumPort      int       `json:"appium_port"`
	AppiumStartedAt time.Time `json:"appium_started_at"`
	TestStartedAt   time.Time `json:"test_started_at"`
	HasTest         bool      `json:"has_test"`
}
