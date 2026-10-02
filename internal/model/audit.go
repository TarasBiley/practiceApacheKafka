package model

import "time"

type AuditRequest struct {
	UserID     string         `json:"user_id"`
	Action     string         `json:"action"`
	ResourceID string         `json:"resource_id"`
	Meta       map[string]any `json:"meta"`
}

type AuditEvent struct {
	EventID    string         `json:"event_id"`
	UserID     string         `json:"user_id"`
	Action     string         `json:"action"`
	ResourceID string         `json:"resource_id"`
	Meta       map[string]any `json:"meta"`
	Timestamp  time.Time      `json:"timestamp"`
}

type AuditResponse struct {
	EventID   string    `json:"event_id"`
	Timestamp time.Time `json:"timestamp"`
}

type AuditDeliveryError struct {
	EventID   string    `json:"event_id"`
	Timestamp time.Time `json:"timestamp"`
	Error     string    `json:"error"`
}
