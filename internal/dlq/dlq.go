package dlq

import (
	"strings"
	"time"
)

type Entry struct {
	OriginID     string    `json:"origin_id"`
	Cap          string    `json:"cap"`
	CrashCount   int       `json:"crash_count"`
	LastWorkerID string    `json:"last_worker_id"`
	DeclaredAt   time.Time `json:"declared_at"`
	PayloadPath  string    `json:"payload_path,omitempty"`
}

type Backend interface {
	Write(entry Entry) error
}

type Nop struct{}

func (Nop) Write(Entry) error { return nil }

var _ Backend = Nop{}
var _ Backend = (*File)(nil)

func sanitize(s string) string {
	return strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, s)
}
