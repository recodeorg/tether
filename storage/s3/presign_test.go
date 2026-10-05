package s3

import (
	"testing"
	"time"
)

func TestDownloadPresignTTL(t *testing.T) {
	now := time.Date(2026, 10, 5, 1, 0, 0, 0, time.UTC)

	if got := downloadPresignTTL(time.Time{}, false, now); got != 16*time.Minute {
		t.Fatalf("missing expiry = %s, want 16m", got)
	}

	expiresAt := now.Add(2 * time.Hour)
	if got := downloadPresignTTL(expiresAt, true, now); got != 2*time.Hour+time.Minute {
		t.Fatalf("token expiry = %s, want 2h1m", got)
	}

	if got := downloadPresignTTL(now.Add(time.Minute), true, now); got != 2*time.Minute {
		t.Fatalf("short token = %s, want 2m", got)
	}
}
