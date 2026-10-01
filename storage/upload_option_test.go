package storage

import (
	"testing"
	"time"
)

func TestEffectiveUploadLimits(t *testing.T) {
	maxBytes, expiresIn, public := EffectiveUploadLimits(nil, nil)
	if maxBytes != defaultMaxBytes || expiresIn != defaultExpiresIn || public != defaultPublic {
		t.Fatalf("built-in defaults = %d, %s", maxBytes, expiresIn)
	}

	maxBytes, expiresIn, public = EffectiveUploadLimits(
		[]UploadOption{WithMaxBytes(1234), WithExpiresIn(time.Hour)},
		nil,
	)
	if maxBytes != 1234 || expiresIn != time.Hour || public != defaultPublic {
		t.Fatalf("defaults = %d, %s", maxBytes, expiresIn)
	}

	maxBytes, expiresIn, public = EffectiveUploadLimits(
		[]UploadOption{WithMaxBytes(1234), WithExpiresIn(time.Hour)},
		[]UploadOption{WithMaxBytes(99), nil},
	)
	if maxBytes != 99 || expiresIn != time.Hour || public != defaultPublic {
		t.Fatalf("call options = %d, %s; want 99 and 1h", maxBytes, expiresIn)
	}

	maxBytes, expiresIn, public = EffectiveUploadLimits(
		[]UploadOption{WithMaxBytes(1234)},
		[]UploadOption{WithMaxBytes(0)},
	)
	if maxBytes != defaultMaxBytes {
		t.Fatalf("zero call option = %d, want built-in default", maxBytes)
	}
}
