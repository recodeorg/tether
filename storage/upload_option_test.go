package storage

import (
	"testing"
	"time"
)

func TestEffectiveUploadLimits(t *testing.T) {
	maxBytes, expiresIn, _, public, _ := EffectiveUploadLimits(nil, nil)
	if maxBytes != defaultMaxBytes || expiresIn != defaultExpiresIn || public != defaultPublic {
		t.Fatalf("built-in defaults = %d, %s", maxBytes, expiresIn)
	}

	maxBytes, expiresIn, _, public, _ = EffectiveUploadLimits(
		[]UploadOption{WithMaxBytes(1234), WithExpiresIn(time.Hour)},
		nil,
	)
	if maxBytes != 1234 || expiresIn != time.Hour || public != defaultPublic {
		t.Fatalf("defaults = %d, %s", maxBytes, expiresIn)
	}

	maxBytes, expiresIn, _, public, _ = EffectiveUploadLimits(
		[]UploadOption{WithMaxBytes(1234), WithExpiresIn(time.Hour)},
		[]UploadOption{WithMaxBytes(99), nil},
	)
	if maxBytes != 99 || expiresIn != time.Hour || public != defaultPublic {
		t.Fatalf("call options = %d, %s; want 99 and 1h", maxBytes, expiresIn)
	}

	maxBytes, expiresIn, _, public, _ = EffectiveUploadLimits(
		[]UploadOption{WithMaxBytes(1234)},
		[]UploadOption{WithMaxBytes(0)},
	)
	if maxBytes != defaultMaxBytes {
		t.Fatalf("zero call option = %d, want built-in default", maxBytes)
	}
}

func TestPublicUploadOption(t *testing.T) {
	_, _, _, public, _ := EffectiveUploadLimits(nil, []UploadOption{Public()})
	if !public {
		t.Fatal("Public() call option was not public")
	}

	_, _, _, public, _ = EffectiveUploadLimits([]UploadOption{Public()}, nil)
	if !public {
		t.Fatal("Public() default was not public")
	}

	maxBytes, _, _, public, _ := EffectiveUploadLimits(
		[]UploadOption{Public(), WithMaxBytes(5)},
		[]UploadOption{WithMaxBytes(9)},
	)
	if !public || maxBytes != 9 {
		t.Fatalf("public = %v, maxBytes = %d; want true and 9", public, maxBytes)
	}
}
