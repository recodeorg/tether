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

func TestDownloadUploadOptions(t *testing.T) {
	_, _, downloadExpiresIn, _, cache := EffectiveUploadLimits(nil, nil)
	if downloadExpiresIn != defaultDownloadExpiresIn || cache != defaultUseCachedURLs {
		t.Fatalf("download defaults = %s, cache %v", downloadExpiresIn, cache)
	}

	_, _, downloadExpiresIn, _, cache = EffectiveUploadLimits(
		[]UploadOption{WithDownloadExpiresIn(time.Hour), UseCachedURLs()},
		nil,
	)
	if downloadExpiresIn != time.Hour || !cache {
		t.Fatalf("defaults = %s, cache %v; want 1h and true", downloadExpiresIn, cache)
	}

	_, _, downloadExpiresIn, _, cache = EffectiveUploadLimits(
		[]UploadOption{WithDownloadExpiresIn(time.Hour), UseCachedURLs()},
		[]UploadOption{WithDownloadExpiresIn(30 * time.Minute), nil},
	)
	if downloadExpiresIn != 30*time.Minute || !cache {
		t.Fatalf("call options = %s, cache %v; want 30m and true", downloadExpiresIn, cache)
	}

	_, _, downloadExpiresIn, _, cache = EffectiveUploadLimits(
		[]UploadOption{WithDownloadExpiresIn(time.Hour), UseCachedURLs()},
		[]UploadOption{WithDownloadExpiresIn(0)},
	)
	if downloadExpiresIn != defaultDownloadExpiresIn || !cache {
		t.Fatalf("zero lifetime = %s, cache %v; want built-in default and cache still on", downloadExpiresIn, cache)
	}

	_, _, _, _, cache = EffectiveUploadLimits(nil, []UploadOption{UseCachedURLs()})
	if !cache {
		t.Fatal("UseCachedURLs() call option did not enable caching")
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
