// Package delivery classifies partner responses and calculates retry backoff.
package delivery

import (
	"errors"
	"math/rand"
	"net/http"
	"time"
)

type Outcome int

const (
	Success Outcome = iota
	Retryable
	Permanent
)

// Classify maps an HTTP result or request error to a delivery outcome.
func Classify(statusCode int, requestError error) Outcome {
	if requestError != nil || statusCode == http.StatusTooManyRequests || statusCode >= 500 {
		return Retryable
	}
	if statusCode >= 200 && statusCode < 300 {
		return Success
	}
	return Permanent
}

// Backoff calculates an exponentially increasing delay with jitter for an attempt.
func Backoff(base time.Duration, attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	maximum := base
	for index := 1; index < attempt; index++ {
		maximum *= 2
	}
	jitter := time.Duration(rand.Int63n(int64(maximum/2 + 1)))
	return maximum/2 + jitter
}

// ErrorText returns the request error text or the HTTP status text when no error exists.
func ErrorText(requestError error, statusCode int) string {
	if requestError != nil {
		return requestError.Error()
	}
	return http.StatusText(statusCode)
}

var ErrTimeout = errors.New("delivery timeout")
