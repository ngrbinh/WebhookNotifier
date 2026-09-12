package delivery

import (
	"errors"
	"net/http"
	"testing"
)

func TestClassify(t *testing.T) {
	cases := []struct {
		status       int
		requestError error
		expected     Outcome
	}{{http.StatusOK, nil, Success}, {http.StatusTooManyRequests, nil, Retryable}, {http.StatusBadGateway, nil, Retryable}, {http.StatusNotFound, nil, Permanent}, {0, errors.New("timeout"), Retryable}}
	for _, testCase := range cases {
		if actual := Classify(testCase.status, testCase.requestError); actual != testCase.expected {
			t.Fatalf("status %d: got %d, want %d", testCase.status, actual, testCase.expected)
		}
	}
}
