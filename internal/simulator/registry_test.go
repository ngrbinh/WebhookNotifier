package simulator

import "testing"

func TestValidateWebhook(t *testing.T) {
	tests := []struct {
		name        string
		eventTypes  []string
		percentages []int
		delay       int
		wantError   bool
	}{
		{name: "one event type", eventTypes: []string{"subscriber.created"}, percentages: []int{100, 0, 0, 0}, delay: 0},
		{name: "mixed response distribution", eventTypes: []string{"subscriber.created", "subscriber.added_to_segment", "subscriber.unsubscribed"}, percentages: []int{80, 5, 10, 5}, delay: 30},
		{name: "no event types", percentages: []int{100, 0, 0, 0}, wantError: true},
		{name: "duplicate event type", eventTypes: []string{"subscriber.created", "subscriber.created"}, percentages: []int{100, 0, 0, 0}, wantError: true},
		{name: "unsupported event type", eventTypes: []string{"subscriber.deleted"}, percentages: []int{100, 0, 0, 0}, wantError: true},
		{name: "negative response percentage", eventTypes: []string{"subscriber.created"}, percentages: []int{-1, 1, 0, 100}, wantError: true},
		{name: "response percentage total below 100", eventTypes: []string{"subscriber.created"}, percentages: []int{99, 0, 0, 0}, wantError: true},
		{name: "response percentage total above 100", eventTypes: []string{"subscriber.created"}, percentages: []int{100, 1, 0, 0}, wantError: true},
		{name: "invalid delay", eventTypes: []string{"subscriber.created"}, percentages: []int{100, 0, 0, 0}, delay: 30001, wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateWebhook(test.eventTypes, test.percentages[0], test.percentages[1], test.percentages[2], test.percentages[3], test.delay)
			if (err != nil) != test.wantError {
				t.Fatalf("validateWebhook() error = %v, wantError %v", err, test.wantError)
			}
		})
	}
}
