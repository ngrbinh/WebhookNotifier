package simulator

import "testing"

func TestValidateWebhook(t *testing.T) {
	tests := []struct {
		name       string
		eventTypes []string
		status     int
		delay      int
		wantError  bool
	}{
		{name: "one event type", eventTypes: []string{"subscriber.created"}, status: 200, delay: 0},
		{name: "three event types", eventTypes: []string{"subscriber.created", "subscriber.added_to_segment", "subscriber.unsubscribed"}, status: 500, delay: 30},
		{name: "no event types", status: 200, wantError: true},
		{name: "duplicate event type", eventTypes: []string{"subscriber.created", "subscriber.created"}, status: 200, wantError: true},
		{name: "unsupported event type", eventTypes: []string{"subscriber.deleted"}, status: 200, wantError: true},
		{name: "invalid status", eventTypes: []string{"subscriber.created"}, status: 99, wantError: true},
		{name: "invalid delay", eventTypes: []string{"subscriber.created"}, status: 200, delay: 30001, wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateWebhook(test.eventTypes, test.status, test.delay)
			if (err != nil) != test.wantError {
				t.Fatalf("validateWebhook() error = %v, wantError %v", err, test.wantError)
			}
		})
	}
}