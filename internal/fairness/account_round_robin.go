// Package fairness contains account-ordering and queue-capacity controls for dispatch.
package fairness

import "sync"

// AccountRoundRobin rotates the first account in each dispatch pass.
type AccountRoundRobin struct {
	mutex  sync.Mutex
	cursor int
}

// OrderAccounts returns accounts in a rotating round-robin order.
func (roundRobin *AccountRoundRobin) OrderAccounts(accounts []string) []string {
	roundRobin.mutex.Lock()
	defer roundRobin.mutex.Unlock()
	if len(accounts) == 0 {
		return nil
	}
	ordered := make([]string, 0, len(accounts))
	start := roundRobin.cursor % len(accounts)
	for index := 0; index < len(accounts); index++ {
		ordered = append(ordered, accounts[(start+index)%len(accounts)])
	}
	roundRobin.cursor = (start + 1) % len(accounts)
	return ordered
}

// WatermarkGate applies hysteresis to queue backpressure decisions.
type WatermarkGate struct {
	paused bool
}

// Update records the queue depth and reports whether dispatch should pause.
func (gate *WatermarkGate) Update(depth, lowWatermark, highWatermark int) bool {
	if gate.paused {
		if depth < lowWatermark {
			gate.paused = false
		}
		return gate.paused
	}
	if depth >= highWatermark {
		gate.paused = true
	}
	return gate.paused
}
