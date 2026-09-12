package fairness

import "testing"

func TestAccountRoundRobinRotatesTheFirstAccount(t *testing.T) {
	roundRobin := &AccountRoundRobin{}
	accounts := []string{"account-a", "account-b", "account-c"}

	first := roundRobin.OrderAccounts(accounts)
	second := roundRobin.OrderAccounts(accounts)

	if first[0] != "account-a" || second[0] != "account-b" {
		t.Fatalf("round-robin order = %v then %v", first, second)
	}
}

func TestWatermarkGateUsesLowWatermarkToResume(t *testing.T) {
	gate := &WatermarkGate{}

	if gate.Update(10, 5, 10) != true {
		t.Fatal("expected dispatch to pause at the high watermark")
	}
	if gate.Update(7, 5, 10) != true {
		t.Fatal("expected dispatch to remain paused between watermarks")
	}
	if gate.Update(5, 5, 10) != true {
		t.Fatal("expected dispatch to remain paused at the low watermark")
	}
	if gate.Update(4, 5, 10) != false {
		t.Fatal("expected dispatch to resume below the low watermark")
	}
}
