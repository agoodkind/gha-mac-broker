package reservation

import (
	"testing"
	"time"
)

func fixedClock(t time.Time) func() time.Time {
	return func() time.Time { return t }
}

func TestReserveWithinCapacitySucceeds(t *testing.T) {
	s := New()
	now := time.Now()
	s.now = fixedClock(now)

	if !s.Reserve("run-1", 2) {
		t.Fatal("first reserve should succeed within capacity 2")
	}
	if !s.Reserve("run-2", 2) {
		t.Fatal("second reserve should succeed within capacity 2")
	}
	if s.Reserve("run-3", 2) {
		t.Fatal("third reserve beyond capacity 2 should fail")
	}
}

func TestReserveDuplicateReturnsFalse(t *testing.T) {
	s := New()
	s.now = fixedClock(time.Now())

	if !s.Reserve("run-1", 5) {
		t.Fatal("first reserve should succeed")
	}
	if s.Reserve("run-1", 5) {
		t.Fatal("duplicate reserve for same run_id should fail")
	}
}

func TestConsumeTrueOnceThenFalse(t *testing.T) {
	s := New()
	s.now = fixedClock(time.Now())

	s.Reserve("run-1", 1)
	if !s.Consume("run-1") {
		t.Fatal("first consume should return true")
	}
	if s.Consume("run-1") {
		t.Fatal("second consume should return false")
	}
}

func TestConsumeNeverReservedReturnsFalse(t *testing.T) {
	s := New()
	if s.Consume("run-999") {
		t.Fatal("consuming unknown run_id should return false")
	}
}

func TestExpiredReservationNotConsumable(t *testing.T) {
	s := New()
	base := time.Now()
	s.now = fixedClock(base)

	s.Reserve("run-1", 1)

	// advance clock past TTL
	s.now = fixedClock(base.Add(defaultTTL + time.Second))

	if s.Consume("run-1") {
		t.Fatal("expired reservation should not be consumable")
	}
}

func TestExpiredReservationFreesCapacity(t *testing.T) {
	s := New()
	base := time.Now()
	s.now = fixedClock(base)

	s.Reserve("run-1", 1)

	// capacity full; new reserve fails
	if s.Reserve("run-2", 1) {
		t.Fatal("should fail when capacity is full")
	}

	// advance past TTL so sweep evicts run-1
	s.now = fixedClock(base.Add(defaultTTL + time.Second))

	// now there is capacity again
	if !s.Reserve("run-2", 1) {
		t.Fatal("should succeed after expired entry is swept")
	}
}
