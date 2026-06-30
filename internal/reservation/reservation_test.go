package reservation

import (
	"testing"
	"time"
)

func testTime() time.Time {
	return time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC)
}

func fixedClock(t time.Time) func() time.Time {
	return func() time.Time { return t }
}

func TestReserveWithinCapacitySucceeds(t *testing.T) {
	s := New()
	now := testTime()
	s.now = fixedClock(now)

	if !s.Reserve("run-1", "image-a", 2) {
		t.Fatal("first reserve should succeed within capacity 2")
	}
	if !s.Reserve("run-2", "image-b", 2) {
		t.Fatal("second reserve should succeed within capacity 2")
	}
	if s.Reserve("run-3", "image-c", 2) {
		t.Fatal("third reserve beyond capacity 2 should fail")
	}
}

func TestReserveDuplicateReturnsFalse(t *testing.T) {
	s := New()
	s.now = fixedClock(testTime())

	if !s.Reserve("run-1", "image-a", 5) {
		t.Fatal("first reserve should succeed")
	}
	if s.Reserve("run-1", "image-a", 5) {
		t.Fatal("duplicate reserve for same run_id should fail")
	}
}

func TestConsumeTrueOnceThenFalse(t *testing.T) {
	s := New()
	s.now = fixedClock(testTime())

	s.Reserve("run-1", "image-a", 1)
	image, ok := s.Consume("run-1")
	if !ok {
		t.Fatal("first consume should return true")
	}
	if image != "image-a" {
		t.Fatalf("consume image = %q, want %q", image, "image-a")
	}
	if _, ok := s.Consume("run-1"); ok {
		t.Fatal("second consume should return false")
	}
}

func TestConsumeNeverReservedReturnsFalse(t *testing.T) {
	s := New()
	if _, ok := s.Consume("run-999"); ok {
		t.Fatal("consuming unknown run_id should return false")
	}
}

func TestExpiredReservationNotConsumable(t *testing.T) {
	s := New()
	base := testTime()
	s.now = fixedClock(base)

	s.Reserve("run-1", "image-a", 1)

	// advance clock past TTL
	s.now = fixedClock(base.Add(defaultTTL + time.Second))

	if _, ok := s.Consume("run-1"); ok {
		t.Fatal("expired reservation should not be consumable")
	}
}

func TestExpiredReservationFreesCapacity(t *testing.T) {
	s := New()
	base := testTime()
	s.now = fixedClock(base)

	s.Reserve("run-1", "image-a", 1)

	// capacity full; new reserve fails
	if s.Reserve("run-2", "image-b", 1) {
		t.Fatal("should fail when capacity is full")
	}

	// advance past TTL so sweep evicts run-1
	s.now = fixedClock(base.Add(defaultTTL + time.Second))

	// now there is capacity again
	if !s.Reserve("run-2", "image-b", 1) {
		t.Fatal("should succeed after expired entry is swept")
	}
}
