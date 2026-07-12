package guestagent

import (
	"context"
	"errors"
	"testing"

	"connectrpc.com/connect"
	"goodkind.io/gha-mac-broker/internal/guestproto"
)

// fakeSlotConfigurer records the requested slot count and returns a scripted
// applied count or error, so the handler contract can be tested without a real
// supervisor.
type fakeSlotConfigurer struct {
	applied uint32
	err     error
	got     uint32
	calls   int
}

func (f *fakeSlotConfigurer) ConfigureSlots(_ context.Context, slotCount uint32) (uint32, error) {
	f.calls++
	f.got = slotCount
	if f.err != nil {
		return 0, f.err
	}
	return f.applied, nil
}

// TestConfigureSlotsAppliesRequestedCount proves the handler forwards the
// requested count to the configurer and echoes the applied inventory covering
// indices 0..N-1, which is what the host gates on.
func TestConfigureSlotsAppliesRequestedCount(t *testing.T) {
	configurer := &fakeSlotConfigurer{applied: 2}
	handler := New(nil, Options{SlotCount: 1, SlotConfigurer: configurer})

	response, err := handler.ConfigureSlots(
		context.Background(),
		connect.NewRequest(&guestproto.ConfigureSlotsRequest{SlotCount: 2}),
	)
	if err != nil {
		t.Fatalf("ConfigureSlots: %v", err)
	}
	if configurer.got != 2 {
		t.Fatalf("configurer got %d, want 2", configurer.got)
	}
	if response.Msg.GetSlotCount() != 2 {
		t.Fatalf("applied slot count = %d, want 2", response.Msg.GetSlotCount())
	}
	indices := make(map[uint32]struct{}, len(response.Msg.GetSlots()))
	for _, slot := range response.Msg.GetSlots() {
		indices[slot.GetIndex()] = struct{}{}
	}
	for want := uint32(0); want < 2; want++ {
		if _, ok := indices[want]; !ok {
			t.Fatalf("response slots %v missing index %d", indices, want)
		}
	}
}

// TestConfigureSlotsRejectsZero proves a zero slot count is an invalid argument
// and never reaches the configurer.
func TestConfigureSlotsRejectsZero(t *testing.T) {
	configurer := &fakeSlotConfigurer{applied: 1}
	handler := New(nil, Options{SlotCount: 1, SlotConfigurer: configurer})

	_, err := handler.ConfigureSlots(
		context.Background(),
		connect.NewRequest(&guestproto.ConfigureSlotsRequest{SlotCount: 0}),
	)
	if err == nil {
		t.Fatal("ConfigureSlots(0) = nil, want invalid argument")
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("ConfigureSlots(0) code = %v, want InvalidArgument", connect.CodeOf(err))
	}
	if configurer.calls != 0 {
		t.Fatalf("configurer calls = %d, want 0 for a rejected zero count", configurer.calls)
	}
}

// TestConfigureSlotsWithoutConfigurerUnimplemented proves a single-process guest
// agent without a supervisor refuses slot reconfiguration.
func TestConfigureSlotsWithoutConfigurerUnimplemented(t *testing.T) {
	handler := New(nil, Options{SlotCount: 1})

	_, err := handler.ConfigureSlots(
		context.Background(),
		connect.NewRequest(&guestproto.ConfigureSlotsRequest{SlotCount: 2}),
	)
	if err == nil {
		t.Fatal("ConfigureSlots without a configurer = nil, want unimplemented")
	}
	if connect.CodeOf(err) != connect.CodeUnimplemented {
		t.Fatalf("code = %v, want Unimplemented", connect.CodeOf(err))
	}
}

// TestConfigureSlotsSurfacesRejection proves a supervisor rejection (a shrink
// below a running slot) surfaces as a failed precondition, so the host leaves
// the VM as is rather than forcing it.
func TestConfigureSlotsSurfacesRejection(t *testing.T) {
	configurer := &fakeSlotConfigurer{err: errors.New("slot 1 is running")}
	handler := New(nil, Options{SlotCount: 2, SlotConfigurer: configurer})

	_, err := handler.ConfigureSlots(
		context.Background(),
		connect.NewRequest(&guestproto.ConfigureSlotsRequest{SlotCount: 1}),
	)
	if err == nil {
		t.Fatal("ConfigureSlots shrink-below-busy = nil, want rejection")
	}
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("code = %v, want FailedPrecondition", connect.CodeOf(err))
	}
}
