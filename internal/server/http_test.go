package server_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"goodkind.io/gha-mac-broker/internal/server"
)

func TestServeReturnsDistinctUnwrappedPanicErrors(t *testing.T) {
	first := server.Serve(
		context.Background(),
		nil,
		http.NewServeMux(),
		nil,
		time.Second,
		time.Second,
	)
	if first == nil {
		t.Fatal("Serve did not return the recovered listener panic")
	}

	if !strings.HasPrefix(first.Error(), "panic: ") {
		t.Fatalf("Serve changed the panic error prefix: %v", first)
	}

	if errors.Unwrap(first) != nil {
		t.Fatal("Serve added an error chain to the recovered panic")
	}

	second := server.Serve(
		context.Background(),
		nil,
		http.NewServeMux(),
		nil,
		time.Second,
		time.Second,
	)
	if second == nil {
		t.Fatal("Serve did not return the second recovered listener panic")
	}

	if first == second {
		t.Fatal("Serve reused an error across recovered panics")
	}
}
