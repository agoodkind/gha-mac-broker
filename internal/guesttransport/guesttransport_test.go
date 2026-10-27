package guesttransport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	testToken           = "boot-scoped-test-token"
	testUnaryProcedure  = "/test.guest.v1.TestService/Echo"
	testStreamProcedure = "/test.guest.v1.TestService/Watch"
)

var testUnarySpec = connect.Spec{
	Procedure:  testUnaryProcedure,
	StreamType: connect.StreamTypeUnary,
}

var testStreamSpec = connect.Spec{
	Procedure:  testStreamProcedure,
	StreamType: connect.StreamTypeServer,
}

func TestUnaryAuthentication(t *testing.T) {
	t.Parallel()

	listener := listenLocal(t, "127.0.0.1:0")
	serverContext, stopServer := context.WithCancel(context.Background())
	serverDone := serveTestService(serverContext, listener, testToken)
	t.Cleanup(func() {
		stopServer()
		waitForServer(t, serverDone)
	})

	testCases := []struct {
		name      string
		token     string
		auth      bool
		wantCode  connect.Code
		wantValue string
	}{
		{
			name:      "correct token",
			token:     testToken,
			auth:      true,
			wantValue: "echo:hello",
		},
		{
			name:     "wrong token",
			token:    "wrong-token",
			auth:     true,
			wantCode: connect.CodeUnauthenticated,
		},
		{
			name:     "absent token",
			wantCode: connect.CodeUnauthenticated,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			clientTransport := DialContext(
				context.Background(),
				tcpDialer(listener.Addr().String()),
				testCase.token,
			)
			clientOptions := make([]connect.ClientInterceptor, 0, 1)
			if testCase.auth {
				clientOptions = append(
					clientOptions,
					clientTransport.Interceptor(),
				)
			}
			client := connect.NewClient(
				connecthttp.NewTransport(clientTransport.HTTPClient(), "http://"+listener.Addr().String()),
				clientOptions...,
			)
			response := new(wrapperspb.StringValue)
			err := client.CallUnary(
				context.Background(),
				testUnarySpec,
				wrapperspb.String("hello"),
				response,
			)
			if testCase.wantCode != 0 {
				if connect.CodeOf(err) != testCase.wantCode {
					t.Fatalf("CallUnary() error code = %v, want %v: %v", connect.CodeOf(err), testCase.wantCode, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("CallUnary() error = %v", err)
			}
			if response.Value != testCase.wantValue {
				t.Fatalf("CallUnary() value = %q, want %q", response.Value, testCase.wantValue)
			}
		})
	}
}

func TestServerAuthInterceptorRejectsWrongToken(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(newTestHandler(testToken))
	t.Cleanup(server.Close)
	client := connect.NewClient(
		connecthttp.NewTransport(server.Client(), server.URL),
		NewClientAuthInterceptor("wrong-token"),
	)
	err := client.CallUnary(
		context.Background(),
		testUnarySpec,
		wrapperspb.String("hello"),
		new(wrapperspb.StringValue),
	)
	if connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Fatalf(
			"CallUnary() error code = %v, want %v: %v",
			connect.CodeOf(err),
			connect.CodeUnauthenticated,
			err,
		)
	}
}

func TestServerStreamAuthenticationAndMessages(t *testing.T) {
	t.Parallel()

	listener := listenLocal(t, "127.0.0.1:0")
	serverContext, stopServer := context.WithCancel(context.Background())
	serverDone := serveTestService(serverContext, listener, testToken)
	t.Cleanup(func() {
		stopServer()
		waitForServer(t, serverDone)
	})

	clientTransport := DialContext(context.Background(), tcpDialer(listener.Addr().String()), testToken)
	client := connect.NewClient(
		connecthttp.NewTransport(clientTransport.HTTPClient(), "http://"+listener.Addr().String()),
		clientTransport.Interceptor(),
	)
	stream, err := client.CallServerStream(
		context.Background(),
		testStreamSpec,
		wrapperspb.String("event"),
	)
	if err != nil {
		t.Fatalf("CallServerStream() error = %v", err)
	}

	var messages []string
	for {
		message := new(wrapperspb.StringValue)
		err := stream.Receive(message)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("Receive() error = %v", err)
		}
		messages = append(messages, message.Value)
	}

	wantMessages := []string{"event:1", "event:2", "event:3"}
	if !reflect.DeepEqual(messages, wantMessages) {
		t.Fatalf("stream messages = %#v, want %#v", messages, wantMessages)
	}
}

func TestClientReconnectsAfterServerRestart(t *testing.T) {
	t.Parallel()

	firstListener := listenLocal(t, "127.0.0.1:0")
	address := firstListener.Addr().String()
	client := newUnaryClient(t, context.Background(), address, testToken)

	firstContext, stopFirst := context.WithCancel(context.Background())
	firstDone := serveTestService(firstContext, firstListener, testToken)
	callUnary(t, client, "before", "echo:before")
	stopFirst()
	waitForServer(t, firstDone)

	secondListener := listenLocal(t, address)
	secondContext, stopSecond := context.WithCancel(context.Background())
	secondDone := serveTestService(secondContext, secondListener, testToken)
	t.Cleanup(func() {
		stopSecond()
		waitForServer(t, secondDone)
	})

	callUnary(t, client, "after", "echo:after")
}

func newTestHandler(token string) http.Handler {
	mux := http.NewServeMux()
	server := connect.NewServer(NewServerAuthInterceptor(token))
	server.Register(
		connect.Method{
			Spec: testUnarySpec,
			Handler: func(_ context.Context, _ connect.Spec, stream connect.ServerStream) error {
				request := new(wrapperspb.StringValue)
				if err := stream.Receive(request); err != nil {
					return err
				}
				return stream.Send(wrapperspb.String("echo:" + request.Value))
			},
		},
		connect.Method{
			Spec: testStreamSpec,
			Handler: func(_ context.Context, _ connect.Spec, stream connect.ServerStream) error {
				request := new(wrapperspb.StringValue)
				if err := stream.Receive(request); err != nil {
					return err
				}
				for sequence := 1; sequence <= 3; sequence++ {
					message := wrapperspb.String(fmt.Sprintf("%s:%d", request.Value, sequence))
					if err := stream.Send(message); err != nil {
						return err
					}
				}
				return nil
			},
		},
	)
	connecthttp.Mount(mux, server)
	return mux
}

func serveTestService(ctx context.Context, listener net.Listener, token string) <-chan error {
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, listener, newTestHandler(token), token)
	}()
	return done
}

func newUnaryClient(
	t *testing.T,
	ctx context.Context,
	address string,
	token string,
) *connect.Client {
	t.Helper()
	clientTransport := DialContext(ctx, tcpDialer(address), token)
	return connect.NewClient(
		connecthttp.NewTransport(clientTransport.HTTPClient(), "http://"+address),
		clientTransport.Interceptor(),
	)
}

func callUnary(
	t *testing.T,
	client *connect.Client,
	value string,
	want string,
) {
	t.Helper()
	response := new(wrapperspb.StringValue)
	err := client.CallUnary(
		context.Background(),
		testUnarySpec,
		wrapperspb.String(value),
		response,
	)
	if err != nil {
		t.Fatalf("CallUnary(%q) error = %v", value, err)
	}
	if response.Value != want {
		t.Fatalf("CallUnary(%q) value = %q, want %q", value, response.Value, want)
	}
}

func listenLocal(t *testing.T, address string) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("net.Listen(%q) error = %v", address, err)
	}
	return listener
}

func waitForServer(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Serve() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve() did not stop within 5 seconds")
	}
}

// tcpDialer adapts a TCP address to a GuestDialer so the transport tests exercise
// DialContext against a real listener. Production dials over tart exec instead.
func tcpDialer(address string) GuestDialer {
	return func(ctx context.Context) (net.Conn, error) {
		var dialer net.Dialer
		return dialer.DialContext(ctx, "tcp", address)
	}
}
