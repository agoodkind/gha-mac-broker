package guesttransport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	testToken           = "boot-scoped-test-token"
	testUnaryProcedure  = "/test.guest.v1.TestService/Echo"
	testStreamProcedure = "/test.guest.v1.TestService/Watch"
)

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
				listener.Addr().String(),
				testCase.token,
			)
			clientOptions := make([]connect.ClientOption, 0, 1)
			if testCase.auth {
				clientOptions = append(
					clientOptions,
					connect.WithInterceptors(clientTransport.Interceptor()),
				)
			}
			client := connect.NewClient[wrapperspb.StringValue, wrapperspb.StringValue](
				clientTransport.HTTPClient(),
				fmt.Sprintf("http://%s%s", listener.Addr(), testUnaryProcedure),
				clientOptions...,
			)
			response, err := client.CallUnary(
				context.Background(),
				connect.NewRequest(wrapperspb.String("hello")),
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
			if response.Msg.Value != testCase.wantValue {
				t.Fatalf("CallUnary() value = %q, want %q", response.Msg.Value, testCase.wantValue)
			}
		})
	}
}

func TestServerAuthInterceptorRejectsWrongToken(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(newTestHandler(testToken))
	t.Cleanup(server.Close)
	client := connect.NewClient[wrapperspb.StringValue, wrapperspb.StringValue](
		server.Client(),
		server.URL+testUnaryProcedure,
		connect.WithInterceptors(NewClientAuthInterceptor("wrong-token")),
	)
	_, err := client.CallUnary(
		context.Background(),
		connect.NewRequest(wrapperspb.String("hello")),
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

	clientTransport := DialContext(context.Background(), listener.Addr().String(), testToken)
	client := connect.NewClient[wrapperspb.StringValue, wrapperspb.StringValue](
		clientTransport.HTTPClient(),
		fmt.Sprintf("http://%s%s", listener.Addr(), testStreamProcedure),
		connect.WithInterceptors(clientTransport.Interceptor()),
	)
	stream, err := client.CallServerStream(
		context.Background(),
		connect.NewRequest(wrapperspb.String("event")),
	)
	if err != nil {
		t.Fatalf("CallServerStream() error = %v", err)
	}

	var messages []string
	for stream.Receive() {
		messages = append(messages, stream.Msg().Value)
	}
	if err := stream.Err(); err != nil {
		t.Fatalf("Receive() error = %v", err)
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
	mux.Handle(
		testUnaryProcedure,
		connect.NewUnaryHandler(
			testUnaryProcedure,
			func(_ context.Context, request *connect.Request[wrapperspb.StringValue]) (*connect.Response[wrapperspb.StringValue], error) {
				return connect.NewResponse(wrapperspb.String("echo:" + request.Msg.Value)), nil
			},
			connect.WithInterceptors(NewServerAuthInterceptor(token)),
		),
	)
	mux.Handle(
		testStreamProcedure,
		connect.NewServerStreamHandler(
			testStreamProcedure,
			func(_ context.Context, request *connect.Request[wrapperspb.StringValue], stream *connect.ServerStream[wrapperspb.StringValue]) error {
				for sequence := 1; sequence <= 3; sequence++ {
					message := wrapperspb.String(fmt.Sprintf("%s:%d", request.Msg.Value, sequence))
					if err := stream.Send(message); err != nil {
						return err
					}
				}
				return nil
			},
			connect.WithInterceptors(NewServerAuthInterceptor(token)),
		),
	)
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
) *connect.Client[wrapperspb.StringValue, wrapperspb.StringValue] {
	t.Helper()
	clientTransport := DialContext(ctx, address, token)
	return connect.NewClient[wrapperspb.StringValue, wrapperspb.StringValue](
		clientTransport.HTTPClient(),
		fmt.Sprintf("http://%s%s", address, testUnaryProcedure),
		connect.WithInterceptors(clientTransport.Interceptor()),
	)
}

func callUnary(
	t *testing.T,
	client *connect.Client[wrapperspb.StringValue, wrapperspb.StringValue],
	value string,
	want string,
) {
	t.Helper()
	response, err := client.CallUnary(
		context.Background(),
		connect.NewRequest(wrapperspb.String(value)),
	)
	if err != nil {
		t.Fatalf("CallUnary(%q) error = %v", value, err)
	}
	if response.Msg.Value != want {
		t.Fatalf("CallUnary(%q) value = %q, want %q", value, response.Msg.Value, want)
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
