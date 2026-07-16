// Package guesttransport provides authenticated ConnectRPC transport over
// unencrypted HTTP/2.
package guesttransport

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect"
)

const (
	authorizationHeader = "Authorization"
	bearerPrefix        = "Bearer "
	readHeaderTimeout   = 10 * time.Second
	readTimeout         = 30 * time.Second
	idleTimeout         = 120 * time.Second
	shutdownTimeout     = 5 * time.Second
)

// Client contains the cleartext HTTP/2 client and authentication interceptor
// needed to construct a ConnectRPC client.
type Client struct {
	httpClient  *http.Client
	interceptor connect.Interceptor
}

// GuestDialer establishes one transport connection to the guest agent. The
// broker supplies a dialer that runs the guest-dial relay over `tart exec`, so
// the HTTP/2 client reaches the guest agent over the guest-agent channel with no
// dependence on the guest's NAT IP or the host bridge route.
type GuestDialer func(ctx context.Context) (net.Conn, error)

// DialContext configures a ConnectRPC transport for unencrypted HTTP/2 whose
// connections come from dial instead of a TCP address. The transport reuses one
// connection across many requests, so dial is invoked only when the HTTP/2
// client needs a fresh connection.
func DialContext(_ context.Context, dial GuestDialer, token string) *Client {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _ string, _ string) (net.Conn, error) {
			return dial(ctx)
		},
	}
	transport.Protocols = new(http.Protocols)
	transport.Protocols.SetUnencryptedHTTP2(true)
	return &Client{
		httpClient:  &http.Client{Transport: transport},
		interceptor: NewClientAuthInterceptor(token),
	}
}

// HTTPClient returns the HTTP client used by a generated ConnectRPC client.
func (client *Client) HTTPClient() *http.Client {
	return client.httpClient
}

// Interceptor returns the bearer-token interceptor used by a generated
// ConnectRPC client.
func (client *Client) Interceptor() connect.Interceptor {
	return client.interceptor
}

// Serve serves handler over unencrypted HTTP/2 using only the caller-provided
// listener.
func Serve(
	ctx context.Context,
	listener net.Listener,
	handler http.Handler,
	token string,
) error {
	trackedListener := newTrackedListener(listener)
	// Serve returns before ctx is canceled when server.Serve fails on its own
	// (for example an Accept error). The shutdown callback only runs on ctx
	// cancellation, so close the tracked connections and listener here to avoid
	// leaking accepted connections and the listener fd on that early path. Both
	// operations are idempotent, so running them again after the shutdown
	// callback already closed everything is safe.
	defer func() {
		_ = trackedListener.closeConnections()
		_ = trackedListener.Close()
	}()
	authenticatedHandler := requireBearerToken(handler, token)
	// WriteTimeout is intentionally omitted: JobStatus is a long-lived
	// server-streaming RPC that writes for the whole job duration, and a write
	// deadline would sever active status streams. ReadTimeout and IdleTimeout
	// still bound request-body reads and idle keep-alive connections, so a slow
	// or malicious peer cannot hold a connection open indefinitely (CWE-400).
	server := &http.Server{
		Handler:           authenticatedHandler,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		IdleTimeout:       idleTimeout,
		BaseContext: func(net.Listener) context.Context {
			return ctx
		},
	}
	server.Protocols = new(http.Protocols)
	server.Protocols.SetHTTP1(true)
	server.Protocols.SetUnencryptedHTTP2(true)
	shutdownDone := make(chan error, 1)
	stopShutdown := context.AfterFunc(ctx, func() {
		shutdownContext, cancelShutdown := context.WithTimeout(
			context.WithoutCancel(ctx),
			shutdownTimeout,
		)
		defer cancelShutdown()
		shutdownError := server.Shutdown(shutdownContext)
		closeError := trackedListener.closeConnections()
		if shutdownError != nil {
			shutdownDone <- shutdownError
			return
		}
		shutdownDone <- closeError
	})

	serveError := normalizeServerError(server.Serve(trackedListener))
	if stopShutdown() {
		return serveError
	}
	if shutdownError := <-shutdownDone; shutdownError != nil {
		return shutdownError
	}
	if serveError != nil {
		return serveError
	}
	return transportError{operation: "context", err: ctx.Err()}
}

type trackedListener struct {
	net.Listener
	mutex       sync.Mutex
	connections map[*trackedConnection]struct{}
}

func newTrackedListener(listener net.Listener) *trackedListener {
	return &trackedListener{
		Listener:    listener,
		mutex:       sync.Mutex{},
		connections: make(map[*trackedConnection]struct{}),
	}
}

func (listener *trackedListener) Accept() (net.Conn, error) {
	connection, err := listener.Listener.Accept()
	if err != nil {
		return nil, transportError{operation: "accept", err: err}
	}
	tracked := &trackedConnection{
		Conn:     connection,
		listener: listener,
		close:    sync.Once{},
		err:      nil,
	}
	listener.mutex.Lock()
	listener.connections[tracked] = struct{}{}
	listener.mutex.Unlock()
	return tracked, nil
}

func (listener *trackedListener) closeConnections() error {
	listener.mutex.Lock()
	connections := make([]*trackedConnection, 0, len(listener.connections))
	for connection := range listener.connections {
		connections = append(connections, connection)
	}
	listener.mutex.Unlock()

	var closeError error
	for _, connection := range connections {
		if err := connection.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			closeError = err
		}
	}
	return closeError
}

func (listener *trackedListener) remove(connection *trackedConnection) {
	listener.mutex.Lock()
	delete(listener.connections, connection)
	listener.mutex.Unlock()
}

type trackedConnection struct {
	net.Conn
	listener *trackedListener
	close    sync.Once
	err      error
}

func (connection *trackedConnection) Close() error {
	connection.close.Do(func() {
		connection.err = connection.Conn.Close()
		connection.listener.remove(connection)
	})
	return connection.err
}

// NewClientAuthInterceptor injects the boot-scoped bearer token into unary and
// streaming ConnectRPC requests.
func NewClientAuthInterceptor(token string) connect.Interceptor {
	return &authInterceptor{token: token, client: true}
}

// NewServerAuthInterceptor validates the boot-scoped bearer token on unary and
// streaming ConnectRPC requests.
func NewServerAuthInterceptor(token string) connect.Interceptor {
	return &authInterceptor{token: token, client: false}
}

type authInterceptor struct {
	token  string
	client bool
}

func (interceptor *authInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, request connect.AnyRequest) (connect.AnyResponse, error) {
		if interceptor.client {
			setAuthorization(request.Header(), interceptor.token)
		} else if !validAuthorization(request.Header(), interceptor.token) {
			return nil, unauthenticatedError()
		}
		return next(ctx, request)
	}
}

func (interceptor *authInterceptor) WrapStreamingClient(
	next connect.StreamingClientFunc,
) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		connection := next(ctx, spec)
		if interceptor.client {
			setAuthorization(connection.RequestHeader(), interceptor.token)
		}
		return connection
	}
}

func (interceptor *authInterceptor) WrapStreamingHandler(
	next connect.StreamingHandlerFunc,
) connect.StreamingHandlerFunc {
	return func(ctx context.Context, connection connect.StreamingHandlerConn) error {
		if !interceptor.client && !validAuthorization(connection.RequestHeader(), interceptor.token) {
			return unauthenticatedError()
		}
		return next(ctx, connection)
	}
}

func requireBearerToken(next http.Handler, token string) http.Handler {
	errorWriter := connect.NewErrorWriter()
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if validAuthorization(request.Header, token) {
			next.ServeHTTP(response, request)
			return
		}
		if err := errorWriter.Write(response, request, unauthenticatedError()); err != nil {
			http.Error(response, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		}
	})
}

func setAuthorization(header http.Header, token string) {
	header.Set(authorizationHeader, bearerPrefix+token)
}

func validAuthorization(header http.Header, expectedToken string) bool {
	providedToken, found := strings.CutPrefix(header.Get(authorizationHeader), bearerPrefix)
	if !found {
		providedToken = ""
	}
	providedHash := sha256.Sum256([]byte(providedToken))
	expectedHash := sha256.Sum256([]byte(expectedToken))
	return found && subtle.ConstantTimeCompare(providedHash[:], expectedHash[:]) == 1
}

func unauthenticatedError() *connect.Error {
	return connect.NewError(connect.CodeUnauthenticated, errors.New("invalid bearer token"))
}

func normalizeServerError(err error) error {
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

type transportError struct {
	operation string
	err       error
}

func (err transportError) Error() string {
	return "guest transport " + err.operation + ": " + err.err.Error()
}

func (err transportError) Unwrap() error {
	return err.err
}

// Timeout and Temporary forward to the wrapped error so a transportError around
// an Accept failure still satisfies [net.Error]. [http.Server.Serve]
// type-asserts the Accept error to [net.Error] and backs off on temporary
// failures, so hiding these methods would turn a transient listener error into
// a fatal Serve exit.
func (err transportError) Timeout() bool {
	var netErr net.Error
	return errors.As(err.err, &netErr) && netErr.Timeout()
}

func (err transportError) Temporary() bool {
	var temporary temporaryError
	return errors.As(err.err, &temporary) && temporary.Temporary()
}

// temporaryError matches the shape of the deprecated Temporary method on
// [net.Error] without naming it, so forwarding it drives [http.Server.Serve]'s
// temporary-error backoff without referencing a deprecated symbol.
type temporaryError interface {
	Temporary() bool
}
