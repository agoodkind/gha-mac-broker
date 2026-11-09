package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"
)

type httpPanicError struct {
	message string
}

func (err *httpPanicError) Error() string { return err.message }

// Serve runs an HTTP handler on an existing listener.
//
// Serve calls onReady after starting the server goroutine when onReady differs
// from nil. It uses readTimeout for request reads and shutdownTimeout after
// context cancellation.
//
// The caller supplies the existing listener:
//
//	err := Serve(
//		ctx,
//		listener,
//		handler,
//		onReady,
//		readTimeout,
//		shutdownTimeout,
//	)
func Serve(
	ctx context.Context,
	listener net.Listener,
	handler http.Handler,
	onReady func(),
	readTimeout time.Duration,
	shutdownTimeout time.Duration,
) error {
	httpSrv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: readTimeout,
		ReadTimeout:       readTimeout,
	}

	errCh := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				panicError := &httpPanicError{message: fmt.Sprintf("panic: %v", r)}
				slog.ErrorContext(ctx, "server goroutine panic recovered", "err", panicError)
				errCh <- &httpPanicError{message: fmt.Sprintf("panic: %v", r)}
			}
		}()
		slog.InfoContext(ctx, "server listening", "addr", listener.Addr().String())
		if err := httpSrv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.ErrorContext(ctx, "server error", "err", err)
			errCh <- fmt.Errorf("listen: %w", err)

			return
		}

		errCh <- nil
	}()

	if onReady != nil {
		onReady()
	}

	select {
	case <-ctx.Done():
		slog.InfoContext(ctx, "shutting down", "reason", ctx.Err())
	case err := <-errCh:
		return err
	}

	shutCtx, shutCancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer shutCancel()
	if err := httpSrv.Shutdown(shutCtx); err != nil {
		slog.WarnContext(shutCtx, "http shutdown error", "err", err)
	}

	return nil
}
