// Package aria2 wraps libaria2 to download many URLs in parallel with ranged
// connections. tart's single-stream pull is throttled per connection by the
// registry, so libaria2's range splitting is used to saturate the link.
package aria2

// #include <stdlib.h>
// #include "aria2_shim.h"
import "C"

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"unsafe"
)

// Item is one download: URL fetched into OutPath, an absolute file path.
type Item struct {
	// URL is the source URL.
	URL string
	// OutPath is the absolute destination file path.
	OutPath string
}

// Options bounds libaria2 parallelism.
type Options struct {
	// Split is the number of connections per download.
	Split int
	// MaxConnPerServer caps connections per server per download.
	MaxConnPerServer int
	// MaxConcurrent caps downloads running at once.
	MaxConcurrent int
}

// Download fetches every item in parallel via libaria2, writing each to its
// OutPath. authHeader, when non-empty, is sent as an HTTP request header on
// every download, for example "Authorization: Bearer <token>". It returns an
// error when any download fails or the library reports a failure.
func Download(ctx context.Context, items []Item, authHeader string, opts Options) error {
	if len(items) == 0 {
		return nil
	}
	n := len(items)
	uris := make([]*C.char, n)
	dirs := make([]*C.char, n)
	outs := make([]*C.char, n)
	for i, item := range items {
		dir := filepath.Dir(item.OutPath)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			slog.ErrorContext(ctx, "aria2 mkdir failed", "err", err, "dir", dir)
			return fmt.Errorf("aria2: mkdir %s: %w", dir, err)
		}
		uris[i] = C.CString(item.URL)
		dirs[i] = C.CString(dir)
		outs[i] = C.CString(filepath.Base(item.OutPath))
	}
	defer func() {
		for i := range n {
			C.free(unsafe.Pointer(uris[i]))
			C.free(unsafe.Pointer(dirs[i]))
			C.free(unsafe.Pointer(outs[i]))
		}
	}()
	var cHeader *C.char
	if authHeader != "" {
		cHeader = C.CString(authHeader)
		defer C.free(unsafe.Pointer(cHeader))
	}
	rc := C.a2_download(
		(**C.char)(unsafe.Pointer(&uris[0])),
		(**C.char)(unsafe.Pointer(&dirs[0])),
		(**C.char)(unsafe.Pointer(&outs[0])),
		C.int(n),
		cHeader,
		C.int(opts.Split),
		C.int(opts.MaxConnPerServer),
		C.int(opts.MaxConcurrent),
	)
	if int(rc) != 0 {
		err := fmt.Errorf("aria2: %d of %d downloads failed (rc=%d)", int(rc), n, int(rc))
		slog.ErrorContext(ctx, "aria2 download reported failures", "err", err, "items", n)
		return err
	}
	return nil
}
