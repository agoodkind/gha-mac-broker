package fastpull

import (
	"strings"
)

const (
	cirrusRepoPrefix = "cirruslabs/macos-"
	cirrusRepoSuffix = "-xcode"
)

// isCirrusXcodeRepo reports whether repo is a Cirrus macOS-Xcode image path. It
// keeps the fast pull restricted to the images the pool is allowed to serve.
func isCirrusXcodeRepo(repo string) bool {
	return strings.HasPrefix(repo, cirrusRepoPrefix) && strings.HasSuffix(repo, cirrusRepoSuffix)
}
