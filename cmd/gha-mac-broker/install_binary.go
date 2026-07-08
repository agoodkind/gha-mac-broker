package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
)

func defaultInstallBinPath(ctx context.Context) (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		slog.ErrorContext(ctx, "resolve home dir failed", "err", err)
		return "", fmt.Errorf("resolve home dir: %w", err)
	}
	return filepath.Join(home, ".local", "bin", brokerBinaryName), nil
}

func installRunningBinary(ctx context.Context, sourcePath string, destinationPath string) error {
	sameFile, err := samePathOrFile(ctx, sourcePath, destinationPath)
	if err != nil {
		return err
	}
	if sameFile {
		return nil
	}
	if err := copyBinary(ctx, sourcePath, destinationPath); err != nil {
		slog.ErrorContext(ctx, "install binary copy failed", "err", err, "source", sourcePath, "destination", destinationPath)
		return err
	}
	return nil
}

func samePathOrFile(ctx context.Context, sourcePath string, destinationPath string) (bool, error) {
	if filepath.Clean(sourcePath) == filepath.Clean(destinationPath) {
		return true, nil
	}
	sourceInfo, err := os.Stat(sourcePath)
	if err != nil {
		slog.ErrorContext(ctx, "source binary stat failed", "err", err, "source", sourcePath)
		return false, fmt.Errorf("install: stat source binary %s: %w", sourcePath, err)
	}
	destinationInfo, err := os.Stat(destinationPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		slog.ErrorContext(ctx, "destination binary stat failed", "err", err, "destination", destinationPath)
		return false, fmt.Errorf("install: stat destination binary %s: %w", destinationPath, err)
	}
	return os.SameFile(sourceInfo, destinationInfo), nil
}

func copyBinary(ctx context.Context, sourcePath string, destinationPath string) error {
	sourceInfo, err := os.Stat(sourcePath)
	if err != nil {
		slog.ErrorContext(ctx, "source binary stat failed", "err", err, "source", sourcePath)
		return fmt.Errorf("install: stat source binary %s: %w", sourcePath, err)
	}
	if !sourceInfo.Mode().IsRegular() {
		return fmt.Errorf("install: source binary %s is not a regular file", sourcePath)
	}
	destinationDir := filepath.Dir(destinationPath)
	if err := os.MkdirAll(destinationDir, 0o755); err != nil {
		slog.ErrorContext(ctx, "binary destination directory create failed", "err", err, "directory", destinationDir)
		return fmt.Errorf("install: create binary dir %s: %w", destinationDir, err)
	}
	source, err := os.Open(sourcePath)
	if err != nil {
		slog.ErrorContext(ctx, "source binary open failed", "err", err, "source", sourcePath)
		return fmt.Errorf("install: open source binary %s: %w", sourcePath, err)
	}
	defer source.Close()
	temp, err := os.CreateTemp(destinationDir, ".gha-mac-broker-*")
	if err != nil {
		slog.ErrorContext(ctx, "temp binary create failed", "err", err, "directory", destinationDir)
		return fmt.Errorf("install: create temp binary in %s: %w", destinationDir, err)
	}
	tempPath := temp.Name()
	removeTemp := true
	defer func() {
		if removeTemp {
			_ = os.Remove(tempPath)
		}
	}()
	if _, err := io.Copy(temp, source); err != nil {
		_ = temp.Close()
		slog.ErrorContext(ctx, "binary copy failed", "err", err, "temp", tempPath)
		return fmt.Errorf("install: copy binary to %s: %w", tempPath, err)
	}
	if err := temp.Chmod(sourceInfo.Mode().Perm()); err != nil {
		_ = temp.Close()
		slog.ErrorContext(ctx, "temp binary chmod failed", "err", err, "temp", tempPath)
		return fmt.Errorf("install: chmod binary %s: %w", tempPath, err)
	}
	if err := temp.Close(); err != nil {
		slog.ErrorContext(ctx, "temp binary close failed", "err", err, "temp", tempPath)
		return fmt.Errorf("install: close binary %s: %w", tempPath, err)
	}
	if err := os.Rename(tempPath, destinationPath); err != nil {
		slog.ErrorContext(ctx, "binary replace failed", "err", err, "temp", tempPath, "destination", destinationPath)
		return fmt.Errorf("install: replace binary %s: %w", destinationPath, err)
	}
	removeTemp = false
	slog.InfoContext(ctx, "install binary copied", "source", sourcePath, "destination", destinationPath)
	return nil
}
