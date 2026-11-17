// Copyright The containerd Authors.
// SPDX-License-Identifier: Apache-2.0
package io

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"
)

type closeNotice struct {
	io.Writer
	closed chan struct{}
}

func (w closeNotice) Close() error { close(w.closed); return nil }

type bufferCloser struct{ bytes.Buffer }

func (w *bufferCloser) Close() error { return nil }

// A program may close stdout before consuming its stdin. EOF on that output
// must not cancel a concurrent upload; only process exit/cancellation may do so.
func TestExecInputSurvivesOutputEOF(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	input, producer := io.Pipe()
	defer input.Close()
	defer producer.Close()
	received := &bufferCloser{}
	outputClosed := make(chan struct{})
	e := &ExecIO{stdioStream: &stdioStream{stdin: received, stdout: io.NopCloser(bytes.NewReader(nil))}, closer: &wgCloser{ctx: ctx, cancel: cancel, wg: &sync.WaitGroup{}}}
	defer e.Cancel()
	done := e.Attach(AttachOptions{Stdin: input, Stdout: closeNotice{io.Discard, outputClosed}, StdinOnce: true, CloseStdin: func() error { return nil }})
	select {
	case <-outputClosed:
	case <-time.After(time.Second):
		t.Fatal("output did not reach EOF")
	}
	expected := bytes.Repeat([]byte("x"), 65792)
	upload := make(chan error, 1)
	go func() { _, err := producer.Write(expected); producer.Close(); upload <- err }()
	select {
	case err := <-upload:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("stdin was closed before upload completed")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("attach did not complete")
	}
	if !bytes.Equal(received.Bytes(), expected) {
		t.Fatalf("received %d of %d bytes", received.Len(), len(expected))
	}
}
