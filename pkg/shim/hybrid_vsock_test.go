//go:build !windows

// Copyright The containerd Authors.
// SPDX-License-Identifier: Apache-2.0
package shim

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"
)

func TestHybridVsockTransientEOFRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vsock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	done := make(chan error, 1)
	go func() {
		for i := 0; i < 4; i++ {
			conn, err := listener.Accept()
			if err != nil {
				done <- err
				return
			}
			line, err := bufio.NewReader(conn).ReadString('\n')
			if err != nil || line != "CONNECT 1024\n" {
				conn.Close()
				done <- fmt.Errorf("handshake %q: %v", line, err)
				return
			}
			if i == 3 {
				fmt.Fprint(conn, "OK 1073741824\n")
			}
			conn.Close()
		}
		done <- nil
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started := time.Now()
	conn, err := AnonReconnectDialerContext(ctx, "hvsock://"+path+":1024", 100*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("recovery took %v", elapsed)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestHybridVsockHandshakeCancellation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vsock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		conn, err := AnonReconnectDialerContext(ctx, "hvsock://"+path+":1024", 100*time.Second)
		if conn != nil {
			conn.Close()
		}
		result <- err
	}()
	peer := <-accepted
	defer peer.Close()
	if _, err := bufio.NewReader(peer).ReadString('\n'); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("handshake ignored cancellation")
	}
}

func TestHybridVsockClearsHandshakeDeadline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "vsock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		bufio.NewReader(conn).ReadString('\n')
		fmt.Fprint(conn, "OK 1073741824\n")
		time.Sleep(100 * time.Millisecond)
		fmt.Fprint(conn, "x")
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	conn, err := AnonReconnectDialerContext(ctx, "hvsock://"+path+":1024", 100*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var b [1]byte
	if _, err := conn.Read(b[:]); err != nil || b[0] != 'x' {
		t.Fatalf("established connection inherited dial cancellation: %q %v", b, err)
	}
}
