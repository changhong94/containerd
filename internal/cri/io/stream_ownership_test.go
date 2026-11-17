// Copyright The containerd Authors.
// SPDX-License-Identifier: Apache-2.0

package io

import (
	"context"
	"errors"
	"io"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	streamingpb "github.com/containerd/containerd/api/services/streaming/v1"
	"github.com/containerd/ttrpc"
	"google.golang.org/protobuf/types/known/anypb"
)

type trackedConn struct {
	net.Conn
	closed chan struct{}
	once   sync.Once
}

func (c *trackedConn) Close() error { c.once.Do(func() { close(c.closed) }); return c.Conn.Close() }

type trackedListener struct {
	net.Listener
	accepted chan *trackedConn
}

func (l *trackedListener) Accept() (net.Conn, error) {
	c, e := l.Listener.Accept()
	if e != nil {
		return nil, e
	}
	t := &trackedConn{Conn: c, closed: make(chan struct{})}
	l.accepted <- t
	return t, nil
}

type handshakeService struct{ reject bool }

func (s handshakeService) Stream(ctx context.Context, stream streamingpb.TTRPCStreaming_StreamServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	if s.reject {
		return errors.New("rejected handshake")
	}
	if err := stream.Send(&anypb.Any{}); err != nil {
		return err
	}
	_, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

// Verify the peer observes the transport closing, not just an RPC stream EOF.
// Repeating without GC catches leaked dedicated clients on success and failure.
func TestOpenStreamReleasesTransport(t *testing.T) {
	for _, reject := range []bool{false, true} {
		name := "normal"
		if reject {
			name = "handshake_error"
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "rpc.sock")
			raw, err := net.Listen("unix", path)
			if err != nil {
				t.Fatal(err)
			}
			listener := &trackedListener{Listener: raw, accepted: make(chan *trackedConn, 1)}
			srv, err := ttrpc.NewServer()
			if err != nil {
				t.Fatal(err)
			}
			streamingpb.RegisterTTRPCStreamingService(srv, handshakeService{reject: reject})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			defer srv.Close()
			go srv.Serve(ctx, listener)
			for i := 0; i < 32; i++ {
				stream, err := openStream(ctx, "ttrpc+unix://"+path+"?streaming_id=probe")
				conn := <-listener.accepted
				if reject {
					if err == nil {
						t.Fatal("expected handshake error")
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					if err := stream.Close(); err != nil {
						t.Fatal(err)
					}
				}
				if !reject {
					if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
						t.Fatalf("expected EOF: %v", err)
					}
				}
				select {
				case <-conn.closed:
				case <-time.After(time.Second):
					t.Fatal("dedicated RPC transport leaked")
				}
				if !reject {
					if err := stream.Close(); err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}

type drainingService struct {
	received chan int
	release  chan struct{}
}

func (s drainingService) Stream(ctx context.Context, stream streamingpb.TTRPCStreaming_StreamServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	if err := stream.Send(&anypb.Any{}); err != nil {
		return err
	}
	total := 0
	for {
		data, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		total += len(data.Value)
	}
	s.received <- total
	select {
	case <-s.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func TestOwnedStreamHalfCloseDrainsAndCancellationCloses(t *testing.T) {
	for _, cancelEarly := range []bool{false, true} {
		name := "drain"
		if cancelEarly {
			name = "cancel"
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "rpc.sock")
			raw, err := net.Listen("unix", path)
			if err != nil {
				t.Fatal(err)
			}
			listener := &trackedListener{Listener: raw, accepted: make(chan *trackedConn, 1)}
			srv, err := ttrpc.NewServer()
			if err != nil {
				t.Fatal(err)
			}
			service := drainingService{received: make(chan int, 1), release: make(chan struct{})}
			streamingpb.RegisterTTRPCStreamingService(srv, service)
			defer srv.Close()
			go srv.Serve(context.Background(), listener)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stream, err := openStream(ctx, "ttrpc+unix://"+path+"?streaming_id=input")
			if err != nil {
				t.Fatal(err)
			}
			conn := <-listener.accepted
			if cancelEarly {
				cancel()
			} else {
				for i := 0; i < 32; i++ {
					if err := stream.Send(&anypb.Any{Value: make([]byte, 32768)}); err != nil {
						t.Fatal(err)
					}
				}
				if err := stream.Close(); err != nil {
					t.Fatal(err)
				}
				select {
				case n := <-service.received:
					if n != 1048576 {
						t.Fatalf("truncated stdin: %d", n)
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				select {
				case <-conn.closed:
					t.Fatal("half-close prematurely closed transport")
				default:
				}
				close(service.release)
				if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
					t.Fatalf("expected remote EOF: %v", err)
				}
			}
			select {
			case <-conn.closed:
			case <-ctx.Done():
				if !cancelEarly {
					t.Fatal(ctx.Err())
				}
				select {
				case <-conn.closed:
				case <-time.After(time.Second):
					t.Fatal("cancel leaked transport")
				}
			}
		})
	}
}
