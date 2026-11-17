// Copyright The containerd Authors.
// SPDX-License-Identifier: Apache-2.0
package v2

import (
	"context"
	"errors"
	"fmt"
	api "github.com/containerd/containerd/api/runtime/task/v3"
	shimclient "github.com/containerd/containerd/v2/pkg/shim"
	"github.com/containerd/ttrpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

type countedListener struct {
	net.Listener
	accepts atomic.Int32
}

func (l *countedListener) Accept() (net.Conn, error) {
	c, e := l.Listener.Accept()
	if e == nil {
		l.accepts.Add(1)
	}
	return c, e
}

type forkRPCService struct {
	api.TTRPCTaskService
	creates       atomic.Int32
	waits         atomic.Int32
	stateID       string
	frozenStates  atomic.Int32
	stalledStates atomic.Int32
}

func (s *forkRPCService) State(ctx context.Context, req *api.StateRequest) (*api.StateResponse, error) {
	if s.stalledStates.Add(-1) >= 0 {
		<-ctx.Done()
		return nil, status.Error(codes.DeadlineExceeded, "stale recovery connection")
	}
	if s.frozenStates.Add(-1) >= 0 {
		return nil, status.Error(codes.FailedPrecondition, "Task lifecycle is frozen for VM fork")
	}
	s.stateID = req.ID
	return &api.StateResponse{Pid: 123}, nil
}
func (s *forkRPCService) Create(context.Context, *api.CreateTaskRequest) (*api.CreateTaskResponse, error) {
	s.creates.Add(1)
	return nil, status.Error(codes.Unavailable, "ambiguous mutation outcome")
}
func TestForkFreshConnectionAndNoMutationReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "task.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	l := &countedListener{Listener: listener}
	defer l.Close()
	server, err := ttrpc.NewServer()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	service := &forkRPCService{}
	api.RegisterTTRPCTaskService(server, service)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go server.Serve(ctx, l)
	client := &sandboxTaskClient{id: "task", params: shimclient.BootstrapParams{Address: "unix://" + path, Protocol: "ttrpc", Version: 3}}
	for i := 0; i < 2; i++ {
		r, e := client.State(ctx, &api.StateRequest{ID: "task"})
		if e != nil || r.Pid != 123 {
			t.Fatalf("state %v %v", r, e)
		}
	}
	if _, err = client.Create(ctx, &api.CreateTaskRequest{ID: "task"}); err == nil {
		t.Fatal("mutation must surface error")
	}
	if service.creates.Load() != 1 || l.accepts.Load() != 3 {
		t.Fatalf("creates=%d connections=%d", service.creates.Load(), l.accepts.Load())
	}
}

func (s *forkRPCService) Wait(context.Context, *api.WaitRequest) (*api.WaitResponse, error) {
	if s.waits.Add(1) == 1 {
		return nil, status.Error(codes.Unavailable, "fork transport reset")
	}
	return &api.WaitResponse{ExitStatus: 42}, nil
}
func TestForkGuestAliasAndWaitReconnect(t *testing.T) {
	path := filepath.Join(t.TempDir(), "task.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	server, err := ttrpc.NewServer()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	service := &forkRPCService{}
	api.RegisterTTRPCTaskService(server, service)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go server.Serve(ctx, listener)
	client := &sandboxTaskClient{id: "external", guestID: "inherited", params: shimclient.BootstrapParams{Address: "unix://" + path, Protocol: "ttrpc", Version: 3}}
	req := &api.StateRequest{ID: "external"}
	if _, err := client.State(ctx, req); err != nil {
		t.Fatal(err)
	}
	if req.ID != "external" || service.stateID != "inherited" {
		t.Fatalf("caller=%s guest=%s", req.ID, service.stateID)
	}
	result, err := client.Wait(ctx, &api.WaitRequest{ID: "external"})
	if err != nil || result.ExitStatus != 42 || service.waits.Load() != 2 {
		t.Fatalf("wait=%v err=%v calls=%d", result, err, service.waits.Load())
	}
}

func TestForkWaitSurvivesMissingSocketDuringRebuild(t *testing.T) {
	path := filepath.Join(t.TempDir(), "task.sock")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := &sandboxTaskClient{id: "external", guestID: "inherited", params: shimclient.BootstrapParams{Address: "unix://" + path, Protocol: "ttrpc", Version: 3}}
	done := make(chan error, 1)
	go func() {
		response, err := client.Wait(ctx, &api.WaitRequest{ID: "external"})
		if err == nil && response.ExitStatus != 42 {
			err = fmt.Errorf("wrong exit status: %v", response)
		}
		done <- err
	}()
	// There is deliberately no socket while the VMM compacts/rebuilds.
	select {
	case err := <-done:
		t.Fatalf("transport gap became a process exit: %v", err)
	case <-time.After(250 * time.Millisecond):
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	server, err := ttrpc.NewServer()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	service := &forkRPCService{}
	service.frozenStates.Store(2)
	api.RegisterTTRPCTaskService(server, service)
	go server.Serve(ctx, listener)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestForkWaitMissingSocketHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client := &sandboxTaskClient{id: "external", params: shimclient.BootstrapParams{Address: "unix://" + filepath.Join(t.TempDir(), "missing"), Protocol: "ttrpc", Version: 3}}
	done := make(chan error, 1)
	go func() { _, err := client.Wait(ctx, &api.WaitRequest{ID: "external"}); done <- err }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Wait ignored cancellation")
	}
}

func TestForkWaitProbeTimeoutReconnectsWithoutInventingExit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "task.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	server, err := ttrpc.NewServer()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	service := &forkRPCService{}
	service.stalledStates.Store(1)
	api.RegisterTTRPCTaskService(server, service)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go server.Serve(ctx, listener)
	client := &sandboxTaskClient{id: "task", params: shimclient.BootstrapParams{Address: "unix://" + path, Protocol: "ttrpc", Version: 3}}
	response, err := client.Wait(ctx, &api.WaitRequest{ID: "task"})
	if err != nil || response.ExitStatus != 42 {
		t.Fatalf("probe timeout became an exit: %v %v", response, err)
	}
	if service.waits.Load() != 2 {
		t.Fatalf("wait calls: %d", service.waits.Load())
	}
}

func TestForkWaitProbeHonorsCallerDeadline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "task.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	server, err := ttrpc.NewServer()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	service := &forkRPCService{}
	service.stalledStates.Store(10)
	api.RegisterTTRPCTaskService(server, service)
	go server.Serve(context.Background(), listener)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	client := &sandboxTaskClient{id: "task", params: shimclient.BootstrapParams{Address: "unix://" + path, Protocol: "ttrpc", Version: 3}}
	_, err = client.Wait(ctx, &api.WaitRequest{ID: "task"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("caller deadline ignored: %v", err)
	}
	if service.waits.Load() != 0 {
		t.Fatal("Wait started after caller deadline")
	}
}
