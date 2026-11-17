// Copyright The containerd Authors.
// SPDX-License-Identifier: Apache-2.0
package v2

import (
	"context"
	"errors"
	api "github.com/containerd/containerd/api/runtime/task/v3"
	shimclient "github.com/containerd/containerd/v2/pkg/shim"
	"github.com/containerd/log"
	"github.com/containerd/ttrpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"io"
	"os"
	"strings"
	"syscall"
	"time"
)

func traceSandboxRPC(ctx context.Context, method, id, execID string) func() {
	started := time.Now()
	return func() {
		if os.Getenv("KUASAR_RPC_TIMING") == "1" {
			log.G(ctx).WithField("method", method).WithField("id", id).WithField("exec_id", execID).WithField("milliseconds", time.Since(started).Seconds()*1000).Info("fork timing: task RPC")
		}
	}
}

// sandboxTaskClient tolerates a completed VM transport reset on the next RPC.
// Mutations are never automatically retried: a lost reply has an unknown outcome.
type sandboxTaskClient struct {
	id      string
	guestID string
	params  shimclient.BootstrapParams
}

func (c *sandboxTaskClient) State(ctx context.Context, req *api.StateRequest) (*api.StateResponse, error) {
	defer traceSandboxRPC(ctx, "State", req.ID, req.ExecID)()
	if c.guestID != "" {
		req = proto.Clone(req).(*api.StateRequest)
		req.ID = c.guestID
	}
	conn, err := makeConnection(ctx, c.id, c.params, func() {})
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client, err := NewTaskClient(conn, c.params.Version)
	if err != nil {
		return nil, err
	}
	return client.State(ctx, req)
}
func (c *sandboxTaskClient) Create(ctx context.Context, req *api.CreateTaskRequest) (*api.CreateTaskResponse, error) {
	if c.guestID != "" {
		req = proto.Clone(req).(*api.CreateTaskRequest)
		req.ID = c.guestID
	}
	conn, err := makeConnection(ctx, c.id, c.params, func() {})
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client, err := NewTaskClient(conn, c.params.Version)
	if err != nil {
		return nil, err
	}
	return client.Create(ctx, req)
}
func (c *sandboxTaskClient) Start(ctx context.Context, req *api.StartRequest) (*api.StartResponse, error) {
	defer traceSandboxRPC(ctx, "Start", req.ID, req.ExecID)()
	if c.guestID != "" {
		req = proto.Clone(req).(*api.StartRequest)
		req.ID = c.guestID
	}
	conn, err := makeConnection(ctx, c.id, c.params, func() {})
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client, err := NewTaskClient(conn, c.params.Version)
	if err != nil {
		return nil, err
	}
	return client.Start(ctx, req)
}
func (c *sandboxTaskClient) Delete(ctx context.Context, req *api.DeleteRequest) (*api.DeleteResponse, error) {
	if c.guestID != "" {
		req = proto.Clone(req).(*api.DeleteRequest)
		req.ID = c.guestID
	}
	conn, err := makeConnection(ctx, c.id, c.params, func() {})
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client, err := NewTaskClient(conn, c.params.Version)
	if err != nil {
		return nil, err
	}
	return client.Delete(ctx, req)
}
func (c *sandboxTaskClient) Pids(ctx context.Context, req *api.PidsRequest) (*api.PidsResponse, error) {
	if c.guestID != "" {
		req = proto.Clone(req).(*api.PidsRequest)
		req.ID = c.guestID
	}
	conn, err := makeConnection(ctx, c.id, c.params, func() {})
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client, err := NewTaskClient(conn, c.params.Version)
	if err != nil {
		return nil, err
	}
	return client.Pids(ctx, req)
}
func (c *sandboxTaskClient) Pause(ctx context.Context, req *api.PauseRequest) (*emptypb.Empty, error) {
	if c.guestID != "" {
		req = proto.Clone(req).(*api.PauseRequest)
		req.ID = c.guestID
	}
	conn, err := makeConnection(ctx, c.id, c.params, func() {})
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client, err := NewTaskClient(conn, c.params.Version)
	if err != nil {
		return nil, err
	}
	return client.Pause(ctx, req)
}
func (c *sandboxTaskClient) Resume(ctx context.Context, req *api.ResumeRequest) (*emptypb.Empty, error) {
	if c.guestID != "" {
		req = proto.Clone(req).(*api.ResumeRequest)
		req.ID = c.guestID
	}
	conn, err := makeConnection(ctx, c.id, c.params, func() {})
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client, err := NewTaskClient(conn, c.params.Version)
	if err != nil {
		return nil, err
	}
	return client.Resume(ctx, req)
}
func (c *sandboxTaskClient) Checkpoint(ctx context.Context, req *api.CheckpointTaskRequest) (*emptypb.Empty, error) {
	if c.guestID != "" {
		req = proto.Clone(req).(*api.CheckpointTaskRequest)
		req.ID = c.guestID
	}
	conn, err := makeConnection(ctx, c.id, c.params, func() {})
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client, err := NewTaskClient(conn, c.params.Version)
	if err != nil {
		return nil, err
	}
	return client.Checkpoint(ctx, req)
}
func (c *sandboxTaskClient) Kill(ctx context.Context, req *api.KillRequest) (*emptypb.Empty, error) {
	if c.guestID != "" {
		req = proto.Clone(req).(*api.KillRequest)
		req.ID = c.guestID
	}
	conn, err := makeConnection(ctx, c.id, c.params, func() {})
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client, err := NewTaskClient(conn, c.params.Version)
	if err != nil {
		return nil, err
	}
	return client.Kill(ctx, req)
}
func (c *sandboxTaskClient) Exec(ctx context.Context, req *api.ExecProcessRequest) (*emptypb.Empty, error) {
	defer traceSandboxRPC(ctx, "Exec", req.ID, req.ExecID)()
	if c.guestID != "" {
		req = proto.Clone(req).(*api.ExecProcessRequest)
		req.ID = c.guestID
	}
	conn, err := makeConnection(ctx, c.id, c.params, func() {})
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client, err := NewTaskClient(conn, c.params.Version)
	if err != nil {
		return nil, err
	}
	return client.Exec(ctx, req)
}
func (c *sandboxTaskClient) ResizePty(ctx context.Context, req *api.ResizePtyRequest) (*emptypb.Empty, error) {
	if c.guestID != "" {
		req = proto.Clone(req).(*api.ResizePtyRequest)
		req.ID = c.guestID
	}
	conn, err := makeConnection(ctx, c.id, c.params, func() {})
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client, err := NewTaskClient(conn, c.params.Version)
	if err != nil {
		return nil, err
	}
	return client.ResizePty(ctx, req)
}
func (c *sandboxTaskClient) CloseIO(ctx context.Context, req *api.CloseIORequest) (*emptypb.Empty, error) {
	if c.guestID != "" {
		req = proto.Clone(req).(*api.CloseIORequest)
		req.ID = c.guestID
	}
	conn, err := makeConnection(ctx, c.id, c.params, func() {})
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client, err := NewTaskClient(conn, c.params.Version)
	if err != nil {
		return nil, err
	}
	return client.CloseIO(ctx, req)
}
func (c *sandboxTaskClient) Update(ctx context.Context, req *api.UpdateTaskRequest) (*emptypb.Empty, error) {
	if c.guestID != "" {
		req = proto.Clone(req).(*api.UpdateTaskRequest)
		req.ID = c.guestID
	}
	conn, err := makeConnection(ctx, c.id, c.params, func() {})
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client, err := NewTaskClient(conn, c.params.Version)
	if err != nil {
		return nil, err
	}
	return client.Update(ctx, req)
}
func (c *sandboxTaskClient) Wait(ctx context.Context, req *api.WaitRequest) (*api.WaitResponse, error) {
	defer traceSandboxRPC(ctx, "Wait", req.ID, req.ExecID)()
	if c.guestID != "" {
		req = proto.Clone(req).(*api.WaitRequest)
		req.ID = c.guestID
	}
	// Wait is read-only. A VM transport reset is not a process exit.
	var failedAt time.Time
	for {
		conn, err := makeConnection(ctx, c.id, c.params, func() {})
		var response *api.WaitResponse
		var probeExpired bool
		if err == nil {
			client, e := NewTaskClient(conn, c.params.Version)
			err = e
			if err == nil {
				// A successful state query proves this is a recovered session.
				// Each future fork gets its own bounded reconnect window, rather
				// than exhausting a timer started by the first fork in its lifetime.
				// A stalled State probe is a transport failure, not proof that
				// the container exited. Reconnect promptly within the existing
				// 45-second recovery window; never extend the caller's deadline.
				stateCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
				_, err = client.State(stateCtx, &api.StateRequest{ID: req.ID, ExecID: req.ExecID})
				probeExpired = errors.Is(stateCtx.Err(), context.DeadlineExceeded)
				cancel()
				if err == nil {
					failedAt = time.Time{}
					response, err = client.Wait(ctx, req)
				}
			}
			conn.Close()
		}
		if err == nil {
			return response, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if !(probeExpired || status.Code(err) == codes.DeadlineExceeded || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ttrpc.ErrClosed) || errors.Is(err, io.EOF) || status.Code(err) == codes.Unavailable || errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) || strings.Contains(err.Error(), "Task lifecycle is frozen for VM fork")) {
			return nil, err
		}
		if failedAt.IsZero() {
			failedAt = time.Now()
		}
		if time.Since(failedAt) > 45*time.Second {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (c *sandboxTaskClient) Stats(ctx context.Context, req *api.StatsRequest) (*api.StatsResponse, error) {
	if c.guestID != "" {
		req = proto.Clone(req).(*api.StatsRequest)
		req.ID = c.guestID
	}
	conn, err := makeConnection(ctx, c.id, c.params, func() {})
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client, err := NewTaskClient(conn, c.params.Version)
	if err != nil {
		return nil, err
	}
	return client.Stats(ctx, req)
}
func (c *sandboxTaskClient) Connect(ctx context.Context, req *api.ConnectRequest) (*api.ConnectResponse, error) {
	if c.guestID != "" {
		req = proto.Clone(req).(*api.ConnectRequest)
		req.ID = c.guestID
	}
	conn, err := makeConnection(ctx, c.id, c.params, func() {})
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client, err := NewTaskClient(conn, c.params.Version)
	if err != nil {
		return nil, err
	}
	return client.Connect(ctx, req)
}
func (c *sandboxTaskClient) Shutdown(ctx context.Context, req *api.ShutdownRequest) (*emptypb.Empty, error) {
	if c.guestID != "" {
		req = proto.Clone(req).(*api.ShutdownRequest)
		req.ID = c.guestID
	}
	conn, err := makeConnection(ctx, c.id, c.params, func() {})
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client, err := NewTaskClient(conn, c.params.Version)
	if err != nil {
		return nil, err
	}
	return client.Shutdown(ctx, req)
}
