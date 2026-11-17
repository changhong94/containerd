// Copyright The containerd Authors.
// SPDX-License-Identifier: Apache-2.0
package v2

import (
	"context"
	"errors"
	"sync"
	"testing"

	api "github.com/containerd/containerd/api/runtime/task/v3"
	"github.com/containerd/containerd/v2/core/runtime"
	"github.com/containerd/containerd/v2/core/sandbox"
	"github.com/containerd/errdefs"
)

type forkStore struct {
	sandbox.Store
	sb sandbox.Sandbox
}

func (s *forkStore) Get(context.Context, string) (sandbox.Sandbox, error) { return s.sb, nil }

type forkController struct {
	sandbox.Controller
	address string
}

func (s *forkController) Status(context.Context, string, bool) (sandbox.ControllerStatus, error) {
	return sandbox.ControllerStatus{Address: s.address}, nil
}

type forkTask struct {
	TaskServiceClient
	states, creates int
}

func (s *forkTask) State(context.Context, *api.StateRequest) (*api.StateResponse, error) {
	s.states++
	return &api.StateResponse{Pid: 123}, nil
}
func (s *forkTask) Create(context.Context, *api.CreateTaskRequest) (*api.CreateTaskResponse, error) {
	s.creates++
	return nil, errors.New("must not create")
}
func TestForkAdoption(t *testing.T) {
	ctx := context.Background()
	sb := sandbox.Sandbox{ID: "sandbox", Labels: map[string]string{"io.kuasar.containerd.namespace": "child"}}
	if err := sb.AddExtension(TasksKey, &Tasks{Tasks: []Task{{TaskID: "inherited"}}}); err != nil {
		t.Fatal(err)
	}
	client := &forkTask{}
	task := &sandboxedTask{namespace: "child", sandboxHandle: &sandboxClient{id: sb.ID, store: &forkStore{sb: sb}, controller: &forkController{address: "ttrpc+hvsock:///child:1024"}, mu: &sync.Mutex{}}, remoteTask: &remoteTask{id: "inherited", client: client}}
	opts := runtime.CreateOpts{Address: "ttrpc+hvsock:///child:1024"}
	if err := task.adopt(ctx, opts); err != nil {
		t.Fatal(err)
	}
	if client.states != 1 || client.creates != 0 {
		t.Fatalf("unexpected calls: %+v", client)
	}
	task.namespace = "parent"
	if err := task.adopt(ctx, opts); !errdefs.IsFailedPrecondition(err) {
		t.Fatalf("wrong namespace: %v", err)
	}
	task.namespace = "child"
	opts.Address = "ttrpc+hvsock:///parent:1024"
	if err := task.adopt(ctx, opts); !errdefs.IsInvalidArgument(err) {
		t.Fatalf("wrong endpoint: %v", err)
	}
	opts.Address = "ttrpc+hvsock:///child:1024"
	opts.IO.Stdout = "host-fifo"
	if err := task.adopt(ctx, opts); !errdefs.IsInvalidArgument(err) {
		t.Fatalf("external IO: %v", err)
	}
	opts.IO.Stdout = ""
	task.id = "unregistered"
	if err := task.adopt(ctx, opts); !errdefs.IsNotFound(err) {
		t.Fatalf("missing inventory: %v", err)
	}
	if client.states != 1 || client.creates != 0 {
		t.Fatalf("invalid adoption contacted guest: %+v", client)
	}
}

type rollbackStore struct{ forkStore }

func (s *rollbackStore) Update(_ context.Context, sb sandbox.Sandbox, _ ...string) (sandbox.Sandbox, error) {
	s.sb = sb
	return sb, nil
}

type failingController struct{ forkController }

func (*failingController) Update(context.Context, string, sandbox.Sandbox, ...string) error {
	return errors.New("controller rejected update")
}
func TestForkTaskMetadataRollback(t *testing.T) {
	sb := sandbox.Sandbox{ID: "sandbox"}
	if err := sb.AddExtension(TasksKey, &Tasks{Tasks: []Task{{TaskID: "inherited"}}}); err != nil {
		t.Fatal(err)
	}
	store := &rollbackStore{forkStore{sb: sb}}
	client := &sandboxClient{id: sb.ID, store: store, controller: &failingController{}, mu: &sync.Mutex{}}
	if err := client.UpdateTasksExtension(context.Background(), func(tasks *Tasks) error { tasks.removeTask("inherited"); return nil }); err == nil {
		t.Fatal("expected controller failure")
	}
	var tasks Tasks
	if err := store.sb.GetExtension(TasksKey, &tasks); err != nil {
		t.Fatal(err)
	}
	if len(tasks.Tasks) != 1 || tasks.Tasks[0].TaskID != "inherited" {
		t.Fatalf("lost inventory after failure: %+v", tasks)
	}
}
