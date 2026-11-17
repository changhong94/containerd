// Copyright The containerd Authors.
// SPDX-License-Identifier: Apache-2.0
package v2

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/containerd/containerd/v2/core/sandbox"
	"github.com/containerd/typeurl/v2"
)

type runtimeInventoryStore struct {
	rollbackStore
	writes int
}

func (s *runtimeInventoryStore) Get(context.Context, string) (sandbox.Sandbox, error) {
	out := s.sb
	out.Extensions = make(map[string]typeurl.Any, len(s.sb.Extensions))
	for k, v := range s.sb.Extensions {
		out.Extensions[k] = v
	}
	return out, nil
}
func (s *runtimeInventoryStore) Update(_ context.Context, sb sandbox.Sandbox, _ ...string) (sandbox.Sandbox, error) {
	s.writes++
	s.sb = sb
	return sb, nil
}

type runtimeInventoryController struct {
	sandbox.Controller
	tasks     Tasks
	advertise bool
	fail      bool
}

func (c *runtimeInventoryController) Status(_ context.Context, _ string, verbose bool) (sandbox.ControllerStatus, error) {
	if !verbose {
		return sandbox.ControllerStatus{}, errors.New("inventory requires verbose")
	}
	info := map[string]string{}
	if c.advertise {
		b, _ := json.Marshal(&c.tasks)
		info[runtimeTasksInfo] = string(b)
	}
	return sandbox.ControllerStatus{Info: info}, nil
}
func (c *runtimeInventoryController) Update(_ context.Context, _ string, sb sandbox.Sandbox, _ ...string) error {
	if c.fail {
		return errors.New("rejected")
	}
	return sb.GetExtension(TasksKey, &c.tasks)
}

func TestRuntimeExecInventoryRecoveryAndStructuralUpdates(t *testing.T) {
	ctx := context.Background()
	initial := Tasks{Tasks: []Task{{TaskID: "container"}}}
	sb := sandbox.Sandbox{ID: "sandbox"}
	if err := sb.AddExtension(TasksKey, &initial); err != nil {
		t.Fatal(err)
	}
	store := &runtimeInventoryStore{rollbackStore: rollbackStore{forkStore{sb: sb}}}
	controller := &runtimeInventoryController{tasks: initial, advertise: true}
	newClient := func() *sandboxClient {
		return &sandboxClient{id: sb.ID, store: store, controller: controller, mu: &sync.Mutex{}, runtimeTasks: true}
	}
	add := func(id string) func(*Tasks) error {
		return func(ts *Tasks) error {
			return ts.updateTask("container", func(task *Task) error { task.addProcess(Process{ExecID: id}); return nil })
		}
	}
	if err := newClient().updateProcessesExtension(ctx, add("first")); err != nil {
		t.Fatal(err)
	}
	// A new client has no process cache (as after daemon restart). It must load
	// the controller's current inventory, not the stale durable snapshot.
	client := newClient()
	if err := client.updateProcessesExtension(ctx, add("second")); err != nil {
		t.Fatal(err)
	}
	if store.writes != 0 || len(controller.tasks.Tasks[0].Processes) != 2 {
		t.Fatalf("writes=%d tasks=%+v", store.writes, controller.tasks)
	}
	if err := client.UpdateTasksExtension(ctx, func(ts *Tasks) error { ts.addTask(Task{TaskID: "another"}); return nil }); err != nil {
		t.Fatal(err)
	}
	var saved Tasks
	if err := store.sb.GetExtension(TasksKey, &saved); err != nil {
		t.Fatal(err)
	}
	if store.writes != 1 || len(saved.Tasks) != 2 || len(saved.Tasks[0].Processes) != 2 {
		t.Fatalf("lost runtime inventory on structural update: %+v", saved)
	}
	controller.fail = true
	if err := client.updateProcessesExtension(ctx, add("rejected")); err == nil {
		t.Fatal("expected rejection")
	}
	if store.writes != 1 || len(controller.tasks.Tasks[0].Processes) != 2 {
		t.Fatal("rejected mutation became visible")
	}
	controller.fail = false
	if err := newClient().updateProcessesExtension(ctx, func(ts *Tasks) error {
		return ts.updateTask("container", func(task *Task) error { task.removeProcess("first"); return nil })
	}); err != nil {
		t.Fatal(err)
	}
	if len(controller.tasks.Tasks[0].Processes) != 1 || controller.tasks.Tasks[0].Processes[0].ExecID != "second" {
		t.Fatal("process deletion lost another process")
	}
}

func TestRuntimeExecInventoryRequiresNegotiation(t *testing.T) {
	initial := Tasks{Tasks: []Task{{TaskID: "container"}}}
	sb := sandbox.Sandbox{ID: "sandbox"}
	if err := sb.AddExtension(TasksKey, &initial); err != nil {
		t.Fatal(err)
	}
	store := &runtimeInventoryStore{rollbackStore: rollbackStore{forkStore{sb: sb}}}
	controller := &runtimeInventoryController{tasks: initial}
	client := &sandboxClient{id: sb.ID, store: store, controller: controller, mu: &sync.Mutex{}, runtimeTasks: true}
	err := client.updateProcessesExtension(context.Background(), func(ts *Tasks) error {
		return ts.updateTask("container", func(task *Task) error { task.addProcess(Process{ExecID: "exec"}); return nil })
	})
	if err != nil {
		t.Fatal(err)
	}
	if store.writes != 1 {
		t.Fatal("older controller must retain durable fallback")
	}
}

type invalidRuntimeInventoryController struct {
	runtimeInventoryController
	statusErr error
}

func (c *invalidRuntimeInventoryController) Status(context.Context, string, bool) (sandbox.ControllerStatus, error) {
	return sandbox.ControllerStatus{Info: map[string]string{runtimeTasksInfo: "not JSON"}}, c.statusErr
}
func TestRuntimeInventoryErrorsDoNotOverwriteWithStaleStore(t *testing.T) {
	for _, statusErr := range []error{nil, errors.New("disconnected")} {
		sb := sandbox.Sandbox{ID: "sandbox"}
		if err := sb.AddExtension(TasksKey, &Tasks{}); err != nil {
			t.Fatal(err)
		}
		store := &runtimeInventoryStore{rollbackStore: rollbackStore{forkStore{sb: sb}}}
		controller := &invalidRuntimeInventoryController{statusErr: statusErr}
		client := &sandboxClient{id: sb.ID, store: store, controller: controller, mu: &sync.Mutex{}, runtimeTasks: true}
		called := false
		err := client.updateProcessesExtension(context.Background(), func(*Tasks) error { called = true; return nil })
		if err == nil || called || store.writes != 0 {
			t.Fatalf("unsafe fallback: error=%v called=%v writes=%d", err, called, store.writes)
		}
	}
}
