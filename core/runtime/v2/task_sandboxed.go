/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package v2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/containerd/containerd/api/runtime/task/v3"
	"github.com/containerd/containerd/api/types"
	"github.com/containerd/containerd/v2/core/metadata"
	"github.com/containerd/containerd/v2/core/runtime"
	"github.com/containerd/containerd/v2/core/sandbox"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/containerd/containerd/v2/pkg/protobuf"
	shimclient "github.com/containerd/containerd/v2/pkg/shim"
	"github.com/containerd/containerd/v2/plugins"
	"github.com/containerd/errdefs"
	"github.com/containerd/log"
	"github.com/containerd/plugin"
	"github.com/containerd/ttrpc"
	"github.com/containerd/typeurl/v2"
	"github.com/opencontainers/runtime-spec/specs-go"
)

const (
	// TasksKey is the key used for storing tasks information in the sandbox extensions
	TasksKey = "tasks"
	// Negotiated controller-owned inventory for live exec processes.
	runtimeTasksInfo = "io.kuasar.runtime-tasks.v1"
)

func init() {
	typeurl.Register(&Tasks{}, "github.com/containerd/containerd", "Tasks")
}

var ErrCanNotHandle = errors.New("can not handle this task")

type SandboxedTaskManager struct {
	sandboxStore       sandbox.Store
	sandboxControllers map[string]sandbox.Controller
	tasks              *runtime.NSMap[*sandboxedTask]
	sandboxLocks       sync.Map
}

func NewSandboxedTaskManager(ic *plugin.InitContext) (*SandboxedTaskManager, error) {
	m, err := ic.GetSingle(plugins.MetadataPlugin)
	if err != nil {
		return nil, err
	}
	sandboxStore := metadata.NewSandboxStore(m.(*metadata.DB))

	scs, err := ic.GetByType(plugins.SandboxControllerPlugin)
	if err != nil {
		return nil, err
	}
	sandboxControllers := make(map[string]sandbox.Controller)
	for name, p := range scs {
		sandboxControllers[name] = p.(sandbox.Controller)
	}

	return &SandboxedTaskManager{
		sandboxStore:       sandboxStore,
		sandboxControllers: sandboxControllers,
		tasks:              runtime.NewNSMap[*sandboxedTask](),
	}, nil
}

func (s *SandboxedTaskManager) Create(ctx context.Context, taskID string, bundle *Bundle, opts runtime.CreateOpts) (runtime.Task, error) {
	if len(opts.SandboxID) == 0 {
		return nil, fmt.Errorf("no sandbox id specified for task %s", taskID)
	}
	sb, err := s.loadSandbox(ctx, opts.SandboxID)
	if err != nil {
		return nil, err
	}

	if _, err := namespaces.NamespaceRequired(ctx); err != nil {
		return nil, err
	}

	// Write sandbox ID this task belongs to.
	if err := os.WriteFile(filepath.Join(bundle.Path, "sandbox"), []byte(opts.SandboxID), 0600); err != nil {
		return nil, err
	}

	if opts.Address == "" {
		return nil, fmt.Errorf("address of task api should not be empty for sandboxed task")
	}

	// The address returned from sandbox controller should be in the form like ttrpc+unix://<uds-path>
	// or grpc+vsock://<cid>:<port>, we should get the protocol from the url first.
	protocol, address, ok := strings.Cut(opts.Address, "+")
	if !ok {
		return nil, fmt.Errorf("the scheme of sandbox address should be in " +
			" the form of <protocol>+<unix|vsock|tcp>, i.e. ttrpc+unix or grpc+vsock")
	}
	params := shimclient.BootstrapParams{
		Version:  int(opts.Version),
		Protocol: protocol,
		Address:  address,
	}

	// Save bootstrap configuration (so containerd can restore shims after restart).
	if err := writeBootstrapParams(filepath.Join(bundle.Path, "bootstrap.json"), params); err != nil {
		return nil, fmt.Errorf("failed to write bootstrap.json: %w", err)
	}

	sandboxedTask, err := newSandboxedTask(ctx, sb, taskID, bundle, params)
	if err != nil {
		return nil, fmt.Errorf("failed to new sandboxed task: %w", err)
	}
	var spec oci.Spec
	if err := typeurl.UnmarshalTo(opts.Spec, &spec); err != nil {
		return nil, err
	}
	if spec.Annotations["io.kuasar.fork.adopt"] == "true" {
		err = sandboxedTask.adopt(ctx, opts)
	} else {
		err = sandboxedTask.Create(ctx, bundle.Path, opts)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create sandboxed task: %w", err)
	}
	if err := s.tasks.Add(ctx, sandboxedTask); err != nil {
		return nil, err
	}
	return sandboxedTask, nil
}

func (s *SandboxedTaskManager) Load(ctx context.Context, sandboxID string, bundle *Bundle) error {
	sb, err := s.loadSandbox(ctx, sandboxID)
	if err != nil {
		return fmt.Errorf("failed to get sandbox %s: %w", sandboxID, err)
	}

	filePath := filepath.Join(bundle.Path, "bootstrap.json")
	params, err := readBootstrapParams(filePath)
	if err != nil {
		return fmt.Errorf("failed to restore connection parameter from %s: %w", filePath, err)
	}

	sandboxedTask, err := newSandboxedTask(ctx, sb, bundle.ID, bundle, params)
	if err != nil {
		return fmt.Errorf("failed to new sandboxed task: %w", err)
	}
	return s.tasks.Add(ctx, sandboxedTask)
}

func (s *SandboxedTaskManager) Get(ctx context.Context, id string) (runtime.Task, error) {
	return s.tasks.Get(ctx, id)
}

func (s *SandboxedTaskManager) GetAll(ctx context.Context, all bool) ([]runtime.Task, error) {
	var tasks []runtime.Task
	ts, err := s.tasks.GetAll(ctx, all)
	if err != nil {
		return tasks, err
	}
	for _, t := range ts {
		tasks = append(tasks, t)
	}
	return tasks, nil
}

func (s *SandboxedTaskManager) Delete(ctx context.Context, taskID string) (*runtime.Exit, error) {
	st, err := s.tasks.Get(ctx, taskID)
	if err != nil {
		return nil, err
	}

	resp, taskErr := st.client.Delete(ctx, &task.DeleteRequest{
		ID: taskID,
	})
	if taskErr != nil {
		log.G(ctx).WithField("id", taskID).WithError(taskErr).Debug("failed to delete task")
		// A reset during VM fork is not proof that the guest task was deleted.
		if _, liveFork := st.client.(*sandboxTaskClient); liveFork && errors.Is(taskErr, ttrpc.ErrClosed) {
			return nil, taskErr
		}
		if !errors.Is(taskErr, ttrpc.ErrClosed) {
			taskErr = errdefs.FromGRPC(taskErr)
			if !errdefs.IsNotFound(taskErr) {
				return nil, taskErr
			}
		}
	}

	if err := st.sandboxHandle.UpdateTasksExtension(ctx, func(ts *Tasks) error {
		ts.removeTask(st.guestID())
		return nil
	}); err != nil {
		return nil, err
	}

	if err := st.bundle.Delete(); err != nil {
		log.G(ctx).WithField("id", taskID).WithError(err).Error("failed to delete bundle")
	}

	s.tasks.Delete(ctx, taskID)

	if taskErr != nil {
		return nil, errdefs.ErrNotFound
	}
	return &runtime.Exit{
		Status:    resp.ExitStatus,
		Timestamp: protobuf.FromTimestamp(resp.ExitedAt),
		Pid:       resp.Pid,
	}, nil
}

// loadSandbox loads sandboxes created by sandbox controller,
// so the pause container and the podsandbox is excluded.
func (s *SandboxedTaskManager) loadSandbox(ctx context.Context, sandboxID string) (*sandboxClient, error) {
	sb, err := s.sandboxStore.Get(ctx, sandboxID)
	if err != nil {
		// If the sandbox is created in a previous version,
		// there is a possibility that it is stored in db as a pause container rather than a sandbox,
		// so we can only return ErrCanNotHandle here so that TaskManager can fallback to the legacy logic.
		if errdefs.IsNotFound(err) {
			return nil, ErrCanNotHandle
		}
		return nil, fmt.Errorf("failed to get sandbox %s: %w", sandboxID, err)
	}
	if sb.Sandboxer == "podsandbox" {
		return nil, ErrCanNotHandle
	}
	sbController, ok := s.sandboxControllers[sb.Sandboxer]
	if !ok {
		return nil, fmt.Errorf("can not find sandbox controller by %s", sb.Sandboxer)
	}
	ns, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		return nil, err
	}
	lock, _ := s.sandboxLocks.LoadOrStore(ns+"/"+sandboxID, &sync.Mutex{})
	return &sandboxClient{
		mu:         lock.(*sync.Mutex),
		id:         sandboxID,
		store:      s.sandboxStore,
		controller: sbController,
	}, nil
}

func newSandboxedTask(
	ctx context.Context,
	sandboxHandle *sandboxClient,
	taskID string,
	bundle *Bundle,
	params shimclient.BootstrapParams) (*sandboxedTask, error) {
	ns, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		return nil, err
	}

	conn, err := makeConnection(ctx, taskID, params, func() {})

	if err != nil {
		return nil, fmt.Errorf("can not connect %v: %w", params, err)
	}

	taskClient, err := NewTaskClient(conn, params.Version)
	if err != nil {
		return nil, err
	}
	// A VM fork resets vsock streams. Use fresh connections per RPC instead
	// of retaining a dead client or replaying a possibly completed mutation.
	if strings.HasPrefix(params.Address, "hvsock://") {
		sandboxHandle.runtimeTasks = true
		conn.Close()
		var spec oci.Spec
		raw, err := os.ReadFile(filepath.Join(bundle.Path, "config.json"))
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(raw, &spec); err != nil {
			return nil, err
		}
		taskClient = &sandboxTaskClient{id: taskID, guestID: spec.Annotations["io.kuasar.fork.guest-id"], params: params}
	}
	t := &sandboxedTask{
		namespace:     ns,
		sandboxHandle: sandboxHandle,
		remoteTask: &remoteTask{
			id:     taskID,
			client: taskClient,
		},
		bundle: bundle,
	}

	return t, nil
}

// sandboxedTask is a wrapped task that is running in a sandbox.
type sandboxedTask struct {
	namespace     string
	sandboxHandle *sandboxClient
	bundle        *Bundle
	*remoteTask
}

type sandboxClient struct {
	runtimeTasks bool
	id           string
	store        sandbox.Store
	controller   sandbox.Controller
	mu           *sync.Mutex
}

func (s *sandboxedTask) ID() string {
	return s.remoteTask.id
}

func (s *sandboxedTask) Namespace() string {
	return s.namespace
}

func (s *sandboxedTask) guestID() string {
	if client, ok := s.client.(*sandboxTaskClient); ok && client.guestID != "" {
		return client.guestID
	}
	return s.id
}

// Create wrap the Create API call of task with update of the sandbox extension
func (s *sandboxedTask) Create(ctx context.Context, bundle string, opts runtime.CreateOpts) error {
	if err := s.sandboxHandle.UpdateTasksExtension(ctx, func(ts *Tasks) error {
		var spec oci.Spec
		if err := typeurl.UnmarshalTo(opts.Spec, &spec); err != nil {
			return err
		}
		var rootfs []*types.Mount
		for _, m := range opts.Rootfs {
			rootfs = append(rootfs, &types.Mount{
				Type:    m.Type,
				Source:  m.Source,
				Target:  m.Target,
				Options: m.Options,
			})
		}
		ts.addTask(Task{
			TaskID:    s.guestID(),
			Spec:      &spec,
			Rootfs:    rootfs,
			Stdin:     opts.IO.Stdin,
			Stdout:    opts.IO.Stdout,
			Stderr:    opts.IO.Stderr,
			Processes: nil,
		})
		return nil
	}); err != nil {
		return err
	}
	// Then call Task Create api in sandbox to create the task inside sandbox
	if err := s.remoteTask.Create(ctx, s.bundle.Path, opts); err != nil {
		// if it is error, we need to roll back the change of sandbox
		if e := s.sandboxHandle.UpdateTasksExtension(ctx, func(ts *Tasks) error {
			ts.removeTask(s.guestID())
			return nil
		}); e != nil {
			log.G(ctx).Warnf("failed to rollback task extension %s in sandbox %s: %v", s.ID(), s.sandboxHandle.id, e)
		}
		return err
	}
	return nil
}

// Exec wrap the Exec API call with update of the sandbox extension
func (s *sandboxedTask) Exec(ctx context.Context, id string, opts runtime.ExecOpts) (runtime.ExecProcess, error) {
	if err := s.sandboxHandle.updateProcessesExtension(ctx, func(ts *Tasks) error {
		return ts.updateTask(s.guestID(), func(t *Task) error {
			var spec specs.Process
			if err := typeurl.UnmarshalTo(opts.Spec, &spec); err != nil {
				return err
			}
			t.addProcess(Process{
				ExecID: id,
				Spec:   &spec,
				Stdin:  opts.IO.Stdin,
				Stdout: opts.IO.Stdout,
				Stderr: opts.IO.Stderr,
			})
			return nil
		})
	}); err != nil {
		return nil, err
	}

	p, err := s.remoteTask.Exec(ctx, id, opts)
	if err != nil {
		if removeErr := s.sandboxHandle.updateProcessesExtension(ctx, func(ts *Tasks) error {
			return ts.updateTask(s.guestID(), func(t *Task) error {
				t.removeProcess(id)
				return nil
			})
		}); removeErr != nil {
			log.G(ctx).Warnf("failed to remove exec task resource %s %s in sandbox %s: %v", s.ID(), id, s.sandboxHandle.id, removeErr)
		}
		return nil, err
	}
	return &sandboxedProcess{
		ExecProcess: p,
		task:        s,
	}, nil
}

// Process also wraps inherited exec processes, which never went through Exec here.
func (s *sandboxedTask) Process(ctx context.Context, id string) (runtime.ExecProcess, error) {
	p, err := s.remoteTask.Process(ctx, id)
	if err != nil {
		return nil, err
	}
	return &sandboxedProcess{ExecProcess: p, task: s}, nil
}

// adopt registers an existing guest Task without Create/Start or checkpoint IO.
func (s *sandboxedTask) adopt(ctx context.Context, opts runtime.CreateOpts) error {
	sb, err := s.sandboxHandle.store.Get(ctx, s.sandboxHandle.id)
	if err != nil {
		return err
	}
	if sb.Labels["io.kuasar.containerd.namespace"] != s.namespace {
		return fmt.Errorf("sandbox is not bound to namespace %s: %w", s.namespace, errdefs.ErrFailedPrecondition)
	}
	status, err := s.sandboxHandle.controller.Status(ctx, sb.ID, false)
	if err != nil {
		return err
	}
	if status.Address != opts.Address {
		return fmt.Errorf("adoption endpoint does not match sandbox: %w", errdefs.ErrInvalidArgument)
	}
	if len(opts.Rootfs) != 0 || opts.IO.Terminal || opts.IO.Stdin != "" || opts.IO.Stdout != "" || opts.IO.Stderr != "" {
		return fmt.Errorf("adoption requires inherited rootfs and empty IO: %w", errdefs.ErrInvalidArgument)
	}
	var inventory Tasks
	if err := sb.GetExtension(TasksKey, &inventory); err != nil {
		return err
	}
	if inventory.findTaskByID(s.guestID()) < 0 {
		return fmt.Errorf("task absent from inherited inventory: %w", errdefs.ErrNotFound)
	}
	_, err = s.remoteTask.State(ctx)
	return err
}

// sandboxedProcess wrapped exec process that is running in a sandbox.
type sandboxedProcess struct {
	runtime.ExecProcess
	task *sandboxedTask
}

// Delete wrap the Delete of the process with sandbox update
func (p *sandboxedProcess) Delete(ctx context.Context) (*runtime.Exit, error) {
	exit, err := p.ExecProcess.Delete(ctx)
	if err != nil && !errdefs.IsNotFound(err) {
		return nil, err
	}
	err = p.task.sandboxHandle.updateProcessesExtension(ctx, func(ts *Tasks) error {
		return ts.updateTask(p.task.guestID(), func(t *Task) error {
			t.removeProcess(p.ID())
			return nil
		})
	})
	if err != nil {
		return nil, errdefs.FromGRPC(err)
	}
	return exit, nil
}

func traceTaskUpdate(ctx context.Context, phase string, started time.Time) {
	if os.Getenv("KUASAR_RPC_TIMING") == "1" {
		log.G(ctx).WithField("phase", phase).WithField("milliseconds", time.Since(started).Seconds()*1000).Info("fork timing: task metadata")
	}
}

// UpdateTasksExtension update the extension of sandbox with the key "tasks"
func (s *sandboxClient) UpdateTasksExtension(ctx context.Context, update func(ts *Tasks) error) error {
	return s.updateTasksExtension(ctx, update, false)
}

// Exec processes are runtime state, owned by the negotiated controller. Query
// that owner on every mutation, including after containerd restarts, instead of
// maintaining a second synchronous Bolt copy of every short-lived command.
// Container definitions and sandbox bindings still use the durable store.
func (s *sandboxClient) updateProcessesExtension(ctx context.Context, update func(ts *Tasks) error) error {
	return s.updateTasksExtension(ctx, update, true)
}

func (s *sandboxClient) updateTasksExtension(ctx context.Context, update func(ts *Tasks) error, processOnly bool) error {
	started := time.Now()
	s.mu.Lock()
	traceTaskUpdate(ctx, "lock", started)
	defer s.mu.Unlock()
	var tasks Tasks
	started = time.Now()
	sb, err := s.store.Get(ctx, s.id)
	traceTaskUpdate(ctx, "load", started)
	if err != nil {
		return err
	}
	old := sb
	old.Extensions = make(map[string]typeurl.Any, len(sb.Extensions))
	for key, value := range sb.Extensions {
		old.Extensions[key] = value
	}
	err = sb.GetExtension(TasksKey, &tasks)
	if err != nil && !errdefs.IsNotFound(err) {
		return err
	}
	runtimeOwned := false
	if s.runtimeTasks {
		status, statusErr := s.controller.Status(ctx, s.id, true)
		if statusErr != nil {
			return statusErr
		}
		if raw, ok := status.Info[runtimeTasksInfo]; ok {
			// A fresh value avoids losing concurrent tasks when a later container
			// mutation persists its structural inventory. Invalid data fails closed.
			var current Tasks
			if err := json.Unmarshal([]byte(raw), &current); err != nil {
				return fmt.Errorf("invalid runtime task inventory: %w", err)
			}
			tasks = current
			runtimeOwned = true
		}
	}
	if err = update(&tasks); err != nil {
		return err
	}
	if err = sb.AddExtension(TasksKey, &tasks); err != nil {
		return err
	}
	if processOnly && runtimeOwned {
		started = time.Now()
		err = s.controller.Update(ctx, sb.ID, sb, "extensions."+TasksKey)
		traceTaskUpdate(ctx, "runtime_controller", started)
		return err
	}
	started = time.Now()
	_, err = s.store.Update(ctx, sb, "extensions."+TasksKey)
	traceTaskUpdate(ctx, "persist", started)
	if err != nil {
		return err
	}
	started = time.Now()
	err = s.controller.Update(ctx, sb.ID, sb, "extensions."+TasksKey)
	traceTaskUpdate(ctx, "controller", started)
	if err != nil {
		// Rollback the change in sandbox store if controller update failed
		if _, err := s.store.Update(ctx, old, "extensions."+TasksKey); err != nil {
			log.G(ctx).Warnf("failed to rollback when update tasks of sandbox %s extensions: %v", s.id, err)
		}
		return err
	}
	return nil
}

// Tasks is the task information list that is stored in sandbox metadata
type Tasks struct {
	Tasks []Task `json:"tasks,omitempty"`
}

func (ts *Tasks) addTask(t Task) {
	ts.Tasks = append(ts.Tasks, t)
}

func (ts *Tasks) removeTask(taskID string) {
	idx := ts.findTaskByID(taskID)
	if idx < 0 {
		return
	}
	ts.Tasks = append(ts.Tasks[:idx], ts.Tasks[idx+1:]...)
}

func (ts *Tasks) updateTask(taskID string, update func(*Task) error) error {
	idx := ts.findTaskByID(taskID)
	if idx < 0 {
		return errdefs.ErrNotFound
	}
	if err := update(&ts.Tasks[idx]); err != nil {
		return err
	}
	return nil
}

func (ts *Tasks) findTaskByID(taskID string) int {
	for idx, t := range ts.Tasks {
		if t.TaskID == taskID {
			return idx
		}
	}
	return -1
}

// Task is the information of a task that is stored in sandbox metadata.
type Task struct {
	TaskID string `json:"task_id,omitempty"`
	// Spec is the oci spec of the task
	Spec *oci.Spec `json:"spec,omitempty"`
	// Rootfs is the mount information of the task's rootfs.
	Rootfs []*types.Mount `json:"rootfs,omitempty"`
	Stdin  string         `json:"stdin,omitempty"`
	Stdout string         `json:"stdout,omitempty"`
	Stderr string         `json:"stderr,omitempty"`
	// Processes is information of processes that is running under the task
	Processes []Process `json:"processes,omitempty"`
}

func (t *Task) addProcess(process Process) {
	t.Processes = append(t.Processes, process)
}

func (t *Task) removeProcess(execID string) {
	idx := t.findProcessByID(execID)
	if idx < 0 {
		return
	}
	t.Processes = append(t.Processes[:idx], t.Processes[idx+1:]...)
}

func (t *Task) findProcessByID(execID string) int {
	for idx, p := range t.Processes {
		if p.ExecID == execID {
			return idx
		}
	}
	return -1
}

// Process is the information of a process that is running inside a sandbox
type Process struct {
	ExecID string         `json:"exec_id,omitempty"`
	Spec   *specs.Process `json:"spec,omitempty"`
	Stdin  string         `json:"stdin,omitempty"`
	Stdout string         `json:"stdout,omitempty"`
	Stderr string         `json:"stderr,omitempty"`
}
