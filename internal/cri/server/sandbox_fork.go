// Copyright The containerd Authors.
// SPDX-License-Identifier: Apache-2.0
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/containerd/errdefs"
	"github.com/containerd/log"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	v2 "github.com/containerd/containerd/v2/core/runtime/v2"
	sb "github.com/containerd/containerd/v2/core/sandbox"
	cio "github.com/containerd/containerd/v2/internal/cri/io"
	crilabels "github.com/containerd/containerd/v2/internal/cri/labels"
	"github.com/containerd/containerd/v2/internal/cri/server/podsandbox"
	containerstore "github.com/containerd/containerd/v2/internal/cri/store/container"
	sandboxstore "github.com/containerd/containerd/v2/internal/cri/store/sandbox"
	"github.com/containerd/containerd/v2/internal/cri/util"
	containerdio "github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/containerd/v2/pkg/netns"
	runtime "k8s.io/cri-api/pkg/apis/runtime/v1"
)

const forkSource = "io.kuasar.fork.source"
const forkPlan = "io.kuasar.fork.plan"

type inheritedContainer struct {
	GuestID, ID string
	Config      *runtime.ContainerConfig
}
type forkAdminResponse struct {
	OK      bool     `json:"ok"`
	Error   string   `json:"error"`
	Address string   `json:"task_address"`
	Tasks   v2.Tasks `json:"tasks"`
}

func criForkAdmin(req map[string]interface{}) (forkAdminResponse, error) {
	address := os.Getenv("KUASAR_FORK_ADMIN_ADDRESS")
	if address == "" {
		return forkAdminResponse{}, fmt.Errorf("CRI fork is disabled: KUASAR_FORK_ADMIN_ADDRESS is unset")
	}
	conn, err := net.DialTimeout("unix", address, 5*time.Second)
	if err != nil {
		return forkAdminResponse{}, err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(90 * time.Second))
	if err = json.NewEncoder(conn).Encode(req); err != nil {
		return forkAdminResponse{}, err
	}
	var r forkAdminResponse
	if err = json.NewDecoder(conn).Decode(&r); err != nil {
		return r, err
	}
	if !r.OK {
		return r, fmt.Errorf("Kuasar fork: %s", r.Error)
	}
	return r, nil
}
func (c *criService) runForkPod(ctx context.Context, r *runtime.RunPodSandboxRequest) (response *runtime.RunPodSandboxResponse, retErr error) {
	config := r.GetConfig()
	started := time.Now()
	previous := started
	mark := func(phase string) {
		now := time.Now()
		if os.Getenv("KUASAR_RPC_TIMING") == "1" {
			log.G(ctx).WithField("pod_uid", config.GetMetadata().GetUid()).WithField("phase", phase).
				WithField("milliseconds", now.Sub(previous).Seconds()*1000).
				WithField("elapsed_ms", now.Sub(started).Seconds()*1000).Info("fork timing: CRI phase")
		}
		previous = now
	}
	defer func() { mark("return") }()
	source := config.GetAnnotations()[forkSource]
	if config.GetMetadata().GetUid() == "" {
		return nil, fmt.Errorf("fork pods require a Pod UID")
	}
	rt, err := c.config.GetSandboxRuntime(config, r.RuntimeHandler)
	if err != nil {
		return nil, err
	}
	if rt.Sandboxer != "vmm" {
		return nil, fmt.Errorf("fork requires vmm sandboxer")
	}
	// Resolve only explicitly shared Pods in the same Kubernetes namespace.
	resolved := false
	for _, parent := range c.sandboxStore.List() {
		if parent.ID != source && parent.Config.GetMetadata().GetUid() != source {
			continue
		}
		if parent.Config.GetMetadata().GetNamespace() != config.GetMetadata().GetNamespace() || parent.Config.GetMetadata().GetUid() == config.GetMetadata().GetUid() || parent.Config.GetAnnotations()["io.kuasar.fork.allow"] != "true" || parent.Status.Get().State != sandboxstore.StateReady {
			return nil, fmt.Errorf("source Pod must be ready, explicitly allow fork, and belong to the same namespace with a different UID")
		}
		source = parent.ID
		resolved = true
		break
	}
	if !resolved {
		for _, template := range strings.Split(os.Getenv("KUASAR_FORK_TEMPLATE_IDS"), ",") {
			if template != "" && template == source {
				resolved = true
				break
			}
		}
	}
	if !resolved {
		return nil, fmt.Errorf("fork source is not an authorized Pod or host-allowlisted template")
	}
	id := util.GenerateID()
	name := makeSandboxName(config.Metadata)
	if err = c.sandboxNameIndex.Reserve(name, id); err != nil {
		return nil, err
	}
	sandbox := sandboxstore.NewSandbox(sandboxstore.Metadata{ID: id, Name: name, Config: config, RuntimeHandler: r.RuntimeHandler}, sandboxstore.Status{State: sandboxstore.StateUnknown, CreatedAt: time.Now().UTC()})
	sandbox.Sandboxer = rt.Sandboxer
	forked, stored, cached := false, false, false
	defer func() {
		if retErr != nil {
			if forked {
				criForkAdmin(map[string]interface{}{"action": "sandbox-destroy", "sandbox_id": id})
			}
			if cached {
				c.sandboxStore.Delete(id)
			}
			if stored {
				c.client.SandboxStore().Delete(ctx, id)
			}
			if sandbox.NetNS != nil {
				c.teardownPodNetwork(context.WithoutCancel(ctx), sandbox)
				sandbox.NetNS.Remove()
			}
			c.sandboxNameIndex.ReleaseByName(name)
		}
	}()
	if !hostNetwork(config) {
		sandbox.NetNS, err = netns.NewNetNS(filepath.Join(c.config.StateDir, "netns"))
		if err != nil {
			return nil, err
		}
		sandbox.NetNSPath = sandbox.NetNS.GetPath()
		if err = c.setupPodNetwork(ctx, &sandbox); err != nil {
			return nil, err
		}
	}
	mark("network_ready")
	if _, err = criForkAdmin(map[string]interface{}{"action": "sandbox-fork", "source_id": source, "sandbox_id": id, "netns": sandbox.NetNSPath}); err != nil {
		return nil, err
	}
	forked = true
	mark("sandbox_forked")
	exported, err := criForkAdmin(map[string]interface{}{"action": "sandbox-export-containerd", "sandbox_id": id, "namespace": "k8s.io"})
	if err != nil {
		return nil, err
	}
	mark("inventory_exported")
	plan := map[string]inheritedContainer{}
	ids := map[string]string{}
	for _, t := range exported.Tasks.Tasks {
		name := t.TaskID
		var inheritedConfig *runtime.ContainerConfig
		for _, parent := range c.containerStore.List() {
			if parent.SandboxID != source {
				continue
			}
			spec, e := parent.Container.Spec(ctx)
			if e != nil {
				return nil, e
			}
			guest := spec.Annotations["io.kuasar.fork.guest-id"]
			if guest == "" {
				guest = parent.ID
			}
			if guest == t.TaskID {
				name = parent.Config.GetMetadata().GetName()
				inheritedConfig = parent.Config
			}
		}
		external := util.GenerateID()
		plan[name] = inheritedContainer{GuestID: t.TaskID, ID: external, Config: inheritedConfig}
		ids[t.TaskID] = external
	}
	if len(plan) == 0 {
		return nil, fmt.Errorf("source has no inherited containers")
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		return nil, err
	}
	config.Annotations[forkPlan] = string(raw)
	exported, err = criForkAdmin(map[string]interface{}{"action": "sandbox-bind-containerd", "sandbox_id": id, "namespace": "k8s.io", "container_ids": ids, "pod_uid": config.Metadata.Uid, "pod_name": config.Metadata.Name, "pod_namespace": config.Metadata.Namespace})
	if err != nil {
		return nil, err
	}
	mark("identities_bound")
	sandbox.Status.Update(func(s sandboxstore.Status) (sandboxstore.Status, error) {
		s.State = sandboxstore.StateReady
		return s, nil
	})
	sandbox.Sandboxer = rt.Sandboxer
	sandbox.Endpoint = sandboxstore.Endpoint{Address: exported.Address, Version: 2}
	info := sb.Sandbox{ID: id, Sandboxer: rt.Sandboxer, Runtime: sb.RuntimeOpts{Name: rt.Type}, Labels: map[string]string{"name": name, "io.kuasar.containerd.namespace": "k8s.io", "io.kubernetes.pod.uid": config.Metadata.Uid}}
	if err = info.AddExtension(podsandbox.MetadataKey, &sandbox.Metadata); err != nil {
		return nil, err
	}
	if err = info.AddExtension(v2.TasksKey, &exported.Tasks); err != nil {
		return nil, err
	}
	if _, err = c.client.SandboxStore().Create(ctx, info); err != nil {
		return nil, err
	}
	stored = true
	mark("metadata_stored")
	status, err := c.sandboxService.SandboxStatus(ctx, rt.Sandboxer, id, false)
	if err != nil {
		return nil, err
	}
	sandbox.Status.Update(func(s sandboxstore.Status) (sandboxstore.Status, error) { s.Pid = status.Pid; return s, nil })
	if err = c.sandboxStore.Add(sandbox); err != nil {
		return nil, err
	}
	cached = true
	mark("status_cached")
	exit, err := c.sandboxService.WaitSandbox(util.NamespacedContext(), rt.Sandboxer, id)
	if err != nil {
		return nil, err
	}
	c.startSandboxExitMonitor(context.Background(), id, exit)
	c.generateAndSendContainerEvent(ctx, id, id, runtime.ContainerEventType_CONTAINER_CREATED_EVENT)
	c.generateAndSendContainerEvent(ctx, id, id, runtime.ContainerEventType_CONTAINER_STARTED_EVENT)
	return &runtime.RunPodSandboxResponse{PodSandboxId: id}, nil
}
func (c *criService) createForkContainer(ctx context.Context, r *runtime.CreateContainerRequest, sandbox sandboxstore.Sandbox) (response *runtime.CreateContainerResponse, retErr error) {
	var plan map[string]inheritedContainer
	if err := json.Unmarshal([]byte(sandbox.Config.Annotations[forkPlan]), &plan); err != nil {
		return nil, err
	}
	entry, ok := plan[r.Config.GetMetadata().GetName()]
	if !ok {
		return nil, fmt.Errorf("container name is absent from inherited inventory")
	}
	if r.Config.GetMetadata().GetAttempt() != 0 {
		return nil, fmt.Errorf("inherited processes cannot be restarted in place; recreate the Pod")
	}
	if err := validateInheritedConfig(r.Config, entry.Config); err != nil {
		return nil, err
	}
	id := entry.ID
	name := makeContainerName(r.Config.Metadata, sandbox.Config.Metadata)
	if err := c.containerNameIndex.Reserve(name, id); err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			c.containerNameIndex.ReleaseByName(name)
		}
	}()
	info, err := c.client.SandboxStore().Get(ctx, sandbox.ID)
	if err != nil {
		return nil, err
	}
	var tasks v2.Tasks
	if err = info.GetExtension(v2.TasksKey, &tasks); err != nil {
		return nil, err
	}
	var inherited *v2.Task
	for i := range tasks.Tasks {
		if tasks.Tasks[i].TaskID == entry.GuestID {
			inherited = &tasks.Tasks[i]
			break
		}
	}
	if inherited == nil || inherited.Spec == nil || inherited.Spec.Process == nil {
		return nil, fmt.Errorf("inherited task missing")
	}
	if entry.Config == nil && len(r.Config.Command) > 0 && !reflect.DeepEqual(append(append([]string{}, r.Config.Command...), r.Config.Args...), inherited.Spec.Process.Args) {
		return nil, fmt.Errorf("requested command differs from inherited template")
	}
	spec := inherited.Spec
	if spec.Process.User.UID != 0 || spec.Process.User.GID != 0 {
		return nil, fmt.Errorf("fork currently requires a root template process")
	}
	if spec.Annotations == nil {
		spec.Annotations = map[string]string{}
	}
	spec.Annotations["io.kuasar.fork.adopt"] = "true"
	spec.Annotations["io.kuasar.fork.guest-id"] = entry.GuestID
	image, err := c.LocalResolve(r.Config.GetImage().GetImage())
	if err != nil {
		return nil, err
	}
	meta := containerstore.Metadata{ID: id, Name: name, SandboxID: sandbox.ID, Config: r.Config, ImageRef: image.ID}
	if sandbox.Config.LogDirectory != "" {
		meta.LogPath = filepath.Join(sandbox.Config.LogDirectory, r.Config.LogPath)
		if err = os.MkdirAll(filepath.Dir(meta.LogPath), 0755); err != nil {
			return nil, err
		}
		if err = os.WriteFile(meta.LogPath, nil, 0600); err != nil {
			return nil, err
		}
	}
	root := c.getContainerRootDir(id)
	if err = os.MkdirAll(root, 0755); err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			os.RemoveAll(root)
			os.RemoveAll(c.getVolatileContainerRootDir(id))
		}
	}()
	if err = os.MkdirAll(c.getVolatileContainerRootDir(id), 0755); err != nil {
		return nil, err
	}
	io, err := cio.NewContainerIO(id, cio.WithFIFOs(&containerdio.FIFOSet{}))
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			io.Close()
		}
	}()
	cntr, err := c.client.NewContainer(ctx, id, containerd.WithSandbox(sandbox.ID), containerd.WithRuntime(info.Runtime.Name, nil), containerd.WithSpec(spec), containerd.WithContainerLabels(buildLabels(r.Config.Labels, nil, crilabels.ContainerKindContainer)), containerd.WithContainerExtension(crilabels.ContainerMetadataExtension, &meta))
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			cntr.Delete(ctx)
		}
	}()
	record, err := containerstore.NewContainer(meta, containerstore.WithStatus(containerstore.Status{CreatedAt: time.Now().UnixNano()}, root), containerstore.WithContainer(cntr), containerstore.WithContainerIO(io))
	if err != nil {
		return nil, err
	}
	if err = c.containerStore.Add(record); err != nil {
		return nil, err
	}
	c.generateAndSendContainerEvent(ctx, id, sandbox.ID, runtime.ContainerEventType_CONTAINER_CREATED_EVENT)
	return &runtime.CreateContainerResponse{ContainerId: id}, nil
}
func (c *criService) startForkContainer(ctx context.Context, cntr containerstore.Container) (*runtime.StartContainerResponse, error) {
	previous := time.Now()
	mark := func(phase string) {
		now := time.Now()
		if os.Getenv("KUASAR_RPC_TIMING") != "" {
			log.G(ctx).WithFields(log.Fields{"id": cntr.ID, "phase": phase, "milliseconds": now.Sub(previous).Seconds() * 1000}).Info("fork timing: container adoption")
		}
		previous = now
	}
	sandbox, err := c.sandboxStore.Get(cntr.SandboxID)
	if err != nil {
		return nil, err
	}
	if sandbox.Status.Get().State != sandboxstore.StateReady {
		return nil, fmt.Errorf("sandbox is not ready")
	}
	if err := setContainerStarting(cntr); err != nil {
		return nil, err
	}
	defer resetContainerStarting(cntr)
	task, err := cntr.Container.Task(ctx, nil)
	if errdefs.IsNotFound(err) {
		task, err = cntr.Container.NewTask(ctx, containerdio.NullIO, containerd.WithTaskAPIEndpoint(sandbox.Endpoint.Address, 2))
	}
	if err != nil {
		return nil, err
	}
	mark("task_attached")
	exit, err := task.Wait(namespaces.WithNamespace(context.Background(), "k8s.io"))
	if err != nil {
		return nil, err
	}
	mark("wait_registered")
	if err = cntr.Status.UpdateSync(func(s containerstore.Status) (containerstore.Status, error) {
		s.Pid = task.Pid()
		s.StartedAt = time.Now().UnixNano()
		return s, nil
	}); err != nil {
		return nil, err
	}
	mark("status_durable")
	c.startContainerExitMonitor(context.Background(), cntr.ID, task.Pid(), exit)
	c.generateAndSendContainerEvent(ctx, cntr.ID, cntr.SandboxID, runtime.ContainerEventType_CONTAINER_STARTED_EVENT)
	mark("event_sent")
	return &runtime.StartContainerResponse{}, nil
}

// A fork keeps the application configuration and guest-resident mounts. Never
// silently apply a different command, image, credentials, devices or host volume.
func validateInheritedConfig(config, source *runtime.ContainerConfig) error {
	if config.Tty || config.Stdin || len(config.Devices) > 0 {
		return fmt.Errorf("inherited main task requires empty IO and no devices")
	}
	for _, mount := range config.Mounts {
		if mount.ContainerPath != "/etc/hosts" && mount.ContainerPath != "/dev/termination-log" {
			return fmt.Errorf("fork does not support host volume %s", mount.ContainerPath)
		}
	}
	if linux := config.GetLinux(); linux != nil {
		sc := linux.GetSecurityContext()
		if sc.GetPrivileged() || sc.GetReadonlyRootfs() || sc.GetRunAsUser().GetValue() != 0 || sc.GetRunAsGroup().GetValue() != 0 || len(sc.GetSupplementalGroups()) > 0 {
			return fmt.Errorf("changing inherited credentials or rootfs policy is unsupported")
		}
		resources := linux.GetResources()
		if resources.GetMemoryLimitInBytes() != 0 || resources.GetCpuQuota() > 0 || resources.GetCpusetCpus() != "" {
			return fmt.Errorf("fork Pod resource limits are not yet supported")
		}
	}
	if source != nil {
		if config.GetImage().GetImage() != source.GetImage().GetImage() || !reflect.DeepEqual(config.Command, source.Command) || !reflect.DeepEqual(config.Args, source.Args) || !reflect.DeepEqual(inheritedEnv(config.Envs), inheritedEnv(source.Envs)) || config.WorkingDir != source.WorkingDir {
			return fmt.Errorf("Pod application configuration differs from inherited source")
		}
	}
	return nil
}

// Kubelet builds service environment entries from maps; ordering is not stable.
func inheritedEnv(env []*runtime.KeyValue) map[string]string {
	result := map[string]string{}
	for _, entry := range env {
		result[entry.Key] = entry.Value
	}
	return result
}
