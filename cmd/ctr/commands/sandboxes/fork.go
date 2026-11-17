// Copyright The containerd Authors.
// SPDX-License-Identifier: Apache-2.0
package sandboxes

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/cmd/ctr/commands"
	v2 "github.com/containerd/containerd/v2/core/runtime/v2"
	"github.com/containerd/containerd/v2/core/sandbox"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/errdefs"
	"github.com/urfave/cli/v2"
)

type forkExport struct {
	OK        bool     `json:"ok"`
	Error     string   `json:"error"`
	ID        string   `json:"id"`
	Namespace string   `json:"namespace"`
	Address   string   `json:"task_address"`
	Tasks     v2.Tasks `json:"tasks"`
}

func forkAdmin(address string, request map[string]string) (forkExport, error) {
	conn, err := net.DialTimeout("unix", address, 5*time.Second)
	if err != nil {
		return forkExport{}, err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(90 * time.Second))
	if err = json.NewEncoder(conn).Encode(request); err != nil {
		return forkExport{}, err
	}
	var result forkExport
	if err = json.NewDecoder(conn).Decode(&result); err != nil {
		return result, err
	}
	if !result.OK {
		return result, fmt.Errorf("Kuasar: %s", result.Error)
	}
	return result, nil
}

var importCommand = &cli.Command{
	Name: "import-running", Usage: "Register an existing Kuasar live-fork sandbox and its running tasks (no guest Create/Start)", ArgsUsage: "<sandbox-id>",
	Flags: []cli.Flag{&cli.StringFlag{Name: "admin-address", Required: true}, &cli.StringFlag{Name: "sandboxer", Value: "vmm"}},
	Action: func(c *cli.Context) error {
		if c.NArg() != 1 {
			return fmt.Errorf("one sandbox ID required")
		}
		client, ctx, cancel, err := commands.NewClient(c)
		if err != nil {
			return err
		}
		defer cancel()
		defer client.Close()
		return importRunning(ctx, client, c.String("admin-address"), c.String("sandboxer"), c.Args().First())
	},
}
var forkCommand = &cli.Command{
	Name: "fork", Usage: "Fork a running Kuasar sandbox into another namespace and register inherited tasks", ArgsUsage: "<source-id> <child-id>",
	Flags: []cli.Flag{&cli.StringFlag{Name: "admin-address", Required: true}, &cli.StringFlag{Name: "target-namespace"}},
	Action: func(c *cli.Context) error {
		if c.NArg() != 2 {
			return fmt.Errorf("source and child sandbox IDs required")
		}
		client, ctx, cancel, err := commands.NewClient(c)
		if err != nil {
			return err
		}
		defer cancel()
		defer client.Close()
		source, child := c.Args().Get(0), c.Args().Get(1)
		ns, err := namespaces.NamespaceRequired(ctx)
		if err != nil {
			return err
		}
		target := c.String("target-namespace")
		if target == "" {
			target = "kuasar-fork-" + child
		}
		if target == ns {
			return fmt.Errorf("fork preserves guest container IDs: target namespace must differ")
		}
		parent, err := client.SandboxStore().Get(ctx, source)
		if err != nil {
			return err
		}
		if parent.Labels["io.kuasar.containerd.namespace"] != ns {
			return fmt.Errorf("source is not bound to this namespace")
		}
		childctx := namespaces.WithNamespace(ctx, target)
		if _, err := namespaces.NamespaceRequired(childctx); err != nil {
			return err
		}
		if _, err = client.SandboxStore().Get(childctx, child); !errdefs.IsNotFound(err) {
			return fmt.Errorf("target sandbox must not exist (lookup: %v)", err)
		}
		var tasks v2.Tasks
		if err = parent.GetExtension(v2.TasksKey, &tasks); err != nil {
			return err
		}
		for _, t := range tasks.Tasks {
			if _, err = client.LoadContainer(childctx, t.TaskID); !errdefs.IsNotFound(err) {
				return fmt.Errorf("target container %s must not exist (lookup: %v)", t.TaskID, err)
			}
		}
		if _, err = forkAdmin(c.String("admin-address"), map[string]string{"action": "sandbox-fork", "source_id": source, "sandbox_id": child}); err != nil {
			return err
		}
		if err = importRunning(childctx, client, c.String("admin-address"), parent.Sandboxer, child); err != nil {
			return fmt.Errorf("child %s retained; retry import-running in namespace %s: %w", child, target, err)
		}
		return nil
	},
}

func importRunning(ctx context.Context, client *containerd.Client, admin, sandboxer, id string) error {
	ns, err := namespaces.NamespaceRequired(ctx)
	if err != nil {
		return err
	}
	exported, err := forkAdmin(admin, map[string]string{"action": "sandbox-export-containerd", "sandbox_id": id, "namespace": ns})
	if err != nil {
		return err
	}
	if exported.ID != id || exported.Namespace != ns || exported.Address == "" {
		return fmt.Errorf("invalid sandbox export")
	}
	// Validate collisions before changing the sandbox's event namespace.
	if existing, e := client.SandboxStore().Get(ctx, id); e == nil {
		if existing.Sandboxer != sandboxer || existing.Labels["io.kuasar.containerd.namespace"] != ns {
			return fmt.Errorf("existing sandbox is not owned by this import")
		}
	} else if !errdefs.IsNotFound(e) {
		return e
	}
	for _, t := range exported.Tasks.Tasks {
		if t.Spec == nil {
			return fmt.Errorf("task %s lacks OCI spec", t.TaskID)
		}
		if existing, e := client.LoadContainer(ctx, t.TaskID); e == nil {
			info, e := existing.Info(ctx)
			if e != nil {
				return e
			}
			spec, e := existing.Spec(ctx)
			if e != nil {
				return e
			}
			if info.SandboxID != id || spec.Annotations["io.kuasar.fork.adopt"] != "true" {
				return fmt.Errorf("container ID collision: %s", t.TaskID)
			}
		} else if !errdefs.IsNotFound(e) {
			return e
		}
	}
	exported, err = forkAdmin(admin, map[string]string{"action": "sandbox-bind-containerd", "sandbox_id": id, "namespace": ns})
	if err != nil {
		return err
	}
	if err = client.NamespaceService().Create(ctx, ns, nil); err != nil && !errdefs.IsAlreadyExists(err) {
		return err
	}
	sb := sandbox.Sandbox{ID: id, Sandboxer: sandboxer, Runtime: sandbox.RuntimeOpts{Name: "io.containerd.kuasar-vmm.v1"}, Labels: map[string]string{"io.kuasar.containerd.namespace": ns}}
	if err = sb.AddExtension(v2.TasksKey, &exported.Tasks); err != nil {
		return err
	}
	if _, err = client.SandboxStore().Create(ctx, sb); err != nil {
		if !errdefs.IsAlreadyExists(err) {
			return err
		}
		old, e := client.SandboxStore().Get(ctx, id)
		if e != nil {
			return e
		}
		if old.Sandboxer != sandboxer || old.Labels["io.kuasar.containerd.namespace"] != ns {
			return fmt.Errorf("existing sandbox is not owned by this import")
		}
	}
	for _, task := range exported.Tasks.Tasks {
		if task.Spec == nil {
			return fmt.Errorf("task %s lacks OCI spec", task.TaskID)
		}
		if task.Spec.Annotations == nil {
			task.Spec.Annotations = map[string]string{}
		}
		task.Spec.Annotations["io.kuasar.fork.adopt"] = "true"
		cn, e := client.NewContainer(ctx, task.TaskID, containerd.WithSandbox(id), containerd.WithRuntime(sb.Runtime.Name, nil), containerd.WithSpec(task.Spec))
		if e != nil {
			if !errdefs.IsAlreadyExists(e) {
				return e
			}
			cn, e = client.LoadContainer(ctx, task.TaskID)
			if e != nil {
				return e
			}
			info, e := cn.Info(ctx)
			if e != nil {
				return e
			}
			if info.SandboxID != id {
				return fmt.Errorf("container %s belongs to another sandbox", task.TaskID)
			}
		}
		if _, e = cn.Task(ctx, nil); e == nil {
			continue
		} else if !errdefs.IsNotFound(e) {
			return e
		}
		if _, e = cn.NewTask(ctx, cio.NullIO, containerd.WithTaskAPIEndpoint(exported.Address, 2)); e != nil {
			return fmt.Errorf("adopt task %s: %w", task.TaskID, e)
		}
	}
	fmt.Printf("sandbox=%s namespace=%s inherited_tasks=%d\n", id, ns, len(exported.Tasks.Tasks))
	return nil
}
